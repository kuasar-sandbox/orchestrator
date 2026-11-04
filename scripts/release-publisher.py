#!/usr/bin/env python3
"""Publish the current validated bundle without persistent build/test/source records."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

SCRIPT = Path(__file__).resolve().parent
STABLE = re.compile(r'(?:(runtime|vmlinux)-)?v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)')
VERSION = re.compile(STABLE.pattern + r'(?:-preview\.[0-9]{8}(?:\.[1-9][0-9]*)?)?')


def require(condition, message):
    if not condition: raise ValueError(message)


def gh(*args, data=None):
    return subprocess.run(['gh', *map(str, args)], input=json.dumps(data) if data is not None else None,
                          text=True, capture_output=True)


def api(endpoint, *, method='GET', data=None, optional=False):
    arguments = ['api', '--method', method, endpoint]
    if data is not None: arguments += ['--input', '-']
    result = gh(*arguments, data=data)
    if result.returncode:
        if optional and '(HTTP 404)' in result.stderr: return None
        raise ValueError(result.stderr.strip() or 'GitHub request failed')
    return json.loads(result.stdout) if result.stdout.strip() else None


def releases(repository):
    result = gh('api', '--paginate', '--slurp', f'repos/{repository}/releases?per_page=100')
    require(result.returncode == 0, result.stderr.strip() or 'Release listing failed')
    return [row for page in json.loads(result.stdout) for row in page]


def find_release(repository, tag):
    rows = [row for row in releases(repository) if row.get('tag_name') == tag]
    require(len(rows) <= 1, 'multiple Releases use the selected tag')
    return rows[0] if rows else None


def unit_for(repository, tag):
    owner = repository.split('/')[-1]
    match = VERSION.fullmatch(tag)
    require(match is not None, 'invalid component release version')
    if owner == 'guest-runtime':
        require(match[1] in ('runtime', 'vmlinux'), 'guest-runtime tag requires its release unit')
        return match[1]
    require(owner in ('accelerator','connector','sandboxer','orchestrator') and match[1] is None,
            'release unit/tag mismatch')
    return owner


def release_cli(command, tag, arch, *args):
    kind = os.environ.get('RELEASE_KIND')
    unit = tag.split('-v', 1)[0] if tag.startswith(('runtime-v','vmlinux-v')) else None
    require(kind is None or kind == unit, 'RELEASE_KIND differs from tag')
    return ['bash', str(SCRIPT/'release.sh'), command, *([unit] if unit else []), tag, arch, *map(str,args)]


def names(tag, arch):
    require(arch in ('x86_64','aarch64','all'), 'invalid package architecture')
    return [subprocess.check_output(release_cli('archive-name', tag, selected), text=True).strip()
            for selected in (('x86_64','aarch64') if arch == 'all' else (arch,))]


def local_assets(bundle, expected):
    directory = bundle/'assets'
    require(directory.is_dir() and not directory.is_symlink(), 'missing or linked asset directory')
    require({p.name for p in directory.iterdir()} == set(expected), 'bundle must contain exactly the selected archives')
    result = {}
    for name in expected:
        path = directory/name
        require(path.is_file() and not path.is_symlink(), 'asset is not a regular file: '+name)
        size = path.stat().st_size
        require(size > 0, 'empty release asset: '+name)
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        result[name] = {'size':size, 'digest':'sha256:'+digest, 'state':'uploaded'}
    return result


def validate_bundle(tag, arch, bundle, source_sha=None):
    environment = dict(os.environ)
    if source_sha is not None: environment['SOURCE_SHA'] = source_sha
    expected = names(tag, arch)
    before = local_assets(bundle, expected)
    if arch == 'all':
        with tempfile.TemporaryDirectory(prefix='component-validation-') as directory:
            for selected, name in zip(('x86_64','aarch64'), expected):
                view = Path(directory)/selected; (view/'assets').mkdir(parents=True)
                shutil.copyfile(bundle/'assets'/name, view/'assets'/name)
                shutil.copyfile(bundle/'release-notes.md', view/'release-notes.md')
                subprocess.run(release_cli('validate',tag,selected,view), env=environment, check=True)
    else:
        subprocess.run(release_cli('validate',tag,arch,bundle), env=environment, check=True)
    require(local_assets(bundle, expected) == before, 'bundle changed during validation')
    return before


def human_notes(bundle):
    path = bundle/'release-notes.md'
    require(path.is_file() and not path.is_symlink(), 'missing regular human release notes')
    notes = path.read_text()
    require(notes.strip() and len(notes.encode('utf-8')) <= 125000,
            'release notes are empty or too long; edit them manually before publishing')
    require(not any(marker in notes for marker in ('<!-- kuasar-release-source ', '<!-- kuasar-preview-binding ',
                    '<!-- kuasar-release-validation ')), 'release notes contain a retired machine marker')
    return notes


def tag_target(repository, tag):
    ref = api(f'repos/{repository}/git/ref/tags/{tag}', optional=True)
    if ref is None: return None
    obj = ref.get('object', {}); seen = set()
    for _ in range(16):
        sha = obj.get('sha')
        require(isinstance(sha,str) and re.fullmatch('[0-9a-f]{40}',sha) and sha not in seen, 'invalid tag target')
        seen.add(sha)
        if obj.get('type') == 'commit': return sha
        require(obj.get('type') == 'tag', 'unsupported tag object')
        obj = api(f'repos/{repository}/git/tags/{sha}').get('object',{})
    raise ValueError('tag nesting exceeds supported limit')


def remote_assets(release, expected, *, complete, identities=None):
    rows = release.get('assets')
    require(isinstance(rows,list) and all(isinstance(row,dict) for row in rows), 'unknown uploaded asset collection')
    observed, ids = {}, set()
    for row in rows:
        name, identifier = row.get('name'), row.get('id')
        require(name in expected and name not in observed, 'unexpected or duplicate remote asset')
        require(type(identifier) is int and identifier > 0 and identifier not in ids, 'invalid remote asset ID')
        require(type(row.get('size')) is int and row['size'] > 0, 'invalid remote asset size')
        require(all(row.get(key)==value for key,value in expected[name].items()),
                'same-name remote asset differs from validated bytes; refusing overwrite: '+name)
        observed[name] = {'id':identifier, **expected[name]}; ids.add(identifier)
        if identities is not None and name in identities:
            require(observed[name] == identities[name], 'remote asset identity changed during publication: '+name)
    require(not complete or set(observed) == set(expected), 'published asset collection is incomplete')
    if identities is not None:
        require(set(identities) <= set(observed), 'previously observed remote asset disappeared')
        identities.update(observed)
    return observed


def release_identity(release, tag, *, public=False):
    require(type(release.get('id')) is int and release['id'] > 0 and release.get('tag_name') == tag,
            'Release identity differs from selected version')
    require(type(release.get('draft')) is bool and type(release.get('prerelease')) is bool
            and release['prerelease'] == ('-preview.' in tag),
            'Release state differs from selected version')
    require(not public or release['draft'] is False, 'Release is still a draft')


def revalidate_preview(tag, unit):
    if '-preview.' not in tag: return
    subprocess.run(['bash', str(SCRIPT/'validate-preview-line.sh'), unit, tag,
                    os.environ.get('AGGREGATE_VERSION',''),os.environ.get('AGGREGATE_SHA','')],check=True)


def stable_version(tag):
    match = STABLE.fullmatch(tag)
    return ((match[1] or ''), tuple(map(int,match.groups()[1:]))) if match else None


def published_time(release):
    value = release.get('published_at')
    require(isinstance(value,str), 'Stable publication time is unavailable; Latest retained')
    try:
        parsed = datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError:
        raise ValueError('Stable publication time is invalid; Latest retained') from None
    require(parsed.tzinfo is not None and parsed.utcoffset() is not None,
            'Stable publication time has no timezone; Latest retained')
    return parsed


def update_latest(repository, release, commit, source_ref):
    candidate = stable_version(release['tag_name'])
    if source_ref != 'main' or candidate is None: return
    require(release['draft'] is False and release['prerelease'] is False, 'Latest requires a published Stable release')
    require(tag_target(repository,release['tag_name']) == commit, 'tag changed before Latest update')
    main = api(f'repos/{repository}/git/ref/heads/main')['object']
    require(main.get('type') == 'commit' and re.fullmatch('[0-9a-f]{40}',main.get('sha','')), 'invalid main commit')
    status = api(f'repos/{repository}/compare/{commit}...{main["sha"]}')['status']
    require(status in ('ahead','identical'), 'current Stable source is not on main')
    # Existing same-unit Stable versions are conservative bounds, not eligibility proofs.
    for row in releases(repository):
        version = stable_version(row.get('tag_name',''))
        if row.get('draft') is False and row.get('prerelease') is False and version and version[0]==candidate[0] and version[1]>candidate[1]:
            print('Latest retained: a higher Stable version exists in this release unit')
            return
    latest = api(f'repos/{repository}/releases/latest', optional=True)
    if latest is not None:
        unit_for(repository, latest.get('tag_name',''))
        version = stable_version(latest.get('tag_name',''))
        require(version is not None and latest.get('draft') is False and latest.get('prerelease') is False,
                'Latest has an unexpected identity; retain it for inspection')
        if latest.get('id') == release['id']:
            require(latest.get('tag_name') == release['tag_name'], 'Latest tag changed during publication')
            return
        if version[0] == candidate[0]:
            if version[1] >= candidate[1]: return
        elif published_time(latest) >= published_time(release):
            print('Latest retained: another release unit was published more recently')
            return
    api(f'repos/{repository}/releases/{release["id"]}',method='PATCH',data={'make_latest':'true'})
    current = api(f'repos/{repository}/releases/latest')
    require(current.get('id') == release['id'] and current.get('tag_name') == release['tag_name'], 'Latest update did not converge')


def publish(repository, tag, arch, commit, bundle, source_ref):
    unit = unit_for(repository,tag)
    require(re.fullmatch('[0-9a-f]{40}',commit), 'commit must be a full lowercase SHA')
    require(source_ref == 'main' or re.fullmatch(r'release/v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.x',source_ref), 'invalid release source branch')
    notes = human_notes(bundle)
    expected = validate_bundle(tag,arch,bundle,source_sha=commit)
    release = find_release(repository,tag)
    identities = {}
    if release is not None:
        release_identity(release,tag)
        remote_assets(release,expected,complete=not release['draft'],identities=identities)
        if release['draft']: require(release.get('body') == notes, 'existing draft notes differ; reconcile them manually')
    target = tag_target(repository,tag)
    require(target is None or target == commit, 'tag already identifies a different commit')
    if release is not None and not release['draft']:
        require(target == commit, 'published Release lacks its selected tag')
        update_latest(repository,release,commit,source_ref)
        print('Verified already published',repository,tag)
        return
    revalidate_preview(tag,unit)
    require(local_assets(bundle,expected) == expected, 'validated bytes changed before upload')
    if target is None:
        api(f'repos/{repository}/git/refs',method='POST',data={'ref':'refs/tags/'+tag,'sha':commit})
    require(tag_target(repository,tag) == commit, 'tag creation did not preserve selected commit')
    if release is None:
        release = api(f'repos/{repository}/releases',method='POST',data={
            'tag_name':tag,'target_commitish':commit,'name':tag,'body':notes,'draft':True,
            'prerelease':'-preview.' in tag,'make_latest':'false'})
    release_identity(release,tag)
    present = remote_assets(release,expected,complete=False,identities=identities)
    for name in sorted(set(expected)-set(present)):
        require(local_assets(bundle,expected) == expected, 'validated bytes changed during upload')
        result = gh('release','upload',tag,bundle/'assets'/name,'--repo',repository)
        require(result.returncode == 0, result.stderr.strip() or 'asset upload failed; retain original stage for retry')
        uploaded = api(f'repos/{repository}/releases/{release["id"]}')
        require(uploaded.get('id') == release['id'], 'Release ID changed during upload')
        release_identity(uploaded,tag)
        remote_assets(uploaded,expected,complete=False,identities=identities)
        require(name in identities, 'uploaded asset is absent after upload')
    fresh = api(f'repos/{repository}/releases/{release["id"]}')
    require(fresh.get('id') == release['id'], 'Release ID changed during publication')
    release_identity(fresh,tag);remote_assets(fresh,expected,complete=True,identities=identities)
    require(fresh.get('body') == notes and fresh['draft'] is True, 'draft changed during upload')
    require(tag_target(repository,tag) == commit, 'tag changed before publication')
    revalidate_preview(tag,unit)
    api(f'repos/{repository}/releases/{release["id"]}',method='PATCH',data={
        'draft':False,'prerelease':'-preview.' in tag,'make_latest':'false'})
    published = api(f'repos/{repository}/releases/{release["id"]}')
    require(published.get('id') == release['id'], 'Release ID changed during publication')
    release_identity(published,tag,public=True);remote_assets(published,expected,complete=True,identities=identities)
    require(published.get('body') == notes and local_assets(bundle,expected) == expected,
            'published notes or local validated bytes changed')
    update_latest(repository,published,commit,source_ref)
    print('Published',repository,tag)


def assemble(tag,x86,arm,output):
    require(not output.exists(), 'dual-architecture bundle output must be fresh')
    require(human_notes(x86) == human_notes(arm), 'architecture bundle notes differ; reconcile them manually')
    for arch,bundle in [('x86_64',x86),('aarch64',arm)]: validate_bundle(tag,arch,bundle)
    (output/'assets').mkdir(parents=True)
    for arch,bundle in [('x86_64',x86),('aarch64',arm)]:
        name=names(tag,arch)[0];shutil.copyfile(bundle/'assets'/name,output/'assets'/name)
    shutil.copyfile(x86/'release-notes.md',output/'release-notes.md')
    validate_bundle(tag,'all',output)


def main(arguments):
    repository=os.environ.get('GH_REPO') or os.environ.get('GITHUB_REPOSITORY','')
    require(re.fullmatch(r'kuasar-sandbox/(?:accelerator|connector|sandboxer|orchestrator|guest-runtime)',repository), 'invalid component repository')
    if len(arguments)==5 and arguments[0]=='assemble':
        _,tag,x86,arm,output=arguments;unit_for(repository,tag);assemble(tag,Path(x86),Path(arm),Path(output))
    elif len(arguments)==3 and arguments[0]=='check':
        _,tag,arch=arguments;unit_for(repository,tag);names(tag,arch)
        release=find_release(repository,tag)
        require(release is None or release.get('draft') is True, 'component Release already exists; reuse the specified published tag')
    elif len(arguments)==6 and arguments[0]=='publish':
        _,tag,arch,commit,bundle,source_ref=arguments;publish(repository,tag,arch,commit,Path(bundle),source_ref)
    else: raise ValueError('usage: publish-release.sh <assemble|check|publish> ...')


if __name__=='__main__':
    try: main(sys.argv[1:])
    except (ValueError,KeyError,OSError,subprocess.CalledProcessError) as error:
        sys.exit('publish-release: '+str(error))
