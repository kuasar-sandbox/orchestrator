#!/usr/bin/env python3
"""Delete only a confirmed component Preview; ambiguous state is retained."""
import json
import os
import re
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def gh(*args):
    return subprocess.run(['gh', 'api', *args], capture_output=True, text=True)


def api(endpoint, optional=False):
    result = gh(endpoint)
    if result.returncode:
        if optional and '(HTTP 404)' in result.stderr:
            return None
        raise ValueError(result.stderr.strip() or 'GitHub read failed')
    return json.loads(result.stdout)


def release_for(repository, tag):
    result = gh('--paginate', '--slurp', f'repos/{repository}/releases?per_page=100')
    require(result.returncode == 0, result.stderr.strip() or 'Release listing failed')
    rows = [row for page in json.loads(result.stdout) for row in page if row.get('tag_name') == tag]
    require(len(rows) <= 1, 'multiple Releases use the Preview tag')
    return rows[0] if rows else None


def tag_target(repository, ref):
    obj = ref.get('object', {})
    seen = set()
    for _ in range(16):
        sha = obj.get('sha', '')
        require(isinstance(sha, str) and re.fullmatch('[0-9a-f]{40}', sha) and sha not in seen,
                'invalid or cyclic tag target')
        seen.add(sha)
        if obj.get('type') == 'commit':
            return sha
        require(obj.get('type') == 'tag', 'unknown tag target type')
        obj = api(f'repos/{repository}/git/tags/{sha}').get('object', {})
    raise ValueError('tag nesting exceeds the supported limit')


def package_names(unit, tag):
    if unit in ('runtime', 'vmlinux'):
        prefix = 'sandbox-runtime' if unit == 'runtime' else 'vmlinux'
        version = tag.removeprefix(unit + '-')
        return {f'{prefix}-{arch}-{version}.tar.gz' for arch in ('x86_64', 'aarch64')}
    return {f'{unit}-{tag}-linux-{arch}.tar.gz' for arch in ('x86_64', 'aarch64')}


def complete(release, names):
    assets = release.get('assets')
    require(isinstance(assets, list), 'unknown asset collection; retain Preview')
    assets = [row for row in assets if row.get('name') != 'SHA256SUMS']
    observed, ids = set(), set()
    for row in assets:
        name, identifier = row.get('name'), row.get('id')
        require(name in names and name not in observed, 'unexpected or duplicate package; retain Preview')
        require(type(identifier) is int and identifier > 0 and identifier not in ids
                and type(row.get('size')) is int and row['size'] > 0
                and isinstance(row.get('digest'), str)
                and re.fullmatch('sha256:[0-9a-f]{64}', row['digest'])
                and row.get('state') in ('uploaded', 'starter'),
                'unknown native asset metadata; retain Preview')
        observed.add(name); ids.add(identifier)
    return (release.get('draft') is False and release.get('prerelease') is True
            and observed in ({name for name in names if 'x86_64' in name}, names)
            and all(row['state'] == 'uploaded' for row in assets))


def delete(endpoint, read_endpoint=None):
    result = gh('--method', 'DELETE', endpoint, '--silent')
    require(result.returncode == 0 or '(HTTP 404)' in result.stderr,
            result.stderr.strip() or 'GitHub deletion failed')
    require(api(read_endpoint or endpoint, optional=True) is None, 'deleted object remains visible')


def asset_identity(release):
    fields = ('id', 'name', 'size', 'digest', 'state')
    return sorted((json.dumps({key: row.get(key) for key in fields}, sort_keys=True)
                   for row in release.get('assets', [])))


def collect(repository, unit, tag, sha, mode):
    require(unit in ('accelerator', 'connector', 'sandboxer', 'orchestrator', 'runtime', 'vmlinux'),
            'unknown release unit')
    owner = 'guest-runtime' if unit in ('runtime', 'vmlinux') else unit
    require(repository == 'kuasar-sandbox/' + owner, 'release unit does not match repository')
    prefix = unit + '-' if unit in ('runtime', 'vmlinux') else ''
    require(re.fullmatch(prefix + r'v[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}(?:\.[1-9][0-9]*)?', tag),
            'invalid Preview tag')
    require(re.fullmatch('[0-9a-f]{40}', sha), 'source-sha must be a full lowercase SHA')
    require(mode in ('incomplete', 'gc'), 'mode must be incomplete or gc')
    ref_endpoint = f'repos/{repository}/git/ref/tags/{tag}'
    original_ref = api(ref_endpoint, optional=True)
    release = release_for(repository, tag)
    if original_ref is None and release is None:
        return []
    if original_ref is not None:
        require(tag_target(repository, original_ref) == sha, 'tag source identity changed')
    elif release is not None:
        require(release.get('target_commitish') == sha, 'tagless Release source is unknown or changed')
    if release is not None:
        require(release.get('draft') is True or
                (release.get('draft') is False and release.get('prerelease') is True),
                'refusing to delete a non-preview release')
        require(release.get('immutable') is not True, 'immutable Preview cannot be deleted')
        require(type(release.get('id')) is int and release['id'] > 0, 'invalid Release ID')
        is_complete = complete(release, package_names(unit, tag))
        require(mode != 'incomplete' or not is_complete, 'refusing incomplete recovery for a complete preview')
    fields = ('id', 'tag_name', 'draft', 'prerelease', 'immutable', 'target_commitish')
    current = release_for(repository, tag)
    require((current is None and release is None) or (current is not None and release is not None
            and all(current.get(key) == release.get(key) for key in fields)
            and asset_identity(current) == asset_identity(release)), 'Release changed before deletion')
    require(api(ref_endpoint, optional=True) == original_ref, 'tag changed before deletion')
    deleted = []
    if release is not None:
        delete(f'repos/{repository}/releases/{release["id"]}')
        deleted.append('release')
    if original_ref is not None:
        require(release_for(repository, tag) is None, 'a Release appeared before tag deletion')
        current_ref = api(ref_endpoint, optional=True)
        require(current_ref is None or current_ref == original_ref, 'tag changed after Release deletion')
        if current_ref is not None:
            delete(f'repos/{repository}/git/refs/tags/{tag}', ref_endpoint)
        deleted.append('tag')
    return deleted


if __name__ == '__main__':
    try:
        require(len(sys.argv) == 5, 'usage: delete-preview.py <unit> <tag> <source-sha> <incomplete|gc>')
        result = collect(os.environ['GITHUB_REPOSITORY'], *sys.argv[1:])
        print('Preview cleanup:', ', '.join(result) if result else 'already absent')
    except (ValueError, KeyError, OSError) as error:
        sys.exit(f'delete-preview: {error}')
