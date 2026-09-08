"""Temporary offline source export, without repository writes or status changes."""
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import urllib.error
import urllib.request

assert os.environ['GITHUB_REPOSITORY'] == 'kuasar-sandbox/orchestrator'
assert os.environ['GITHUB_REF'] == 'refs/heads/fix/proxy-invalid-traffic-policy'
out = Path('/tmp/proxy-followup-workbench')
out.mkdir(exist_ok=True)
root = Path.cwd()
subprocess.run(['git', 'archive', '--format=tar', '-o', str(out / 'source.tar'), 'HEAD'], check=True)
(out / 'head.txt').write_text(subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True))

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def api(path):
    request = urllib.request.Request('https://api.github.com/repos/kuasar-sandbox/' + path,
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'], 'Accept': 'application/vnd.github+json'})
    try:
        return urllib.request.build_opener(NoRedirect).open(request, timeout=120)
    except urllib.error.HTTPError as exc:
        if exc.code != 302 or not exc.headers['Location'].startswith('https://codeload.github.com/'):
            raise
        # Signed/authorized redirect: never forward the API credential to another host.
        return urllib.request.urlopen(exc.headers['Location'], timeout=120)

revisions = {}
for repo in ('accelerator', 'connector', 'sandboxer'):
    with api(repo + '/git/ref/heads/main') as response:
        sha = json.load(response)['object']['sha']
    assert re.fullmatch('[0-9a-f]{40}', sha)
    revisions[repo] = sha
    archive = out / (repo + '.tar.gz')
    with api(repo + '/tarball/' + sha) as response, archive.open('wb') as target:
        import shutil
        shutil.copyfileobj(response, target)
    destination = root.parent / repo
    destination.mkdir(exist_ok=True)
    with tarfile.open(archive) as source:
        for member in source.getmembers():
            parts = member.name.split('/', 1)
            if len(parts) != 2 or not parts[1]:
                continue
            member.name = parts[1]
            source.extract(member, destination, filter='data')
(out / 'revisions.json').write_text(json.dumps(revisions, indent=2) + '\n')
with (out / 'vendor.log').open('w') as log:
    subprocess.run(['go', 'mod', 'vendor'], stdout=log, stderr=subprocess.STDOUT, check=True)
subprocess.run(['tar', '-czf', str(out / 'vendor.tar.gz'), 'vendor'], check=True)
goroot = subprocess.check_output(['go', 'env', 'GOROOT'], text=True).strip()
subprocess.run(['tar', '-czf', str(out / 'go.tar.gz'), '-C', goroot, '.'], check=True)
print('Exported current source, exact sibling revisions, vendor tree and Go toolchain; no credentials or Git configuration exported.')
