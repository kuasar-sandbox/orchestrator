"""Upload the locally tested exact tree; never move refs or write CI statuses."""
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.request

assert os.environ['GITHUB_REPOSITORY'] == 'kuasar-sandbox/orchestrator'
assert os.environ['GITHUB_REF'] == 'refs/heads/fix/proxy-invalid-traffic-policy'
EXPECTED_TREE = 'd097d39964de7578de320c507fb64e5b7161ad40'
PATCH_SHA256 = '392ec445c2790d3569538fab474d7bac8510c18e86980f515b1f2e4455becb87'
FRAGMENTS = ('b7afba5319c74b7711114a4d33b33ec33d2bc703', '5fec1a55c36191884390b4e81928d12bed9e6d28')
out = Path('/tmp/proxy-followup-workbench')
out.mkdir(exist_ok=True)

def git(*args):
    return subprocess.check_output(['git', *args]).decode().strip()

def api(method, path, payload=None):
    # Only Git-object endpoints are used. No ref, workflow, or status mutation.
    assert path.startswith('/git/blobs') or path == '/git/trees'
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request('https://api.github.com/repos/kuasar-sandbox/orchestrator' + path,
        data=data, method=method, headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
        'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)

head = git('rev-parse', 'HEAD')
assert head == os.environ['GITHUB_SHA']
input_tree = git('rev-parse', 'HEAD^{tree}')
fragments = []
for sha in FRAGMENTS:
    blob = api('GET', '/git/blobs/' + sha)
    assert blob['sha'] == sha and blob['encoding'] == 'base64'
    fragments.append(base64.b64decode(blob['content']))
patch = gzip.decompress(base64.b64decode(b''.join(fragments), validate=True))
assert len(patch) == 70760 and hashlib.sha256(patch).hexdigest() == PATCH_SHA256
patch_file = out / 'completion.patch'
patch_file.write_bytes(patch)
subprocess.run(['git', 'apply', '--check', str(patch_file)], check=True)
subprocess.run(['git', 'apply', '--index', str(patch_file)], check=True)
subprocess.run(['git', 'rm', '.github/proxy-followup-workbench.py', '.github/workflows/proxy-followup-workbench.yml'], check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], check=True)
assert git('write-tree') == EXPECTED_TREE, 'Final source differs from locally reviewed/tested tree'
entries = []
for path in git('diff', '--cached', '--name-only').splitlines():
    index_entry = git('ls-files', '--stage', '--', path)
    if not index_entry:
        entries.append({'path': path, 'mode': '100644', 'type': 'blob', 'sha': None})
        continue
    mode, local_sha, _ = index_entry.split(None, 2)
    contents = subprocess.check_output(['git', 'show', ':' + path])
    result = api('POST', '/git/blobs', {'content': base64.b64encode(contents).decode(), 'encoding': 'base64'})
    assert result['sha'] == local_sha
    entries.append({'path': path, 'mode': mode, 'type': 'blob', 'sha': local_sha})
result = api('POST', '/git/trees', {'base_tree': input_tree, 'tree': entries})
assert result['sha'] == EXPECTED_TREE
receipt = {'input_commit': head, 'input_tree': input_tree, 'output_tree': result['sha'],
    'patch_sha256': PATCH_SHA256, 'files': entries, 'refs_modified': False, 'statuses_modified': False}
(out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
(out / 'final.diff').write_bytes(subprocess.check_output(['git', 'diff', '--cached', '--binary']))
print(json.dumps(receipt, indent=2))
