"""Upload only the exact locally reviewed documentation/comment correction tree."""
import base64
import json
import os
from pathlib import Path
import subprocess
import urllib.request
assert os.environ['GITHUB_REPOSITORY'] == 'kuasar-sandbox/orchestrator'
assert os.environ['GITHUB_REF'] == 'refs/heads/fix/proxy-invalid-traffic-policy'
EXPECTED = 'd20fad07c9f6ac2c14ab1ac2f64ed06a0fe164b9'
out = Path('/tmp/proxy-followup-workbench')
out.mkdir(exist_ok=True)
def git(*args):
    return subprocess.check_output(['git', *args]).decode().strip()
def upload(path, payload):
    assert path in ('/git/blobs', '/git/trees')
    req = urllib.request.Request('https://api.github.com/repos/kuasar-sandbox/orchestrator' + path,
        data=json.dumps(payload).encode(), method='POST', headers={'Authorization': 'Bearer '+os.environ['GH_TOKEN'], 'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)
head = git('rev-parse', 'HEAD')
assert head == os.environ['GITHUB_SHA']
input_tree = git('rev-parse', 'HEAD^{tree}')
for name in ('docs/node.md', 'docs/node_zh.md'):
    p = Path(name)
    text = p.read_text()
    assert text.count('routesync v7') == 1
    p.write_text(text.replace('routesync v7', 'routesync v8'))
p = Path('internal/proxyapp/worker.go')
text = p.read_text()
old = '// Close releases descriptors and is safe after Run has already closed them,\n// and to call more than once. Successful mappings are deliberately NOT unmapped here: asynchronous users need not have\n// stopped when Run returns. Kernel process teardown reclaims both mappings;\n// the master separately clears this worker\'s counters only after cmd.Wait.'
new = '// Close releases descriptors and is safe after Run has already closed them,\n// and to call more than once. Successful mappings are deliberately NOT unmapped\n// here: asynchronous users need not have stopped when Run returns. Kernel\n// process teardown reclaims both mappings; the master separately clears this\n// worker\'s counters only after cmd.Wait.'
assert text.count(old) == 1
p.write_text(text.replace(old,new))
subprocess.run(['git','add','docs/node.md','docs/node_zh.md','internal/proxyapp/worker.go'],check=True)
subprocess.run(['git','rm','.github/proxy-followup-workbench.py','.github/workflows/proxy-followup-workbench.yml'],check=True)
subprocess.run(['git','diff','--cached','--check'],check=True)
assert git('write-tree') == EXPECTED
entries=[]
for path in git('diff','--cached','--name-only').splitlines():
    entry=git('ls-files','--stage','--',path)
    if not entry:
        entries.append({'path':path,'mode':'100644','type':'blob','sha':None})
        continue
    mode,sha,_=entry.split(None,2)
    data=subprocess.check_output(['git','show',':'+path])
    result=upload('/git/blobs',{'content':base64.b64encode(data).decode(),'encoding':'base64'})
    assert result['sha'] == sha
    entries.append({'path':path,'mode':mode,'type':'blob','sha':sha})
result=upload('/git/trees',{'base_tree':input_tree,'tree':entries})
assert result['sha'] == EXPECTED
receipt={'input_commit':head,'input_tree':input_tree,'output_tree':EXPECTED,'refs_modified':False,'statuses_modified':False,'files':entries}
(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
