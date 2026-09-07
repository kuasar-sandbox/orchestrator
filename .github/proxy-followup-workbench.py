"""Temporary, same-repository branch workbench; remove before final review."""
import os
from pathlib import Path
import subprocess
import urllib.request

assert os.environ['GITHUB_REPOSITORY'] == 'kuasar-sandbox/orchestrator'
assert os.environ['GITHUB_REF'] == 'refs/heads/fix/proxy-invalid-traffic-policy'
out = Path('/tmp/proxy-followup-workbench')
out.mkdir(exist_ok=True)
subprocess.run(['git', 'archive', '--format=tar', '-o', str(out/'source.tar'), 'HEAD'], check=True)
(out/'head.txt').write_text(subprocess.check_output(['git','rev-parse','HEAD'], text=True))
url = 'https://api.github.com/repos/kuasar-sandbox/orchestrator/actions/jobs/101694937508/logs'
req = urllib.request.Request(url, headers={'Authorization': 'Bearer '+os.environ['GH_TOKEN'], 'Accept':'application/vnd.github+json'})
try:
    with urllib.request.urlopen(req, timeout=60) as response:
        (out/'pr322-failed-job.log').write_bytes(response.read())
except Exception as exc:
    (out/'log-export-error.txt').write_text(type(exc).__name__+': '+str(exc))
print('Exported repository source and available diagnostics; no merge gate was modified.')
