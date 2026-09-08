"""Temporary source-only export; no credentials, settings, or status writes."""
import os
from pathlib import Path
import subprocess
assert os.environ['GITHUB_REPOSITORY'] == 'kuasar-sandbox/orchestrator'
assert os.environ['GITHUB_REF'] == 'refs/heads/fix/proxy-invalid-traffic-policy'
out = Path('/tmp/proxy-followup-workbench')
out.mkdir(exist_ok=True)
subprocess.run(['git','archive','--format=tar','-o',str(out/'source.tar'),'HEAD'], check=True)
subprocess.run(['git','bundle','create',str(out/'source.bundle'),'HEAD'], check=True)
(out/'head.txt').write_text(subprocess.check_output(['git','rev-parse','HEAD'],text=True))
print('Exported refreshed source and reachable commit objects; Git configuration and credentials are not included.')
