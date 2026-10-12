import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("template_cache", HERE / "template_cache.py")
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)

# Format/storage substitutes isolate the cache orchestration contract. Native
# paired E2E must separately prove the real format, key isolation and Guest boot.
TOOL = '''#!/usr/bin/env python3
import hashlib,json,os,pathlib,sys
a=sys.argv[1:]; cfg=json.load(open(a[a.index('--manifest-config')+1]))
assert cfg['crypto']['local']=='off'
store=pathlib.Path(cfg['store']['endpoint']); key=os.environ['MANIFEST_KEY']
if pathlib.Path(sys.argv[0]).name=='flatten-ctl':
    ref=a[-1]; p=store/ref[11:] if ref.startswith('manifest://') else pathlib.Path(ref)
    assert p.read_bytes()==b'neutral execute image'
    print(json.dumps({'erofs_size':4096,'config':{'Entrypoint':['app']}}))
elif a[0]=='load':
    output=pathlib.Path(a[a.index('--output')+1])
    with output.open('xb') as f:f.write((store/a[-1]).read_bytes())
    if os.environ.get('INTERRUPT_EXPORT'):sys.exit(17)
else:
    raw=pathlib.Path(a[-1]).read_bytes()
    digest=hashlib.sha256(key.encode()+raw).hexdigest()
    (store/digest).write_bytes(raw)
    print(digest)
'''

FLOW = r'''
set -euo pipefail
fail() { echo "$*" >&2; exit 1; }
. "$SCRIPT_DIR/execute_template.sh"
build_image_template_source() {
    python3 - "$WORK" "$1" "${2:-}" "$MK" >> "$CALLS" <<'PY'
import base64,hashlib,json,pathlib,sys
work,profile,source,key=sys.argv[1:]
store=pathlib.Path(work)/'store'
if source:
    assert source.startswith(profile+'-img-')
    encoded=source.split('-',2)[2]
    ref=base64.urlsafe_b64decode(encoded+'='*(-len(encoded)%4)).decode()[11:]
    assert (store/ref).read_bytes()==b'neutral execute image'
else:
    ref=hashlib.sha256(key.encode()+b'neutral execute image').hexdigest()
    (store/ref).write_bytes(b'neutral execute image')
template=profile+'-img-'+base64.urlsafe_b64encode(('manifest://'+ref).encode()).decode().rstrip('=')
(pathlib.Path(work)/'template').write_text(template)
print(json.dumps({'profile':profile,'source':'fromTemplate' if source else 'fromImage','key':ref}))
PY
    TEMPLATE=$(cat "$WORK/template")
}
build_image_template "$PROFILE"
'''


class CacheContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.cleanup)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in ('manifest-ctl', 'flatten-ctl'):
            tool = self.bin / name
            tool.write_text(TOOL)
            tool.chmod(0o755)
        self.cache = self.root / 'cache'
        self.cache.mkdir(mode=0o700)
        self.calls = self.root / 'calls.jsonl'
        self.env = os.environ | dict(BIN=str(self.bin), SCRIPT_DIR=str(HERE),
            E2E_TEMPLATE_CACHE_DIR=str(self.cache), E2E_TEMPLATE_CACHE_PROVENANCE='a'*64,
            ORCHESTRATOR_EXECUTE_IMAGE='sha256:'+'b'*64, E2E_ARCH=platform.machine(), CALLS=str(self.calls))

    def cleanup(self):
        for directory, _, files in os.walk(self.root):
            Path(directory).chmod(0o700)
            for name in files:
                path = Path(directory) / name
                if not path.is_symlink():
                    path.chmod(0o600)
        self.temporary.cleanup()

    def run_case(self, name, profile='e2b', **overrides):
        work = self.root / name
        work.mkdir()
        (work/'store').mkdir()
        (work/'manifest.yaml').write_text(json.dumps({'store':{'endpoint':str(work/'store')},
                                                    'crypto':{'chunk':'aes','manifest':'aes'}}))
        before = (work/'manifest.yaml').read_bytes()
        env = self.env | dict(WORK=str(work), CASE_OUT=str(work), MK=name, PROFILE=profile) | overrides
        result = subprocess.run(['bash','-c',FLOW],env=env,text=True,capture_output=True)
        self.assertEqual((work/'manifest.yaml').read_bytes(),before)
        return result,work

    def entries(self):
        return [p for p in self.cache.iterdir() if not p.name.startswith('.') and p.is_dir()]

    def test_miss_then_hit_after_producer_cleanup_with_independent_keys_and_profiles(self):
        first,a=self.run_case('producer')
        self.assertEqual(first.returncode,0,first.stderr)
        entry,=self.entries()
        receipt=(entry/'receipt.json').read_text()
        self.assertNotIn('producer',receipt)
        shutil.rmtree(a)
        for name,profile in [('consumer','e2b'),('bare-consumer','bare')]:
            result,work=self.run_case(name,profile)
            self.assertEqual(result.returncode,0,result.stderr)
            record=json.loads((work/'template-preparation.jsonl').read_text())
            self.assertEqual(record['mode'],'hit')
            self.assertEqual(record['profile'],profile)
            self.assertTrue((work/'template').read_text().startswith(profile+'-img-'))
        calls=[json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertEqual([c['source'] for c in calls],['fromImage','fromTemplate','fromTemplate'])
        self.assertEqual(len({c['key'] for c in calls}),3)

    def test_disabled_uses_real_preparation_without_touching_cache(self):
        result,work=self.run_case('disabled',E2E_TEMPLATE_CACHE_DIR='',E2E_TEMPLATE_CACHE_PROVENANCE='')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(list(self.cache.iterdir()),[])
        self.assertEqual(json.loads((work/'template-preparation.jsonl').read_text())['mode'],'disabled')

    def test_provenance_image_recipe_and_tool_bytes_invalidate_key(self):
        original=cache.identity(self.env)
        keys={cache.entry_path(self.env,original)}
        for variable in ('E2E_TEMPLATE_CACHE_PROVENANCE','ORCHESTRATOR_EXECUTE_IMAGE'):
            env=self.env | {variable:('sha256:' if variable=='ORCHESTRATOR_EXECUTE_IMAGE' else '')+'c'*64}
            keys.add(cache.entry_path(env,cache.identity(env)))
        with patch.object(cache,'RECIPE','changed-recipe'):
            keys.add(cache.entry_path(self.env,cache.identity(self.env)))
        (self.bin/'flatten-ctl').write_text(TOOL+'\n# new reader\n')
        keys.add(cache.entry_path(self.env,cache.identity(self.env)))
        self.assertEqual(len(keys),5)
        with self.assertRaises(ValueError):
            cache.identity(self.env | {'E2E_ARCH':'emulated'})

    def test_damaged_entry_fails_before_registration_and_never_rebuilds(self):
        result,_=self.run_case('producer')
        self.assertEqual(result.returncode,0,result.stderr)
        entry,=self.entries()
        image=entry/'image.tar'
        image.chmod(0o600)
        image.write_bytes(b'corrupt')
        before=self.calls.read_bytes()
        result,_=self.run_case('consumer')
        self.assertNotEqual(result.returncode,0)
        self.assertIn('integrity failure',result.stderr)
        self.assertEqual(self.calls.read_bytes(),before)
        self.assertEqual(image.read_bytes(),b'corrupt')

    def test_interrupted_export_is_not_published_or_accepted_as_a_hit(self):
        result,_=self.run_case('interrupted',INTERRUPT_EXPORT='1')
        self.assertNotEqual(result.returncode,0)
        self.assertFalse(self.entries())
        self.assertTrue(list(self.cache.glob('.*.pending-*')))
        result,work=self.run_case('next')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads((work/'template-preparation.jsonl').read_text())['mode'],'miss')

    def test_noncanonical_and_non_image_references_are_rejected(self):
        valid=cache.image_template('a'*64,'e2b')
        self.assertEqual(cache.image_key(valid,'e2b'),'a'*64)
        for value in (valid+'=',valid.replace('-img-','-snp-'),valid.replace('e2b-','bare-')):
            with self.assertRaises(ValueError):cache.image_key(value,'e2b')

    def test_concurrent_callers_publish_once_under_the_entry_lock(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(self.run_case,['parallel-a','parallel-b']))
        modes=[]
        for result,work in results:
            self.assertEqual(result.returncode,0,result.stderr)
            modes.append(json.loads((work/'template-preparation.jsonl').read_text())['mode'])
        self.assertEqual(sorted(modes),['hit','miss'])
        self.assertEqual(len(self.entries()),1)

    def test_incomplete_symlink_hardlink_and_wrong_identity_entries_fail(self):
        value=cache.identity(self.env)
        entry=cache.entry_path(self.env,value)
        entry.mkdir()
        with self.assertRaisesRegex(ValueError,'incomplete'):cache.validate(entry,value)
        image=entry/'image.tar'
        receipt=entry/'receipt.json'
        image.write_bytes(b'valid')
        row={'identity':value,'bytes':5,'sha256':cache.digest(image)}
        receipt.write_text(json.dumps(row))
        self.assertEqual(cache.validate(entry,value),image)
        row['identity']={}
        receipt.write_text(json.dumps(row))
        with self.assertRaisesRegex(ValueError,'identity'):cache.validate(entry,value)
        row['identity']=value
        receipt.write_text(json.dumps(row))
        outside=self.root/'outside'
        image.rename(outside)
        image.symlink_to(outside)
        with self.assertRaisesRegex(ValueError,'regular'):cache.validate(entry,value)
        image.unlink()
        os.link(outside,image)
        with self.assertRaisesRegex(ValueError,'regular'):cache.validate(entry,value)


if __name__=='__main__':
    unittest.main()
