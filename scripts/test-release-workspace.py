"""Exercise the actual local release recipe with real Git, Go and GNU Make."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
OWNER = 'orchestrator'
def selected_platform():
    directory = os.environ.get('KUASAR_PLATFORM_DIR')
    selected_root = os.environ.get('KUASAR_PLATFORM_ROOT')
    if directory and selected_root and Path(directory).resolve() != Path(selected_root).resolve():
        raise SystemExit('KUASAR_PLATFORM_DIR and KUASAR_PLATFORM_ROOT select different platforms')
    return Path(directory or selected_root or ROOT.parent/'kuasar-sandbox').resolve()


PLATFORM = selected_platform()


class ReleaseWorkspaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not (PLATFORM/'ci/integration/go_workspace.py').is_file():
            raise RuntimeError('selected platform workspace tooling is missing: ' + str(PLATFORM))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='release workspace ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = self.root/OWNER
        self.env = dict(os.environ, GOENV='off', GOWORK='off', GOPROXY='off',
                        GONOPROXY='none', GOSUMDB='off', PYTHONDONTWRITEBYTECODE='1')
        for key in list(self.env):
            if key.startswith(('RELEASE_', 'MAKE')) or key in ('MFLAGS', 'KUASAR_E2E_GO'):
                self.env.pop(key)
        goroot = subprocess.check_output(['go', 'env', 'GOROOT'], cwd=self.root, env=self.env, text=True).strip()
        self.env['PATH'] = str(Path(goroot)/'bin') + os.pathsep + os.defpath
        self.make = shutil.which('make', path=os.defpath)
        self.deps = ['accelerator', 'connector', 'sandboxer', 'guest-runtime']
        for name in set([OWNER, 'sandboxer', *self.deps]):
            p=self.root/name; p.mkdir()
            (p/'go.mod').write_text('module github.com/kuasar-sandbox/'+name+'\n\ngo 1.26.1\n')
            (p/'.gitignore').write_text('bin/\nbuild/\npackage.json\n')
        (self.root/'accelerator/selected.go').write_text('package accelerator\nconst Selected = "selected-local"\n')
        with (self.owner/'go.mod').open('a') as f:
            f.write('require github.com/kuasar-sandbox/accelerator v0.1.10\nrequire github.com/kuasar-sandbox/guest-runtime v0.1.10 // indirect\n')
        (self.owner/'main.go').write_text('package main\nimport ("fmt"; "github.com/kuasar-sandbox/accelerator")\nfunc main(){fmt.Print(accelerator.Selected)}\n')
        self.target = 'release-runtime' if OWNER == 'guest-runtime' else 'release'
        original=(ROOT/'Makefile').read_text()
        recursive=original[original.index('release_make ='):].split('\n\n',1)[0]
        recipe=original.split(self.target+':\n',1)[1].split('\n\n',1)[0]
        variables = 'SHELL := /bin/bash\nBINDIR := bin/x86_64\nBUILD_DIR := build/x86_64\nTARGET_ARCH := x86_64\nSANDBOXER_DIR := ../sandboxer\nENVD := native-deps/bin/x86_64/envd\nVERSION := v0.1.99\nRUNTIME_VERSION := runtime-v0.1.10\n'
        requirement=original[original.index('go_requirement ='):].split('\nACCELERATOR_VERSION',1)[0]
        variables += 'GO := go\n'+recursive+'\n'+requirement+'\nACCELERATOR_VERSION ?= $(call go_requirement,accelerator)\n'
        variables += ''.join(name.upper()+'_VERSION := v0.1.10\n' for name in set(self.deps+['sandboxer'])-{'accelerator','guest-runtime'})
        build = '\n.PHONY: release release-runtime build flatten-ctl sandbox-runtime\nbuild flatten-ctl:\n\t@mkdir -p "$(BINDIR)"\n\tgo build -o "$(BINDIR)/flatten-ctl" .\n'
        build += 'sandbox-runtime:\n\t@test "$(FLATTEN_CTL)" = "$(BINDIR)/flatten-ctl"\n\t@test "$$("$(FLATTEN_CTL)")" = selected-local\n\t@cp "$(FLATTEN_CTL)" "$(BINDIR)/sandbox-runtime.bundle"\n'
        # The release recipe is copied verbatim; only costly build/package boundaries are fixtures.
        (self.owner/'Makefile').write_text(variables+self.target+':\n'+recipe+'\n'+build)
        scripts=self.owner/'scripts'; scripts.mkdir()
        (scripts/'release.sh').write_text('python3 scripts/package.py\n')
        (scripts/'package.py').write_text('import json,os,subprocess\nfrom pathlib import Path\np=Path(os.environ["RELEASE_BIN_DIR"])/"flatten-ctl"\nassert subprocess.check_output([p],text=True)=="selected-local"\nPath("package.json").write_text(json.dumps(dict(os.environ)))\n')
        (scripts/'prepare-sandbox-init.py').write_text('import argparse\nfrom pathlib import Path\np=argparse.ArgumentParser();p.add_argument("--output");p.add_argument("--go-toolchain-output");a,_=p.parse_known_args();f=Path(a.output);f.parent.mkdir(parents=True,exist_ok=True);f.write_text("selected-published-init");Path(a.go_toolchain_output).mkdir(exist_ok=True)\n')
        # A real file proxy serves the selected stable module. Isolate the cache
        # so an installed public release with the same name cannot hide a mistake.
        repositories = list(self.root.iterdir())
        proxy = self.root / 'proxy'
        self.env.update(GOPROXY=proxy.as_uri(), GOMODCACHE=str(self.root / 'mod-cache'))
        for name in self.deps:
            source = self.root / name
            module = 'github.com/kuasar-sandbox/' + name
            version = 'v0.1.10'
            endpoint = proxy / module / '@v'
            endpoint.mkdir(parents=True)
            (endpoint / (version + '.mod')).write_bytes((source / 'go.mod').read_bytes())
            (endpoint / (version + '.info')).write_text(json.dumps(
                {'Version': version, 'Time': '2026-01-01T00:00:00Z'}))
            with zipfile.ZipFile(endpoint / (version + '.zip'), 'w') as archive:
                for item in (source / 'go.mod', *source.glob('*.go')):
                    archive.write(item, module + '@' + version + '/' + item.name)
        subprocess.run(['go', 'mod', 'download', 'all'], cwd=self.owner, env=self.env,
                       check=True, capture_output=True)
        for p in repositories:
            for args in (['init','-q'], ['add','.'], ['-c','user.name=Test','-c','user.email=test@example.invalid','commit','-qm','fixture'], ['tag', 'runtime-v0.1.10' if p.name == 'guest-runtime' else 'v0.1.10']):
                subprocess.run(['git','-C',str(p),*args], check=True, capture_output=True)
        self.before = {p: p.read_bytes() for repo in repositories for p in (repo/'go.mod', repo/'go.sum')
                       if p.exists()}

    def tearDown(self):
        self.assertEqual({p:p.read_bytes() for p in self.before},self.before)
        sources = {self.root / name for name in (OWNER, 'sandboxer', *self.deps)}
        self.assertEqual({file for repo in sources for file in (repo/'go.mod', repo/'go.sum') if file.exists()},
                         set(self.before))

    def run_release(self, *arguments, success=True):
        result=subprocess.run([self.make,self.target,'KUASAR_PLATFORM_DIR='+str(PLATFORM),*arguments],cwd=self.owner,env=self.env,text=True,capture_output=True)
        if success:
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            return json.loads((self.owner/'package.json').read_text())
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertFalse((self.owner/'package.json').exists())
        self.assertFalse((self.owner/'bin').exists())
        return result.stderr

    def test_outer_workspace_and_old_output_overrides_cannot_change_release_bytes(self):
        for override in ('off',str(self.root/'unrelated.work')):
            with self.subTest(override=override):
                output=self.owner/'bin/custom'; output.mkdir(parents=True,exist_ok=True)
                (output/'flatten-ctl').unlink(missing_ok=True)
                shutil.copyfile('/bin/true', output/'flatten-ctl')
                package=self.run_release('GOWORK='+override, 'BINDIR=bin/custom',
                                         'FLATTEN_CTL=/old/flatten-ctl', 'RELEASE_BIN_DIR=/old/package')
                self.assertEqual(package['RELEASE_BIN_DIR'],str(output))
                self.assertEqual(package['RELEASE_SOURCE_ROOT'],str(self.owner))
                self.assertEqual(package['RELEASE_GUEST_RUNTIME_VERSION'], 'runtime-v0.1.10')
                self.assertEqual(package['RELEASE_GUEST_RUNTIME_SOURCE_DIR'], str(self.root/'guest-runtime'))
                self.assertEqual(package['GOWORK'], 'off')
                info = subprocess.check_output(['go', 'version', '-m', str(output/'flatten-ctl')],
                                               env=self.env, text=True)
                self.assertIn('dep\tgithub.com/kuasar-sandbox/accelerator\tv0.1.10\t', info)
                if OWNER=='guest-runtime':
                    self.assertEqual((output/'flatten-ctl').read_bytes(),(output/'sandbox-runtime.bundle').read_bytes())
                    self.assertEqual(Path(package['RELEASE_SANDBOX_INIT_BIN']).read_text(),'selected-published-init')
                    self.assertEqual(package['RELEASE_ENVD_BIN'],str(self.owner/'native-deps/bin/x86_64/envd'))

    def test_preview_uses_exact_upstream_tag_without_fetching_its_future_core(self):
        # A not-yet-published core belongs in go.mod. Its selected Preview tag
        # supplies the code for the build; the public proxy only has v0.1.10.
        preview = 'v0.1.11-preview.20261112'
        accelerator = self.root / 'accelerator'
        subprocess.run(['git', '-C', str(accelerator), 'tag', preview], check=True)
        module = self.owner / 'go.mod'
        module.write_text(module.read_text().replace('accelerator v0.1.10', 'accelerator v0.1.11'))
        self.before[module] = module.read_bytes()
        producer = ('RUNTIME_VERSION=runtime-' if OWNER == 'guest-runtime' else 'VERSION=') + preview
        package = self.run_release(producer, 'ACCELERATOR_VERSION=' + preview)
        workspace = Path(package['GOWORK'])
        document = json.loads(subprocess.check_output(['go', 'work', 'edit', '-json', workspace],
                                                      env=self.env))
        self.assertEqual([(workspace.parent / use['DiskPath']).resolve() for use in document['Use']],
                         [self.owner])
        bindings = {entry['Old']['Path']: (entry['Old'].get('Version'),
                                            (workspace.parent / entry['New']['Path']).resolve())
                    for entry in document.get('Replace') or []}
        self.assertEqual(bindings, {'github.com/kuasar-sandbox/accelerator': ('v0.1.11', accelerator)})

    def test_external_consumer_uses_public_stable_or_private_preview(self):
        telemetry = self.owner / 'app/telemetry'
        telemetry.mkdir(parents=True)
        (telemetry / 'telemetry.go').write_text(
            'package telemetry\nimport "github.com/kuasar-sandbox/accelerator"\nconst Version = accelerator.Selected\n')
        script = ['bash', str(ROOT/'test/source/go_consumer.sh'), str(self.owner)]
        go = shutil.which('go', path=self.env['PATH'])
        env = dict(self.env, KUASAR_CONSUMER_GO=go)
        for mode in ('off', 'auto'):
            subprocess.run(script, cwd=self.root, env=dict(env, GOWORK=mode), check=True, capture_output=True)
        module = self.owner / 'go.mod'
        module.write_text(module.read_text().replace('accelerator v0.1.10', 'accelerator v0.1.11'))
        self.before[module] = module.read_bytes()
        subprocess.run([go, 'work', 'init', './orchestrator'], cwd=self.root, env=env, check=True)
        work = self.root / 'go.work'
        subprocess.run([go, 'work', 'edit', '-go=1.26.0',
                        '-replace=github.com/kuasar-sandbox/accelerator@v0.1.11=./accelerator',
                        str(work)], cwd=self.root, env=env, check=True)
        before = work.read_bytes()
        for mode in (str(work), 'auto'):
            subprocess.run(script, cwd=self.root, env=dict(env, GOWORK=mode), check=True)
        self.assertEqual(work.read_bytes(), before)
        self.assertEqual(list(self.root.glob('consumer-*.work*')), [])

    def test_external_consumer_ignores_candidate_only_replaces_in_source_workspace(self):
        private = self.root / 'only-local'
        private.mkdir()
        (private / 'go.mod').write_text('module example.com/only-local\n\ngo 1.26.1\n')
        (private / 'local.go').write_text('package local\nconst V = 1\n')
        telemetry = self.owner / 'app/telemetry'
        telemetry.mkdir(parents=True)
        (telemetry / 'telemetry.go').write_text(
            'package telemetry\nimport local "example.com/only-local"\nconst Version = local.V\n')
        module = self.owner / 'go.mod'
        with module.open('a') as target:
            target.write('require example.com/only-local v0.1.0\nreplace example.com/only-local => ../only-local\n')
        self.before[module] = module.read_bytes()
        go = shutil.which('go', path=self.env['PATH'])
        subprocess.run([go, 'work', 'init', './orchestrator', './accelerator'], cwd=self.root,
                       env=self.env, check=True)
        env = dict(self.env, KUASAR_CONSUMER_GO=go, GOWORK=str(self.root / 'go.work'))
        subprocess.run([go, 'build', './app/telemetry'], cwd=self.owner, env=env, check=True)
        result = subprocess.run(['bash', str(ROOT/'test/source/go_consumer.sh'), str(self.owner)],
                                cwd=self.root, env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('example.com/only-local@v0.1.0', result.stderr)

    @unittest.skipIf(OWNER == 'orchestrator', 'no native sub-build')
    def test_actual_native_recipe_does_not_inherit_product_output_directory(self):
        native=self.owner/'native-deps'; native.mkdir()
        target='envd' if OWNER=='guest-runtime' else 'cloud-hypervisor'
        (self.owner/'Makefile').write_bytes((ROOT/'Makefile').read_bytes())
        with (self.owner/'Makefile').open('a') as f:
            f.write('\nshow-envd:\n\t@printf "%s" "$(ENVD)"\n')
        (native/'Makefile').write_text(target+':\n\t@mkdir -p "$(BINDIR)"\n\t@printf native-current > "$(BINDIR)/'+target+'"\n')
        output=self.owner/'bin/custom'
        result=subprocess.run([self.make,'-s',target,'TARGET_ARCH=x86_64','BINDIR='+str(output)],cwd=self.owner,env=self.env,text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual((native/'bin/x86_64'/target).read_text(),'native-current')
        if OWNER=='sandboxer':
            self.assertEqual((output/target).read_text(),'native-current')
        else:
            result=subprocess.check_output([self.make,'-s','show-envd','TARGET_ARCH=x86_64','BINDIR='+str(output)],cwd=self.owner,env=self.env,text=True)
            self.assertEqual(result,'native-deps/bin/x86_64/envd')

    def test_dry_run_uses_no_selection_tools_and_preserves_artifacts(self):
        (self.owner/'Makefile').write_bytes((ROOT/'Makefile').read_bytes())
        tools = self.root/'recording-tools'
        tools.mkdir()
        for command in ('go', 'python3', 'jq', 'bash'):
            tool = tools/command
            tool.write_text('#!/bin/sh\nprintf "%s\\n" "$0" >> "$TRACE"\nexit 9\n')
            tool.chmod(0o755)
        trace = self.root/'dry-run-trace'
        env = dict(self.env, TRACE=str(trace), PATH=str(tools)+os.pathsep+self.env['PATH'])
        prior = self.owner/'build/release-bundle/prior'
        prior.parent.mkdir(parents=True)
        prior.write_text('preserved')
        for version in ('v0.1.11', 'v0.1.11-preview.20261112'):
            variable = 'RUNTIME_VERSION=runtime-' if OWNER=='guest-runtime' else 'VERSION='
            result = subprocess.run([self.make, '-n', '-j2', self.target, variable+version, 'GO='+str(tools/'go')],
                                    cwd=self.owner, env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('go_workspace.py', result.stdout)
            self.assertFalse(trace.exists(), trace.read_text() if trace.exists() else '')
            self.assertEqual(prior.read_text(), 'preserved')

    def test_missing_tag_stops_before_build(self):
        subprocess.run(['git','-C',str(self.root/'accelerator'),'tag','-d','v0.1.10'],check=True,capture_output=True)
        self.run_release(success=False)

    def test_dirty_selected_dependency_stops_before_build(self):
        (self.root/'accelerator/extra.go').write_text('package accelerator\n')
        self.assertIn('uncommitted',self.run_release(success=False))

    def test_license_source_overrides_stop_before_build(self):
        for name in ('RELEASE_SOURCE_ROOT','RELEASE_ACCELERATOR_SOURCE_DIR'):
            with self.subTest(name=name):
                self.assertIn('source directory',self.run_release(name+'='+str(self.root/'elsewhere'),success=False))


if __name__ == '__main__':
    unittest.main()
