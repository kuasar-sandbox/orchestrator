#!/usr/bin/env python3
import copy
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('deletion', Path(__file__).with_name('delete-preview.py'))
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)
SHA = 'a' * 40


class DeletionTests(unittest.TestCase):
    def setUp(self):
        owner = Path(__file__).resolve().parents[1].name
        self.unit = 'runtime' if owner == 'guest-runtime' else owner
        self.repo = 'kuasar-sandbox/' + owner
        self.tag = ('runtime-' if self.unit == 'runtime' else '') + 'v1.2.3-preview.20261104.1'
        self.ref = {'object': {'type': 'commit', 'sha': SHA}}
        self.release = dict(id=1, tag_name=self.tag, target_commitish=SHA, draft=True,
                            prerelease=False, assets=[])
        self.deleted, self.errors = [], []
        def api(endpoint, optional=False):
            if '/git/ref/tags/' in endpoint:
                return copy.deepcopy(self.ref)
            if endpoint.endswith('/releases/1'):
                return copy.deepcopy(self.release)
            self.fail('unexpected endpoint: ' + endpoint)
        def gh(*args):
            self.assertEqual(args[:2], ('--method', 'DELETE'))
            endpoint = args[2]
            self.deleted.append(endpoint)
            error = self.errors.pop(0) if self.errors else None
            if error and '(HTTP 404)' not in error:
                return subprocess.CompletedProcess(args, 1, '', error)
            if '/releases/' in endpoint:
                self.release = None
            else:
                self.ref = None
            return subprocess.CompletedProcess(args, 1 if error else 0, '', error or '')
        for name, options in [('api', dict(side_effect=api)),
                ('gh', dict(side_effect=gh)),
                ('release_for', dict(side_effect=lambda *args: copy.deepcopy(self.release)))]:
            patcher = mock.patch.object(subject, name, **options)
            setattr(self, name, patcher.start()); self.addCleanup(patcher.stop)

    def collect(self, mode='incomplete'):
        return subject.collect(self.repo, self.unit, self.tag, SHA, mode)

    def packages(self, both=True):
        names = sorted(subject.package_names(self.unit, self.tag))
        self.release.update(draft=False, prerelease=True, assets=[
            dict(id=i, name=name, size=10, digest='sha256:'+'b'*64, state='uploaded')
            for i, name in enumerate(names if both else [name for name in names if 'x86_64' in name], 1)])

    def test_complete_old_and_new_packages_survive_incomplete_cleanup(self):
        for both in (False, True):
            for old in (False, True):
                with self.subTest(both=both, old=old):
                    self.packages(both)
                    if old:
                        self.release['assets'].append({'name': 'SHA256SUMS'})
                    with self.assertRaisesRegex(ValueError, 'complete preview'):
                        self.collect()
                    self.assertEqual(self.deleted, [])

    def test_gc_removes_confirmed_packages_and_tag(self):
        self.packages(); self.assertEqual(self.collect('gc'), ['release', 'tag'])
        self.assertEqual(self.collect('gc'), [])

    def test_partial_delete_retry(self):
        self.errors = [None, 'network disconnected']
        with self.assertRaisesRegex(ValueError, 'network'):
            self.collect()
        self.assertIsNone(self.release); self.assertIsNotNone(self.ref)
        self.assertEqual(self.collect(), ['tag'])

    def test_release_only_and_tag_only(self):
        self.ref = None
        self.assertEqual(self.collect(), ['release'])
        self.ref = {'object': {'type': 'commit', 'sha': SHA}}
        self.assertEqual(self.collect(), ['tag'])

    def test_tagless_unknown_or_changed_source_survives(self):
        self.ref = None
        for source in ('main', 'b' * 40, None):
            self.release['target_commitish'] = source
            with self.assertRaisesRegex(ValueError, 'tagless Release source'):
                self.collect()
        self.assertEqual(self.deleted, [])

    def test_existing_tag_identity_ignores_display_target(self):
        self.release['target_commitish'] = 'main'
        self.assertEqual(self.collect(), ['release', 'tag'])

    def test_unknown_asset_metadata_survives_gc(self):
        self.packages(); original = copy.deepcopy(self.release)
        for field, value in (('id', None), ('id', True), ('size', 0), ('digest', None),
                             ('digest', 'sha256:no'), ('state', None), ('state', 'unknown')):
            with self.subTest(field=field, value=value):
                self.release = copy.deepcopy(original)
                self.release['assets'][0][field] = value
                with self.assertRaisesRegex(ValueError, 'unknown native asset metadata'):
                    self.collect('gc')
        self.assertEqual(self.deleted, [])

    def test_unknown_names_and_duplicate_assets_survive(self):
        self.packages(); original = copy.deepcopy(self.release)
        for field, value in (('name', 'unexpected.tar.gz'),
                             ('name', original['assets'][1]['name']),
                             ('id', original['assets'][1]['id'])):
            self.release = copy.deepcopy(original); self.release['assets'][0][field] = value
            with self.assertRaises(ValueError): self.collect('gc')
        self.assertEqual(self.deleted, [])

    def test_non_preview_immutable_and_moved_tag_survive(self):
        for change in ({'draft': False, 'prerelease': False}, {'immutable': True}, {'id': None}):
            original = self.release; self.release = dict(original, **change)
            with self.assertRaises(ValueError): self.collect('gc')
            self.release = original
        self.ref['object']['sha'] = 'b'*40
        with self.assertRaisesRegex(ValueError, 'tag source identity'): self.collect('gc')
        self.assertEqual(self.deleted, [])

    def test_concurrent_release_change_prevents_deletion(self):
        first = copy.deepcopy(self.release)
        self.release_for.side_effect = [first, dict(first, id=2)]
        with self.assertRaisesRegex(ValueError, 'Release changed'): self.collect()
        self.assertEqual(self.deleted, [])

    def test_recreated_release_protects_tag(self):
        initial = copy.deepcopy(self.release)
        self.release_for.side_effect = [initial, initial, dict(initial, id=2)]
        with self.assertRaisesRegex(ValueError, 'Release appeared'): self.collect()
        self.assertIsNotNone(self.ref); self.assertEqual(len(self.deleted), 1)

    def test_tag_disappearing_after_release_delete_is_converged(self):
        original = self.api.side_effect
        def observed(endpoint, optional=False):
            if self.release is None and '/git/ref/tags/' in endpoint:
                self.ref = None
            return original(endpoint, optional=optional)
        self.api.side_effect = observed
        self.assertEqual(self.collect(), ['release', 'tag'])
        self.assertEqual(len(self.deleted), 1)

    def test_confirmed_404_is_idempotent_but_permission_error_is_not(self):
        self.errors = ['permission denied (HTTP 403)']
        with self.assertRaisesRegex(ValueError, '403'): self.collect()
        self.assertIsNotNone(self.release)
        self.errors = ['Not Found (HTTP 404)', 'Not Found (HTTP 404)']
        self.assertEqual(self.collect(), ['release', 'tag'])

    def test_arm_only_remote_release_is_incomplete(self):
        self.packages()
        self.release['assets'] = [row for row in self.release['assets'] if 'aarch64' in row['name']]
        self.assertEqual(self.collect(), ['release', 'tag'])

    def test_annotated_tag_and_cycle(self):
        self.api.side_effect = lambda *a, **kw: {'object': {'type': 'commit', 'sha': SHA}}
        self.assertEqual(subject.tag_target(self.repo, {'object': {'type': 'tag', 'sha': 'b'*40}}), SHA)
        self.api.side_effect = lambda *a, **kw: {'object': {'type': 'tag', 'sha': 'b'*40}}
        with self.assertRaisesRegex(ValueError, 'cyclic'):
            subject.tag_target(self.repo, {'object': {'type': 'tag', 'sha': 'b'*40}})

    def test_runtime_and_kernel_names_are_independent(self):
        self.assertEqual(subject.package_names('runtime', 'runtime-v1.2.3-preview.20261104'),
                         {f'sandbox-runtime-{arch}-v1.2.3-preview.20261104.tar.gz' for arch in ('x86_64', 'aarch64')})
        self.assertEqual(subject.package_names('vmlinux', 'vmlinux-v4.5.6-preview.20261104'),
                         {f'vmlinux-{arch}-v4.5.6-preview.20261104.tar.gz' for arch in ('x86_64', 'aarch64')})


class ApiErrorsTests(unittest.TestCase):
    def test_only_confirmed_404_is_absence(self):
        for error in ('network failed', 'Forbidden (HTTP 403)'):
            with mock.patch.object(subject, 'gh', return_value=subprocess.CompletedProcess([], 1, '', error)):
                with self.assertRaises(ValueError): subject.api('endpoint', optional=True)
        with mock.patch.object(subject, 'gh', return_value=subprocess.CompletedProcess([], 1, '', 'Not Found (HTTP 404)')):
            self.assertIsNone(subject.api('endpoint', optional=True))
            with self.assertRaises(ValueError): subject.api('endpoint')


if __name__ == '__main__': unittest.main()
