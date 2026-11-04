#!/usr/bin/env python3
"""Current-byte upload, publication recovery and independent Stable Latest lines."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('publisher',Path(__file__).with_name('release-publisher.py'))
subject=importlib.util.module_from_spec(spec);spec.loader.exec_module(subject)
SHA='a'*40


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);(self.root/'assets').mkdir()
        (self.root/'assets/package.tar.gz').write_bytes(b'validated bytes')
        (self.root/'release-notes.md').write_text('Human release notes.\n')
        self.repo='kuasar-sandbox/accelerator';self.tag='v1.2.3';self.target=None
        self.release=None;self.other=[];self.latest=None;self.calls=[];self.uploads=[];self.fail_upload=False
        self.publish_failure=False;self.latest_failure=False;self.now='2026-11-04T01:00:00Z'
        def names(*args):return ['package.tar.gz']
        def validation(*args,**kwargs):return subject.local_assets(self.root,names())
        self.patchers={}
        for name,options in [('names',dict(side_effect=names)),('validate_bundle',dict(side_effect=validation)),
          ('api',dict(side_effect=self.api)),('releases',dict(side_effect=lambda repo:copy.deepcopy(self.other+([self.release] if self.release else [])))),
          ('gh',dict(side_effect=self.gh)),('revalidate_preview',dict())]:
            patched=patch.object(subject,name,**options);self.patchers[name]=patched;setattr(self,name,patched.start());self.addCleanup(patched.stop)

    def api(self,endpoint,method='GET',data=None,optional=False):
        self.calls.append((method,endpoint,copy.deepcopy(data)))
        if endpoint.endswith('/git/ref/heads/main'):return {'object':{'type':'commit','sha':self.target or SHA}}
        if '/compare/' in endpoint:return {'status':'identical'}
        if '/git/ref/tags/' in endpoint:return {'object':{'type':'commit','sha':self.target}} if self.target else None
        if endpoint.endswith('/git/refs'):
            self.assertEqual(method,'POST');self.assertIsNone(self.target);self.target=data['sha'];return {}
        if endpoint.endswith('/releases/latest'):
            return copy.deepcopy(self.latest)
        if endpoint.endswith('/releases'):
            self.assertEqual(method,'POST');self.assertIsNone(self.release)
            self.assertEqual(data['make_latest'],'false')
            self.release=dict(data,id=12,assets=[],published_at=None);return copy.deepcopy(self.release)
        if endpoint.endswith('/releases/12'):
            if method=='PATCH':
                if data.get('make_latest')=='true':
                    if self.latest_failure:raise ValueError('Latest transport failed')
                    self.latest=copy.deepcopy(self.release)
                else:
                    self.assertEqual(data['make_latest'],'false');self.release.update(data,published_at=self.now)
                    if self.publish_failure:raise ValueError('undraft response lost')
            return copy.deepcopy(self.release)
        self.fail('unexpected API endpoint (no historical qualification allowed): '+endpoint)

    def gh(self,*args,data=None):
        self.assertEqual(args[:3],('release','upload',self.tag))
        self.assertNotIn('--clobber',args)
        path=Path(args[3]);self.uploads.append(path.name)
        if self.fail_upload:return subprocess.CompletedProcess(args,1,'','interrupted upload')
        payload=path.read_bytes();self.release['assets'].append(dict(id=100+len(self.uploads),name=path.name,size=len(payload),
            digest='sha256:'+hashlib.sha256(payload).hexdigest(),state='uploaded'))
        return subprocess.CompletedProcess(args,0,'','')

    def publish(self,source='main'):
        return subject.publish(self.repo,self.tag,'x86_64',SHA,self.root,source)

    def test_new_main_stable_verifies_upload_then_updates_latest(self):
        self.publish()
        self.assertEqual(self.release['body'],'Human release notes.\n')
        self.assertEqual(self.latest['id'],12);self.assertEqual(self.uploads,['package.tar.gz'])
        self.assertFalse(any(method=='DELETE' for method,_,_ in self.calls))
        self.assertEqual([data for method,url,data in self.calls if method=='PATCH'],
                         [{'draft':False,'prerelease':False,'make_latest':'false'},{'make_latest':'true'}])

    def test_preview_and_maintenance_never_touch_latest(self):
        self.tag='v1.2.3-preview.20261104.1';self.publish()
        self.assertIsNone(self.latest);self.assertEqual(self.revalidate_preview.call_count,2)
        self.release=None;self.target=None;self.calls.clear();self.tag='v1.2.4'
        self.publish('release/v1.2.x')
        self.assertIsNone(self.latest)
        self.assertFalse(any(url.endswith('/releases/latest') for _,url,_ in self.calls))

    def test_incomplete_draft_resumes_without_deletion_or_overwrite(self):
        self.fail_upload=True
        with self.assertRaisesRegex(ValueError,'interrupted'):self.publish()
        draft_id=self.release['id'];self.fail_upload=False
        self.publish()
        self.assertEqual(self.release['id'],draft_id);self.assertFalse(self.release['draft'])
        self.assertFalse(any(method=='DELETE' for method,_,_ in self.calls))

    def test_uploaded_draft_asset_is_reused_and_conflicts_are_retained(self):
        self.publish_failure=True
        with self.assertRaisesRegex(ValueError,'undraft'):self.publish()
        self.release['draft']=True;self.publish_failure=False
        self.publish();self.assertEqual(len(self.uploads),1)
        for field,value in [('digest','sha256:'+'f'*64),('size',99),('state','starter')]:
            original=copy.deepcopy(self.release)
            self.release['assets'][0][field]=value;self.calls.clear()
            with self.assertRaisesRegex(ValueError,'refusing overwrite'):self.publish()
            self.assertFalse(any(method!='GET' for method,_,_ in self.calls))
            self.release=original

    def test_ambiguous_asset_ids_and_extra_names_fail(self):
        self.publish()
        original=copy.deepcopy(self.release)
        for assets in ([dict(original['assets'][0],id=0)],original['assets']*2,
                       [dict(original['assets'][0],name='SHA256SUMS')]):
            self.release=copy.deepcopy(original);self.release['assets']=assets
            with self.assertRaises(ValueError):self.publish()
        self.assertEqual(len(self.uploads),1)

    def test_public_recovery_preserves_human_edits_and_does_not_upload(self):
        self.publish_failure=True
        with self.assertRaisesRegex(ValueError,'undraft'):self.publish()
        self.publish_failure=False;self.release['body']='Manually clarified human notes.'
        self.calls.clear();self.publish('release/v1.2.x')
        self.assertEqual(self.release['body'],'Manually clarified human notes.')
        self.assertEqual(len(self.uploads),1)
        self.assertTrue(all(method=='GET' for method,_,_ in self.calls))

    def test_latest_failure_recovers_from_same_published_bytes(self):
        self.latest_failure=True
        with self.assertRaisesRegex(ValueError,'Latest transport'):self.publish()
        self.assertFalse(self.release['draft']);self.latest_failure=False
        self.publish();self.assertEqual(self.latest['id'],12);self.assertEqual(len(self.uploads),1)

    def test_changed_local_bytes_cannot_recover_public_release(self):
        self.publish();(self.root/'assets/package.tar.gz').write_bytes(b'new bytes')
        with self.assertRaisesRegex(ValueError,'refusing overwrite'):self.publish()
        self.assertEqual(len(self.uploads),1)

    def test_wrong_tag_and_conflicting_draft_notes_are_retained(self):
        self.target='b'*40
        with self.assertRaisesRegex(ValueError,'different commit'):self.publish()
        self.assertIsNone(self.release)
        self.target=None;self.fail_upload=True
        with self.assertRaises(ValueError):self.publish()
        self.release['body']='manual draft changes';self.fail_upload=False
        with self.assertRaisesRegex(ValueError,'draft notes differ'):self.publish()
        self.assertEqual(self.release['body'],'manual draft changes')

    def test_bad_notes_fail_before_any_external_mutation(self):
        for text in ('x'*125001,'<!-- kuasar-preview-binding {} -->','<!-- kuasar-release-source {} -->',''):
            (self.root/'release-notes.md').write_text(text)
            with self.assertRaises(ValueError):self.publish()
            self.assertEqual(self.calls,[])

    def test_higher_same_unit_stable_and_large_versions_prevent_rollback(self):
        self.other=[dict(id=88,tag_name='v9223372036854775808.0.0',draft=False,prerelease=False)]
        self.publish();self.assertIsNone(self.latest)
        self.assertFalse(any(data=={'make_latest':'true'} for _,_,data in self.calls))

    def test_guest_runtime_units_do_not_compare_semver_across_lines(self):
        self.repo='kuasar-sandbox/guest-runtime';self.tag='runtime-v1.2.3'
        self.latest=dict(id=99,tag_name='vmlinux-v999.0.0',draft=False,prerelease=False,published_at='2026-11-03T01:00:00Z')
        self.publish();self.assertEqual(self.latest['id'],12)
        self.latest=dict(id=99,tag_name='vmlinux-v0.1.0',draft=False,prerelease=False,published_at='2026-11-05T01:00:00Z')
        self.publish();self.assertEqual(self.latest['id'],99)

    def test_unknown_cross_unit_timestamp_retains_latest(self):
        self.repo='kuasar-sandbox/guest-runtime';self.tag='vmlinux-v2.0.0'
        self.latest=dict(id=99,tag_name='runtime-v1.0.0',draft=False,prerelease=False,published_at=None)
        with self.assertRaisesRegex(ValueError,'time is unavailable'):self.publish()
        self.assertEqual(self.latest['id'],99)

    def test_asset_identity_change_between_upload_and_publication_is_rejected(self):
        original=self.api.side_effect
        def race(endpoint,**kwargs):
            result=original(endpoint,**kwargs)
            if kwargs.get('data',{}).get('draft') is False:
                self.release['assets'][0]['id']=999
            return result
        self.api.side_effect=race
        with self.assertRaisesRegex(ValueError,'identity changed'):self.publish()
        self.assertIsNone(self.latest)

    def test_existing_draft_asset_identity_cannot_change_on_retry(self):
        self.publish_failure=True
        with self.assertRaisesRegex(ValueError,'undraft'):self.publish()
        self.release['draft']=True;self.publish_failure=False
        original=self.api.side_effect
        def race(endpoint,**kwargs):
            if endpoint.endswith('/releases/12') and kwargs.get('method','GET')=='GET':
                self.release['assets'][0]['id']=999
            return original(endpoint,**kwargs)
        self.api.side_effect=race
        with self.assertRaisesRegex(ValueError,'identity changed'):self.publish()
        self.assertTrue(self.release['draft']);self.assertIsNone(self.latest)

    def test_wrong_repository_latest_unit_is_retained(self):
        self.latest=dict(id=99,tag_name='runtime-v1.0.0',draft=False,prerelease=False,published_at='2026-11-03T01:00:00Z')
        with self.assertRaisesRegex(ValueError,'unit/tag mismatch'):self.publish()
        self.assertEqual(self.latest['id'],99)

    def test_cross_unit_equal_time_or_invalid_time_retains_latest(self):
        self.repo='kuasar-sandbox/guest-runtime';self.tag='runtime-v1.2.3'
        self.latest=dict(id=99,tag_name='vmlinux-v1.0.0',draft=False,prerelease=False,published_at=self.now)
        self.publish();self.assertEqual(self.latest['id'],99)
        for value in ('2026-11-03T01:00:00','invalid',None):
            self.latest['published_at']=value
            with self.assertRaisesRegex(ValueError,'publication time'):self.publish()
            self.assertEqual(self.latest['id'],99)

    def test_same_latest_id_requires_same_tag_and_public_stable_state(self):
        self.publish()
        for changes in ({'tag_name':'v1.0.0'},{'tag_name':'v1.2.3-preview.20261104.1'},
                        {'draft':True},{'prerelease':True}):
            self.latest=dict(self.release,**changes)
            with self.assertRaises(ValueError):self.publish()

    def test_native_metadata_rejects_boolean_size_and_prerelease(self):
        (self.root/'assets/package.tar.gz').write_bytes(b'x')
        self.publish();self.release['assets'][0]['size']=True
        with self.assertRaisesRegex(ValueError,'asset size'):self.publish()
        self.release['assets'][0]['size']=1;self.release['prerelease']=0
        with self.assertRaisesRegex(ValueError,'Release state'):self.publish()

    def test_symlink_and_unexpected_local_asset_fail(self):
        extra=self.root/'assets/extra';extra.write_bytes(b'bad')
        with self.assertRaisesRegex(ValueError,'exactly'):self.publish()
        extra.unlink();path=self.root/'assets/package.tar.gz';path.unlink();path.symlink_to(self.root/'release-notes.md')
        with self.assertRaisesRegex(ValueError,'regular file'):self.publish()
        self.assertEqual(self.calls,[])


class AssemblyTests(unittest.TestCase):
    def prepare(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        root=Path(temporary.name)
        for arch in ('x86_64','aarch64'):
            bundle=root/arch;(bundle/'assets').mkdir(parents=True)
            (bundle/'assets'/('package-'+arch+'.tar.gz')).write_bytes(arch.encode())
            (bundle/'release-notes.md').write_text('Human notes preserved.\n')
        return root

    def test_dual_assembly_preserves_validated_bytes_and_human_notes(self):
        root=self.prepare()
        with patch.object(subject,'names',side_effect=lambda tag,arch:['package-'+arch+'.tar.gz']), \
             patch.object(subject,'validate_bundle') as validate:
            subject.assemble('v1.2.3',root/'x86_64',root/'aarch64',root/'output')
        self.assertEqual(validate.call_count,3)
        for arch in ('x86_64','aarch64'):
            self.assertEqual((root/'output/assets'/('package-'+arch+'.tar.gz')).read_bytes(),arch.encode())
        self.assertEqual((root/'output/release-notes.md').read_text(),'Human notes preserved.\n')

    def test_conflicting_architecture_notes_require_manual_reconciliation(self):
        root=self.prepare();(root/'aarch64/release-notes.md').write_text('Manual clarification')
        with patch.object(subject,'validate_bundle') as validate, self.assertRaisesRegex(ValueError,'notes differ'):
            subject.assemble('v1.2.3',root/'x86_64',root/'aarch64',root/'output')
        validate.assert_not_called();self.assertFalse((root/'output').exists())


class WorkflowTests(unittest.TestCase):
    def test_only_publish_steps_can_update_latest_and_share_repository_queue(self):
        root=Path(__file__).resolve().parents[1]
        self.assertFalse((root/'.github/workflows/reconcile-latest.yml').exists())
        publications=0
        for path in (root/'.github/workflows').glob('*.yml'):
            text=path.read_text()
            if '\n  publish:\n' not in text:continue
            publications+=1
            job=text.split('\n  publish:\n',1)[1]
            self.assertIn('    concurrency:\n      group: component-publication-${{ github.repository }}\n      queue: max\n      cancel-in-progress: false\n',job)
            self.assertNotIn('RELEASE_DEPENDENCIES:',job)
            build=text.split('\n  build:\n',1)[1].split('\n  publish:\n',1)[0]
            self.assertNotIn('bash scripts/release.sh',build)
            self.assertIn('ref: ${{ github.workflow_sha }}\n          path: trusted/'+root.name,build)
            self.assertIn('git -C trusted/',build)
            self.assertIn('archive --format=tar HEAD scripts',build)
            self.assertIn('RELEASE_SOURCE_ROOT="$PWD" bash /inputs/release/.ci/component-packaging/scripts/release.sh package',build)
            self.assertIn('RELEASE_SOURCE_ROOT="$PWD" bash /inputs/release/.ci/component-packaging/scripts/release.sh validate',build)

        self.assertEqual(publications,2 if root.name=='guest-runtime' else 1)


class TagTests(unittest.TestCase):
    def test_annotated_tag_and_cycle(self):
        with patch.object(subject,'api',side_effect=[{'object':{'type':'tag','sha':'b'*40}},{'object':{'type':'commit','sha':SHA}}]):
            self.assertEqual(subject.tag_target('kuasar-sandbox/sandboxer','v1.2.3'),SHA)
        with patch.object(subject,'api',return_value={'object':{'type':'tag','sha':'b'*40}}):
            with self.assertRaisesRegex(ValueError,'invalid tag'):subject.tag_target('kuasar-sandbox/sandboxer','v1.2.3')

    def test_only_confirmed_http_404_is_absent(self):
        for message in ('network failed','Forbidden (HTTP 403)'):
            with patch.object(subject,'gh',return_value=subprocess.CompletedProcess([],1,'',message)):
                with self.assertRaises(ValueError):subject.api('endpoint',optional=True)
        with patch.object(subject,'gh',return_value=subprocess.CompletedProcess([],1,'','Not Found (HTTP 404)')):
            self.assertIsNone(subject.api('endpoint',optional=True))


def fixture_bundle(arguments):
    publisher,bundle,repository,tag,commit,source_ref=arguments
    harness=PublisherTests();harness.setUp()
    original_script=subject.SCRIPT
    try:
        harness.patchers['names'].stop();harness.patchers['validate_bundle'].stop()
        subject.SCRIPT=Path(publisher).resolve().parent
        harness.root=Path(bundle);harness.repo=repository;harness.tag=tag
        # The standalone source-admission tests exercise preview policy. Here the
        # real package validator and publisher share the actual fixture bytes.
        arch=os.environ.get('PUBLISHER_TEST_ARCH','x86_64')
        expected=subject.names(tag,arch)
        subject.publish(repository,tag,arch,commit,harness.root,source_ref)
        assert set(harness.uploads)==set(expected) and 'SHA256SUMS' not in harness.uploads
        assert harness.release['body']==(harness.root/'release-notes.md').read_text()
        first_uploads=list(harness.uploads)
        subject.publish(repository,tag,arch,commit,harness.root,source_ref)
        assert harness.uploads==first_uploads
    finally:
        subject.SCRIPT=original_script;harness.doCleanups()
    print('Actual fixture bundle validated, uploaded and recovered:',repository,tag,arch)


if __name__=='__main__':
    if sys.argv[1:2]==['--bundle-test']:fixture_bundle(sys.argv[2:])
    else:unittest.main()

