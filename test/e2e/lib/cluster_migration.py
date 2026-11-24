import base64, json, os, pathlib, signal, sqlite3, time, urllib.error, urllib.parse, urllib.request
work=pathlib.Path(os.environ['MIGRATION_WORK']);out=pathlib.Path(os.environ['MIGRATION_OUT'])
sid=os.environ['MIGRATION_SID'];group=os.environ['MIGRATION_GROUP'];route=os.environ['MIGRATION_ROUTE']
host='api.'+os.environ['MIGRATION_DOMAIN'];key=os.environ['MIGRATION_API_KEY']
router='http://127.0.0.1:'+os.environ['MIGRATION_ROUTER_PORT'];node='http://127.0.0.1:'+os.environ['MIGRATION_NODE_PORT']
created=json.loads((work/'migration-create.secret').read_text());exported=json.loads((work/'migration-export.secret').read_text())
assert exported['result'].startswith('kmt1.') and '@runtime:' in exported['snapshotRef'], 'missing actual annotated Snapshot token'
def request(base,method,path,body=None,extra=None):
 headers={'Host':host,'X-API-KEY':key,'X-Kuasar-Sandbox-Group':group,'X-Kuasar-Route-Key':route,'Content-Type':'application/json'}
 headers.update(extra or {})
 req=urllib.request.Request(base+path,headers=headers,data=None if body is None else json.dumps(body).encode(),method=method)
 try:response=urllib.request.urlopen(req,timeout=120)
 except urllib.error.HTTPError as e:response=e
 with response:
  raw=response.read()
  try:body=json.loads(raw) if raw else {}
  except json.JSONDecodeError:body={'error':raw.decode(errors='replace')}
  return response.code,body
def row(path):
 with sqlite3.connect(path) as db:
  db.row_factory=sqlite3.Row
  found=db.execute('select id,stable_id,run_id,state from sandboxes where stable_id=?',(sid,)).fetchall()
 assert len(found)<=1
 return dict(found[0]) if found else None
source=row(work/'cl/node-ctl.db');assert source and source['state']=='paused',source
# A different, unavailable default runtime must not replace the token's actual
# captured input. Registry sees only the opaque token, Node performs decoding.
path='/groups?'+urllib.parse.urlencode({'group':group})
status,detail=request(node,'GET',path);assert status==200
wrong=exported['snapshotRef'].split('@runtime:')[0]+'@runtime:unavailable.bundle:digest:'+'0'*64
template='e2b-snp-'+base64.urlsafe_b64encode(wrong.encode()).decode().rstrip('=')
context=dict(detail['context']);context['templateID']=template
status,changed=request(node,'PUT',path,{'expected_version':detail['version'],'context':context});assert status==200,(status,changed)
time.sleep(1)
pid=int(os.environ['MIGRATION_SOURCE_PID'])
assert str(work/'cluster-node.yaml').encode() in pathlib.Path('/proc/'+str(pid)+'/cmdline').read_bytes(), 'unexpected source PID'
os.kill(pid,signal.SIGKILL)
time.sleep(1)
status,connected=request(router,'POST','/sandboxes/'+sid+'/connect',{}, {'X-Kuasar-Migration-Token':exported['result']})
assert status in (200,201),('migration Connect',status)
assert connected['sandboxID']==sid
for field in ('envdAccessToken','trafficAccessToken','forwardAccessToken'):
 assert connected[field]==created[field],field+' changed'
for _ in range(600):
 target=row(work/'target/cl/node-ctl.db')
 if target and target['state']=='running':break
 time.sleep(.1)
else:raise AssertionError('migrated VM not running')
assert source['id']!=target['id'] and source['stable_id']==target['stable_id']==sid
(out/'migration-results.json').write_text(json.dumps({'source':source,'target':target,'public_status':status,'stable_credentials':True,'target_default_runtime_unavailable':True,'runtime_source':'Node-decoded opaque KMT'},indent=2)+'\n')
