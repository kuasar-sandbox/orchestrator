#!/usr/bin/env python3
"""Public HTTP product-process benchmark; only sandbox execution is simulated."""
import concurrent.futures,http.client,json,os,pathlib,statistics,subprocess,time,urllib.request

PORT=int(os.environ['CLUSTER_BENCH_PORT'])
GROUP=os.environ['CLUSTER_BENCH_GROUP']
KEY=os.environ['CLUSTER_BENCH_KEY']
DOMAIN=os.environ['CLUSTER_BENCH_DOMAIN']
PIDS=[int(x) for x in os.environ['CLUSTER_BENCH_PIDS'].split()]
COUNT=int(os.environ.get('CLUSTER_BENCH_COUNT','200'))
OUTPUT=pathlib.Path(os.environ['CLUSTER_BENCH_OUTPUT'])
RESULT={'backend':os.environ['CLUSTER_BENCH_BACKEND'],'nodes':int(os.environ['CLUSTER_BENCH_NODES']),'product_processes':len(PIDS),'vm_execution':'stub','samples':COUNT,'quantile_method':'sorted order statistic at floor((n-1)*p)','clock_ticks_per_second':os.sysconf('SC_CLK_TCK'),'phases':{}}

def database_counters():
    # Optional externally supplied CLI argv, against this fixture's isolated
    # database. These are server-wide sampled counters, not per-request counts.
    command=os.environ.get('CLUSTER_BENCH_FDB_STATUS_COMMAND')
    if not command:return None
    time.sleep(5)  # Outside measured phases; allow the server sample to refresh.
    result=subprocess.run(json.loads(command),check=True,capture_output=True,text=True,timeout=15)
    status=json.loads(result.stdout)
    return {name:value['counter'] for name,value in status['cluster']['workload']['transactions'].items() if 'counter' in value}

def usage():
    out={}
    for pid in PIDS:
        try:
            status=pathlib.Path(f'/proc/{pid}/status').read_text().splitlines()
            fields={line.split(':')[0]:line.split(':',1)[1].strip() for line in status}
            stat=pathlib.Path(f'/proc/{pid}/stat').read_text().split()
            out[str(pid)]={'name':fields['Name'],'rss_kib':int(fields.get('VmRSS','0 kB').split()[0]),'peak_rss_kib':int(fields.get('VmHWM','0 kB').split()[0]),'cpu_ticks':int(stat[13])+int(stat[14])}
        except FileNotFoundError:pass
    return out

def request(method,path,route,body=None,token=None,sid=None):
    headers={'Host':'api.'+DOMAIN,'X-Kuasar-Sandbox-Group':GROUP,'X-Kuasar-Route-Key':route}
    if token is None:headers['X-API-KEY']=KEY
    else:headers.update({'Host':'49983-'+sid+'.'+DOMAIN,'X-Access-Token':token})
    if body is not None:headers['Content-Type']='application/json';body=json.dumps(body)
    conn=http.client.HTTPConnection('127.0.0.1',PORT,timeout=20)
    begin=time.perf_counter_ns()
    try:
        conn.request(method,path,body,headers)
        response=conn.getresponse();raw=response.read()
        elapsed=(time.perf_counter_ns()-begin)/1e6
        return response.status,raw,elapsed
    finally:conn.close()

def measure(name,fn,count,workers=1):
    before=usage();start=time.perf_counter();values=[];statuses={}
    def item(i):
        status,raw,elapsed=fn(i)
        if status>=400:raise RuntimeError(name+' status '+str(status))
        return status,elapsed
    if workers==1:results=map(item,range(count))
    else:
        executor=concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        results=executor.map(item,range(count))
    for status,elapsed in results:
        values.append(elapsed);statuses[str(status)]=statuses.get(str(status),0)+1
    if workers!=1:executor.shutdown()
    wall=time.perf_counter()-start;values.sort()
    def pct(p):return values[min(len(values)-1,int((len(values)-1)*p))]
    RESULT['phases'][name]={'n':count,'concurrency':workers,'rps':count/wall,'wall_s':wall,'p50_ms':pct(.5),'p95_ms':pct(.95),'p99_ms':pct(.99),'mean_ms':statistics.mean(values),'latencies_ms':values,'status_counts':statuses,'processes_before':before,'processes_after':usage()}
    OUTPUT.write_text(json.dumps(RESULT,indent=2))
    print(json.dumps({'phase':name,**{k:v for k,v in RESULT['phases'][name].items() if not k.startswith('processes') and k != 'latencies_ms'}}),flush=True)

RESULT['database_counters_before']=database_counters()
status,raw,_=request('POST','/sandboxes','benchmark/existing',{})
assert status==201,status
existing=json.loads(raw);sid=existing['sandboxID'];token=existing['envdAccessToken']
measure('ensure_existing',lambda i:request('POST','/sandboxes','benchmark/existing',{}),COUNT*5)
measure('control_get',lambda i:request('GET','/sandboxes/'+sid,'benchmark/existing'),COUNT*5)
measure('data_cached',lambda i:request('GET','/health','benchmark/existing',token=token,sid=sid),COUNT*10)
created=[]
def create(i,prefix):
    result=request('POST','/sandboxes',prefix+str(i),{})
    if result[0]==201:created.append((prefix+str(i),json.loads(result[1])['sandboxID']))
    return result
measure('first_create_serial',lambda i:create(i,'benchmark/serial/'),COUNT)
measure('first_create_concurrent',lambda i:create(i,'benchmark/concurrent/'),COUNT,8)
admin=os.environ.get('CLUSTER_BENCH_ADMIN')
identifiers={sid for _,sid in created}
def pending_instances():
    with urllib.request.urlopen(admin+'/v1/nodes',timeout=5) as response:nodes=json.load(response)
    return sum(s.get('cluster',{}).get('stable_id') in identifiers for node in nodes for s in node.get('sandboxes',[]))
if admin and pending_instances()!=len(identifiers):raise RuntimeError('cleanup observation cannot account for every created instance')
measure('delete',lambda i:request('DELETE','/sandboxes/'+created[i][1],created[i][0]),len(created),8)
RESULT['post_delete_processes']=usage()
if admin:
    start=time.perf_counter();observations=[]
    while True:
        pending=pending_instances()
        observations.append({'elapsed_ms':1000*(time.perf_counter()-start),'pending_local_instances':pending})
        if pending==0 or time.perf_counter()-start>=10:break
        time.sleep(.05)
    RESULT['cleanup']={'scope':'only the instances created by the two measured Create phases; Node execution is simulated','created':len(identifiers),'observations':observations}
    if pending:raise RuntimeError('benchmark-created instances remain after Delete')
RESULT['database_counters_after']=database_counters()
if RESULT['database_counters_before'] is not None:
    RESULT['database_counter_delta']={key:value-RESULT['database_counters_before'][key] for key,value in RESULT['database_counters_after'].items()}
    RESULT['database_counter_scope']='isolated FDB server sampled totals; includes background work and status requests; not per-API transaction/conflict attribution'
OUTPUT.write_text(json.dumps(RESULT,indent=2))
