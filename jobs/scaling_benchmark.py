from pathlib import Path
import argparse,json,time,datetime,hashlib,statistics
from pyspark.sql import SparkSession
from optimized_views import install_views,YEAR_FILTERS
ROOT=Path('/mnt/data/woosang/TPC-H/node-scaling');DB='tpch_sf100_optimized'
p=argparse.ArgumentParser();p.add_argument('--nodes',type=int,choices=[2,3,4,5,6],required=True);p.add_argument('--hosts',required=True);p.add_argument('--output',required=True);a=p.parse_args();hosts=set(a.hosts.split(','));assert len(hosts)==a.nodes
out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
s=SparkSession.builder.appName('TPC-H-SF100-scaling-%dnodes'%a.nodes).enableHiveSupport().getOrCreate();s.sparkContext.setLogLevel('WARN')
assert s.conf.get('spark.sql.adaptive.enabled')=='false';assert s.conf.get('spark.sql.cbo.enabled')=='true'
assert int(s.sparkContext.getConf().get('spark.executor.instances'))==a.nodes
assert json.loads((ROOT/'results/preparation.json').read_text())['status']=='complete'
reference=json.loads((ROOT/'results/reference-hashes.json').read_text())
def placement():
 entries=s.sparkContext._jsc.sc().statusStore().executorList(True)
 aliases={'tpch-scaling-'+h.split('.')[-1]:h for h in hosts}
 return [{'id':entries.apply(i).id(),'host':aliases.get(entries.apply(i).hostPort().rsplit(':',1)[0],entries.apply(i).hostPort().rsplit(':',1)[0]),'cores':entries.apply(i).totalCores()} for i in range(entries.size()) if entries.apply(i).id()!='driver']
def valid_placement():
 e=placement();assert len(e)==a.nodes and {x['host'] for x in e}==hosts and all(x['cores']==7 for x in e),e
 return e
def canonical(rows):
 h=hashlib.sha256()
 for row in sorted(json.dumps(r,sort_keys=True,default=str,separators=(',',':')) for r in rows):h.update(row.encode());h.update(b'\n')
 return h.hexdigest()
status={'status':'running','nodes':a.nodes,'completed_executions':0,'started_at':datetime.datetime.now().isoformat()}
def save():(out/'status.json').write_text(json.dumps(status,indent=2))
try:
 deadline=time.monotonic()+300
 while True:
  e=placement()
  if len(e)==a.nodes:break
  assert time.monotonic()<deadline,('Executor startup timeout',e)
  time.sleep(2)
 e=valid_placement();s.catalog.setCurrentDatabase(DB)
 # Validate the actual file-scan RDD hostnames against registered executor hostnames.
 def scan_rdd(r):
  if r.getClass().getName().endswith('FileScanRDD'):return r
  d=r.dependencies()
  for i in range(d.size()):
   found=scan_rdd(d.apply(i).rdd())
   if found is not None:return found
  return None
 leaf=scan_rdd(s.sql('SELECT l_orderkey,l_quantity FROM '+DB+'.lineitem WHERE l_ship_year=1994')._jdf.queryExecution().toRdd())
 assert leaf is not None,'Missing file-scan RDD for locality validation'
 partitions=leaf.partitions();preferred=[set(leaf.preferredLocations(partitions[i]).mkString(',').split(',')) for i in range(min(12,len(partitions)))]
 assert preferred and all(p and p<=hosts and '' not in p for p in preferred),('HDFS/Spark hostname mismatch',preferred,hosts)
 (out/'locality-preflight.json').write_text(json.dumps({'registered_hosts':sorted(hosts),'file_preferred_hosts':[sorted(p) for p in preferred],'validated':True},indent=2))

 (out/'configuration.json').write_text(json.dumps({'nodes':a.nodes,'hosts':sorted(hosts),'executor_placement':e,'sf':100,'replication':2,'warmup':1,'repeat':3,'spark_conf':dict(s.sparkContext.getConf().getAll()),'design':'compute_and_datanode_scaling','timing_scope':'spark.sql through collect; views and validation excluded','cache_policy':'full warmup, no OS cache flush','node_hardware':'heterogeneous; fixed eight physical CPUs and 64GiB YARN per node','network':s.sparkContext.getConf().get('spark.tpch.network','WireGuard via server 43')},indent=2))
 save()
 with (out/'timings.jsonl').open('w',buffering=1) as log:
  for run in range(4):
   for q in range(1,23):
    valid_placement();views=install_views(s,DB,q);sql=(ROOT/'sql'/('q%02d.sql'%q)).read_text()
    if q==11:sql=sql.replace('0.0001000000','(0.0001000000 / 100)')
    s.sparkContext.setJobGroup('scaling-n%d-r%d-q%02d'%(a.nodes,run,q),'TPC-H scaling',interruptOnCancel=True)
    start_epoch=time.time();start=time.perf_counter();df=s.sql(sql);rows=[r.asDict(recursive=True) for r in df.collect()];seconds=time.perf_counter()-start;end_epoch=time.time()
    valid_placement();digest=canonical(rows);valid=len(rows)==reference[str(q)]['rows'] and digest==reference[str(q)]['sha256'];assert valid,('Result mismatch',q,len(rows),digest)
    plan=df._jdf.queryExecution().executedPlan().toString();features={'broadcast_hash_joins':plan.count('BroadcastHashJoin'),'bucketed_scans':plan.count('Bucketed: true'),'year_partition_filter_present':any(('l_ship_year' in l or 'o_order_year' in l) for l in plan.splitlines() if 'PartitionFilters:' in l)}
    record={'sf':100,'nodes':a.nodes,'query':q,'run':run,'warmup':run==0,'seconds':seconds,'start_epoch':start_epoch,'end_epoch':end_epoch,'rows':len(rows),'result_multiset_sha256':digest,'result_validation_passed':True,'plan_features':features};log.write(json.dumps(record)+'\n')
    (out/('q%02d-plan.txt'%q)).write_text(df._jdf.queryExecution().toString());(out/('q%02d-effective.sql'%q)).write_text(sql)
    status.update(completed_executions=status['completed_executions']+1,last_query=q,last_run=run);save();print(record,flush=True)
 status.update(status='complete',finished_at=datetime.datetime.now().isoformat());save()
except BaseException as e:status.update(status='failed',error=repr(e));save();raise
finally:s.stop()
