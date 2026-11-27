#!/usr/bin/env python3
"""Explicit opt-in benchmark runner; installation/preparation never calls this."""
import argparse,datetime,json,time,hashlib
from pathlib import Path
from pyspark.sql import SparkSession
p=argparse.ArgumentParser()
p.add_argument('--sf',type=int,choices=[1,10,30],default=10)
p.add_argument('--aqe',choices=['off','on'],default='off')
p.add_argument('--layout',choices=['base','partitioned','bucketed','optimized'],default='base')
p.add_argument('--repeat',type=int,default=3)
p.add_argument('--warmup',type=int,default=1)
p.add_argument('--queries',default='1-22')
p.add_argument('--shuffle-join',action='store_true',help='Disable automatic broadcasting to inspect bucket/shuffle joins')
a=p.parse_args()
assert a.repeat>0 and a.warmup>=0
query_ids=[]
for part in a.queries.split(','):
 if '-' in part:
  lo,hi=map(int,part.split('-'));query_ids.extend(range(lo,hi+1))
 else:query_ids.append(int(part))
assert query_ids and all(1<=q<=22 for q in query_ids)
root=Path('/home/junhee/woosang/TPC-H')
preparation_file=root/'results'/('preparation-sf%d-optimized.json'%a.sf if a.layout=='optimized' else 'preparation.json' if a.sf==10 else 'preparation-sf%d.json'%a.sf)
assert json.loads(preparation_file.read_text())['status']=='complete','Data preparation must complete first'
out=root/'results'/('benchmark-sf%d-%s-'%(a.sf,a.layout)+datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
out.mkdir(parents=True)
spark=(SparkSession.builder.appName('TPC-H-SF%d-explicit-benchmark'%a.sf).config('spark.sql.adaptive.enabled',str(a.aqe=='on').lower()).enableHiveSupport().getOrCreate())
assert spark.conf.get('spark.sql.adaptive.enabled')==str(a.aqe=='on').lower()
spark.sparkContext.setLogLevel('WARN')
db='tpch_sf%d'%a.sf+{'base':'','partitioned':'_partitioned','bucketed':'_bucketed','optimized':'_optimized'}[a.layout]
spark.catalog.setCurrentDatabase(db)
if a.layout=='optimized':
 from optimized_views import install_views,YEAR_FILTERS
 assert a.aqe=='off' and not a.shuffle_join
 optimized_conditions={'broadcast_dimensions':['nation','region'],'year_filters_by_query':YEAR_FILTERS,'buckets':32,'all_optimizations':True}
else:optimized_conditions=None
if a.shuffle_join:
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
 spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',-1)
(out/'configuration.json').write_text(json.dumps({'arguments':vars(a),'database':db,'optimized_conditions':optimized_conditions,'spark_conf':dict(spark.sparkContext.getConf().getAll()),'note':'Warmups do not flush OS/HDFS caches; year partition pruning needs explicit year predicates. This is not an audited TPC-H score.'},indent=2))
try:
 with (out/'timings.jsonl').open('w') as log:
  for run in range(a.warmup+a.repeat):
   for q in query_ids:
    sql=(root/'sql'/('q%02d.sql'%q)).read_text()
    if q==11:
     assert sql.count('0.0001000000')==1
     sql=sql.replace('0.0001000000','(0.0001000000 / %d)'%a.sf)
    (out/('q%02d-effective.sql'%q)).write_text(sql)
    if a.layout=='optimized':
     definitions=install_views(spark,db,q)
     (out/('q%02d-views.json'%q)).write_text(json.dumps(definitions,indent=2))
    start_epoch=time.time()
    start=time.perf_counter()
    df=spark.sql(sql)
    rows=df.collect()
    elapsed=time.perf_counter()-start
    end_epoch=time.time()
    data=[r.asDict(recursive=True) for r in rows]
    encoded=json.dumps(data,default=str,sort_keys=True).encode()
    physical=df._jdf.queryExecution().executedPlan().toString()
    features={'broadcast_hash_joins':physical.count('BroadcastHashJoin'),'broadcast_nested_loop_joins':physical.count('BroadcastNestedLoopJoin'),'bucketed_scans':physical.count('Bucketed: true'),'year_partition_filter_present':'PartitionFilters:' in physical and ('l_ship_year' in physical or 'o_order_year' in physical)}
    record={'start_epoch':start_epoch,'end_epoch':end_epoch,'query':q,'run':run,'warmup':run<a.warmup,'seconds':elapsed,'rows':len(rows),'result_sha256':hashlib.sha256(encoded).hexdigest()}
    record['plan_features']=features
    log.write(json.dumps(record)+'\n');log.flush()
    (out/('q%02d-plan.txt'%q)).write_text(df._jdf.queryExecution().toString())
    if run==a.warmup:(out/('q%02d-result.json'%q)).write_bytes(encoded)
    print(record,flush=True)
finally:spark.stop()
print('BENCHMARK_COMPLETE',out,flush=True)
