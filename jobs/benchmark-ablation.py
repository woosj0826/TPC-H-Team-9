#!/usr/bin/env python3
"""One case/SF per fresh Spark session; preserve semantics and verify the disabled factor."""
import argparse,datetime,json,time,hashlib,traceback
from pathlib import Path
from pyspark.sql import SparkSession
from ablation_config import CASES,ROOT,database,sql_settings,apply_settings,install_views,query_sql,check_plan,multiset_hash
p=argparse.ArgumentParser();p.add_argument('--sf',type=int,choices=[10],required=True);p.add_argument('--case',choices=[k for k in CASES if k!='all_on'],required=True);p.add_argument('--output',required=True);p.add_argument('--reference');p.add_argument('--warmup',type=int,default=1);p.add_argument('--repeat',type=int,default=3);a=p.parse_args()
assert a.warmup==1 and a.repeat==3
policy=json.loads((ROOT/'results/ablation-preflight/report.json').read_text())
assert policy['status']=='complete' and policy['q16_null_guards'][str(a.sf)] and policy['q16_equivalence'][str(a.sf)]['matches_previous_original_query']
assert policy['conditions']['sf%d-%s'%(a.sf,a.case)]['validated']
matched=json.loads((ROOT/'results/preparation-sf10-ablation-layouts.json').read_text());assert matched['status']=='complete' and len(matched['tables'])==16
out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
expected={}
if a.case!='all_on':
 assert a.reference,'Existing all_on reference is required'
 reference=Path(a.reference)
 cfg=json.loads((reference/'configuration.json').read_text());assert cfg['arguments']['sf']==a.sf and (cfg['arguments'].get('case')=='all_on' or cfg['arguments'].get('layout')=='optimized')
 for q in range(1,23):
  data=json.loads((reference/('q%02d-result.json'%q)).read_text());expected[q]=(len(data),multiset_hash(data))
report_path=ROOT/'results'/('preparation-sf%d-optimized.json'%a.sf)
assert json.loads(report_path.read_text())['status']=='complete'
builder=SparkSession.builder.appName('TPC-H-ablation-SF%d-%s'%(a.sf,a.case))
for key,value in sql_settings(a.case).items():builder=builder.config(key,value)
spark=builder.enableHiveSupport().getOrCreate();spark.sparkContext.setLogLevel('WARN')
effective=apply_settings(spark,a.sf,a.case)
configuration={'arguments':vars(a),'case_factors':CASES[a.case],'database':database(a.sf,a.case),'effective_sql_settings':effective,'spark_conf':dict(spark.sparkContext.getConf().getAll()),'resource_limits':{'node_vcores':8,'node_yarn_memory_gib':64,'executor_count':3,'executor_cores':7,'executor_heap_gib':48,'executor_overhead_gib':6},'timing_scope':'spark.sql through collect; excludes view setup, result serialization, plan extraction and Spark startup','cache_policy':'one full warmup, no OS/HDFS cache flush','result_validation':'order-insensitive row multiset; preserve duplicate rows; compare all runs against same-session all_on','partition_policy':'add implied year predicates only for the original queries having applicable date predicates','query16_policy':'Equivalent NOT EXISTS applied to all cases after validating non-null keys; avoids mandatory null-aware broadcast anti-join', 'layout_notes':'no_partition uses unpartitioned bucketed tables; no_bucket uses matched 32-writer year-partitioned unbucketed tables without extra date sorting; baseline uses matched 32-writer plain Parquet tables'}
(out/'configuration.json').write_text(json.dumps(configuration,indent=2))
status={'status':'running','case':a.case,'sf':a.sf,'completed_executions':0,'started_at':datetime.datetime.now().isoformat()}
def save_status():(out/'status.json').write_text(json.dumps(status,indent=2))
save_status();print('RESULT_DIRECTORY',out,flush=True)
try:
 # Verify policy before any timed execution. This compiles plans, without collecting query results.
 preflight=[]
 for q in range(1,23):
  definitions=install_views(spark,a.sf,a.case,q);sql=query_sql(a.sf,q)
  plan=spark.sql(sql)._jdf.queryExecution().executedPlan().toString();features=check_plan(a.case,q,plan)
  preflight.append({'query':q,'features':features});(out/('q%02d-effective.sql'%q)).write_text(sql);(out/('q%02d-views.json'%q)).write_text(json.dumps(definitions,indent=2))
 (out/'preflight.json').write_text(json.dumps(preflight,indent=2))
 with (out/'timings.jsonl').open('w') as log:
  for run in range(a.warmup+a.repeat):
   for q in range(1,23):
    install_views(spark,a.sf,a.case,q);sql=query_sql(a.sf,q)
    spark.sparkContext.setJobGroup('ablation-sf%d-%s-r%d-q%02d'%(a.sf,a.case,run,q),'TPC-H ablation',interruptOnCancel=True)
    start_epoch=time.time();start=time.perf_counter();df=spark.sql(sql);rows=df.collect();seconds=time.perf_counter()-start;end_epoch=time.time()
    data=[row.asDict(recursive=True) for row in rows];encoded=json.dumps(data,sort_keys=True,default=str).encode();canonical=multiset_hash(data)
    if a.case=='all_on' and run==0:expected[q]=(len(data),canonical)
    valid=expected[q]==(len(data),canonical)
    plan=df._jdf.queryExecution().executedPlan().toString();features=check_plan(a.case,q,plan)
    record={'sf':a.sf,'case':a.case,'query':q,'run':run,'warmup':run<a.warmup,'seconds':seconds,'start_epoch':start_epoch,'end_epoch':end_epoch,'rows':len(rows),'result_sha256':hashlib.sha256(encoded).hexdigest(),'result_multiset_sha256':canonical,'result_validation_passed':valid,'plan_features':features}
    log.write(json.dumps(record)+'\n');log.flush();print(record,flush=True)
    (out/('q%02d-plan.txt'%q)).write_text(df._jdf.queryExecution().toString())
    if run==a.warmup:(out/('q%02d-result.json'%q)).write_bytes(encoded)
    if not valid:
     (out/('q%02d-r%d-mismatch.json'%(q,run))).write_bytes(encoded)
     raise AssertionError('Query result mismatch against all_on: SF=%d case=%s q=%d run=%d'%(a.sf,a.case,q,run))
    status['completed_executions']+=1;status.update(query=q,run=run);save_status()
 status.update(status='complete',finished_at=datetime.datetime.now().isoformat());save_status();print('ABLATION_CELL_COMPLETE',a.sf,a.case,out,flush=True)
except BaseException as error:
 status.update(status='failed',error=repr(error));save_status();traceback.print_exc();raise
finally:spark.stop()
