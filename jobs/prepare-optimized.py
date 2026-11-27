#!/usr/bin/env python3
"""Create combined year-partitioned/bucketed Parquet tables, then collect CBO stats."""
from pathlib import Path
import argparse,json,datetime,traceback
from pyspark.sql import SparkSession,functions as F
from optimized_views import install_views
p=argparse.ArgumentParser();p.add_argument('--sf',type=int,choices=[1,10,30],required=True);a=p.parse_args()
ROOT=Path('/home/junhee/woosang/TPC-H');HDFS='hdfs://192.168.10.80:9000'
DB='tpch_sf%d_optimized'%a.sf;BASE='tpch_sf%d'%a.sf
BUCKETS={'supplier':'s_suppkey','customer':'c_custkey','part':'p_partkey','partsupp':'ps_partkey','orders':'o_orderkey','lineitem':'l_orderkey'}
DATES={'orders':('o_orderdate','o_order_year'),'lineitem':('l_shipdate','l_ship_year')}
TABLES=['region','nation','supplier','customer','part','partsupp','orders','lineitem']
source=ROOT/'results'/('preparation.json' if a.sf==10 else 'preparation-sf%d.json'%a.sf)
original=json.loads(source.read_text());assert original['status']=='complete'
COUNTS={t:original['tables'][BASE+'.'+t]['rows'] for t in TABLES}
REPORT=ROOT/'results'/('preparation-sf%d-optimized.json'%a.sf)
report={'scale_factor':a.sf,'design':'year_partitioning+32_buckets+broadcast_dimensions','database':DB,'aqe':False,'cbo':True,'started_at':datetime.datetime.now().isoformat(),'tables':{},'validation':{},'status':'running'}
spark=(SparkSession.builder.appName('TPC-H-SF%d-combined-layout-preparation'%a.sf).config('spark.sql.adaptive.enabled','false').enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel('WARN')
def save():
 tmp=REPORT.with_suffix('.tmp');tmp.write_text(json.dumps(report,indent=2,default=str));tmp.replace(REPORT)
def log(s):print(datetime.datetime.now().isoformat(),s,flush=True)
def plan(name,sql):
 text=spark.sql(sql)._jdf.queryExecution().executedPlan().toString()
 (ROOT/'results'/('plan-sf%d-optimized-%s.txt'%(a.sf,name))).write_text(text+'\n');return text
try:
 spark.sql('CREATE DATABASE IF NOT EXISTS '+DB+' LOCATION \''+HDFS+'/tpch/sf%d/optimized-warehouse\''%a.sf)
 for table in TABLES:
  name=DB+'.'+table;location=HDFS+'/tpch/sf%d/optimized/'%a.sf+table
  if not spark.catalog.tableExists(name):
   log('Writing combined table '+name)
   df=spark.table(BASE+'.'+table)
   if table in DATES:
    datecol,yearcol=DATES[table];df=df.withColumn(yearcol,F.year(F.col(datecol)))
   if table in BUCKETS:df=df.repartition(32,F.col(BUCKETS[table]))
   else:df=df.coalesce(1)
   writer=df.write.mode('errorifexists').format('parquet').option('compression','snappy').option('path',location)
   if table in DATES:writer=writer.partitionBy(DATES[table][1])
   if table in BUCKETS:writer=writer.bucketBy(32,BUCKETS[table]).sortBy(BUCKETS[table])
   writer.saveAsTable(name)
  rows=spark.table(name).count();assert rows==COUNTS[table],(name,rows,COUNTS[table])
  info={r['col_name'].strip():r['data_type'] for r in spark.sql('DESCRIBE EXTENDED '+name).collect() if r['col_name']}
  if table in BUCKETS:
   assert info.get('Num Buckets')=='32',(name,info)
   assert BUCKETS[table] in info.get('Bucket Columns',''),(name,info)
  if table in DATES:
   assert DATES[table][1] in spark.table(name).columns
   partitions=spark.sql('SHOW PARTITIONS '+name).count();assert 1<=partitions<=10,(name,partitions)
  report['tables'][name]={'rows':rows,'row_count_verified':True,'bucket_key':BUCKETS.get(table),'buckets':32 if table in BUCKETS else None,'year_partition':DATES[table][1] if table in DATES else None,'location':location}
  save();log('Verified '+name+' rows='+str(rows))
 for table in TABLES:
  name=DB+'.'+table;log('Collecting statistics '+name)
  spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS')
  spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS FOR ALL COLUMNS')
  report['tables'][name]['statistics_collected']=True;save()
 # Assert that broadcasting and year pruning occur together on combined tables.
 install_views(spark,DB,6)
 pruning=plan('partition-pruning','SELECT l_orderkey FROM lineitem WHERE l_shipdate >= DATE \'1994-01-01\' AND l_shipdate < DATE \'1995-01-01\'')
 assert 'PartitionFilters:' in pruning and 'l_ship_year' in pruning,pruning
 combined=plan('broadcast-and-pruning',"SELECT l.l_orderkey,n.n_name FROM lineitem l JOIN "+DB+".supplier s ON l.l_suppkey=s.s_suppkey JOIN nation n ON s.s_nationkey=n.n_nationkey WHERE l.l_shipdate >= DATE '1994-01-01' AND l.l_shipdate < DATE '1995-01-01'")
 assert 'BroadcastHashJoin' in combined and 'l_ship_year' in combined,combined
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
 bucket=plan('partitioned-bucket-join','SELECT /*+ MERGE(o,l) */ o.o_orderkey,l.l_linenumber FROM '+DB+'.orders o JOIN '+DB+'.lineitem l ON o.o_orderkey=l.l_orderkey')
 assert bucket.count('Bucketed: true')>=2,bucket
 assert 'Exchange hashpartitioning' not in bucket,bucket
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold',10485760)
 report['validation']={'year_partition_pruning':True,'broadcast_join_with_pruning':True,'partitioned_bucket_join_without_shuffle':True,'benchmark_queries_executed':False}
 report.update(status='complete',finished_at=datetime.datetime.now().isoformat());save();log('COMBINED_PREPARATION_COMPLETE')
except BaseException as error:
 report.update(status='failed',error=repr(error));save();traceback.print_exc();raise
finally:spark.stop()
