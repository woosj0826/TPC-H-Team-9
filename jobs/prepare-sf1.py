#!/usr/bin/env python3
"""Prepare SF1 Parquet layouts and CBO statistics. Does not run TPC-H queries."""
import json,datetime,time,traceback,urllib.request
from pathlib import Path
from pyspark.sql import SparkSession,functions as F,types as T

ROOT=Path('/home/junhee/woosang/TPC-H')
SF=1
HDFS='hdfs://192.168.10.80:9000'
RAW_MANIFEST=json.load(urllib.request.urlopen('http://192.168.10.80:9870/webhdfs/v1/tpch/raw/sf1/MANIFEST.json?op=OPEN&user.name=junhee',timeout=60))
assert RAW_MANIFEST['scale_factor']==1
COUNTS={table:metadata['rows'] for table,metadata in RAW_MANIFEST['tables'].items()}
# Decimal arithmetic avoids introducing binary floating-point error into prices.
D=T.DecimalType(15,2); I=T.IntegerType(); L=T.LongType(); S=T.StringType(); DATE=T.DateType()
SCHEMAS={
'region':[('r_regionkey',I),('r_name',S),('r_comment',S)],
'nation':[('n_nationkey',I),('n_name',S),('n_regionkey',I),('n_comment',S)],
'supplier':[('s_suppkey',I),('s_name',S),('s_address',S),('s_nationkey',I),('s_phone',S),('s_acctbal',D),('s_comment',S)],
'customer':[('c_custkey',I),('c_name',S),('c_address',S),('c_nationkey',I),('c_phone',S),('c_acctbal',D),('c_mktsegment',S),('c_comment',S)],
'part':[('p_partkey',I),('p_name',S),('p_mfgr',S),('p_brand',S),('p_type',S),('p_size',I),('p_container',S),('p_retailprice',D),('p_comment',S)],
'partsupp':[('ps_partkey',I),('ps_suppkey',I),('ps_availqty',I),('ps_supplycost',D),('ps_comment',S)],
'orders':[('o_orderkey',L),('o_custkey',I),('o_orderstatus',S),('o_totalprice',D),('o_orderdate',DATE),('o_orderpriority',S),('o_clerk',S),('o_shippriority',I),('o_comment',S)],
'lineitem':[('l_orderkey',L),('l_partkey',I),('l_suppkey',I),('l_linenumber',I),('l_quantity',D),('l_extendedprice',D),('l_discount',D),('l_tax',D),('l_returnflag',S),('l_linestatus',S),('l_shipdate',DATE),('l_commitdate',DATE),('l_receiptdate',DATE),('l_shipinstruct',S),('l_shipmode',S),('l_comment',S)]}
FILES={'region':1,'nation':1,'supplier':4,'customer':16,'part':16,'partsupp':32,'orders':48,'lineitem':96}
BUCKETS={'supplier':'s_suppkey','customer':'c_custkey','part':'p_partkey','partsupp':'ps_partkey','orders':'o_orderkey','lineitem':'l_orderkey'}
PARTITIONS={'orders':('o_orderdate','o_order_year'),'lineitem':('l_shipdate','l_ship_year')}
spark=(SparkSession.builder.appName('TPC-H-SF1-data-preparation').enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel('WARN')
report={'scale_factor':1,'started_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'tables':{},'validation':{}}
REPORT=ROOT/'results/preparation-sf1.json'
def checkpoint():
 REPORT.parent.mkdir(parents=True,exist_ok=True)
 tmp=REPORT.with_suffix('.json.tmp');tmp.write_text(json.dumps(report,indent=2,default=str));tmp.replace(REPORT)
def log(message):print(datetime.datetime.now().isoformat(),message,flush=True)
def existing(db,t):return spark.catalog.tableExists(db+'.'+t)
def verify(db,t):
 name=db+'.'+t
 count=spark.table(name).count()
 assert count==COUNTS[t],(name,count,COUNTS[t])
 report['tables'][name]={'rows':count,'row_count_verified':True}
 checkpoint();log('Verified '+name+' rows='+str(count))
def stats(db,t):
 name=db+'.'+t
 log('Collecting table and column statistics: '+name)
 spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS')
 spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS FOR ALL COLUMNS')
 details=[r.asDict() for r in spark.sql('DESCRIBE EXTENDED '+name).collect()]
 report['tables'][name]['statistics_collected']=True
 report['tables'][name]['describe_extended']=details
 checkpoint()
try:
 for db in ['tpch_sf1','tpch_sf1_partitioned','tpch_sf1_bucketed']:
  spark.sql('CREATE DATABASE IF NOT EXISTS '+db+' LOCATION \''+HDFS+'/tpch/sf1/'+db+'\'')
 # Base tables: all eight tables in Snappy-compressed Parquet.
 for t,fields in SCHEMAS.items():
  if not existing('tpch_sf1',t):
   log('Writing base table '+t)
   schema=T.StructType([T.StructField(name,typ,True) for name,typ in fields]+[T.StructField('_trailing',S,True)])
   raw=(spark.read.schema(schema).option('sep','|').option('quote','').option('mode','FAILFAST').csv(HDFS+'/tpch/raw/sf1/'+t+'.tbl').drop('_trailing'))
   # Check parsing: raw TPC-H data contains no null fields.
   nulls=raw.select([F.sum(F.col(c).isNull().cast('long')).alias(c) for c in raw.columns]).first().asDict()
   assert all(v==0 for v in nulls.values()),(t,'unexpected nulls',nulls)
   raw.repartition(FILES[t]).write.mode('errorifexists').format('parquet').option('compression','snappy').option('path',HDFS+'/tpch/sf1/base/'+t).saveAsTable('tpch_sf1.'+t)
  verify('tpch_sf1',t)
 # Partition layout: fact tables by year; dimensions reuse the same base Parquet files.
 for t in SCHEMAS:
  if not existing('tpch_sf1_partitioned',t):
   log('Writing partitioned layout '+t)
   if t in PARTITIONS:
    datecol,yearcol=PARTITIONS[t]
    (spark.table('tpch_sf1.'+t).withColumn(yearcol,F.year(F.col(datecol)))
      .repartition(32).sortWithinPartitions(yearcol,datecol)
      .write.mode('errorifexists').format('parquet').option('compression','snappy')
      .option('path',HDFS+'/tpch/sf1/partitioned/'+t).partitionBy(yearcol).saveAsTable('tpch_sf1_partitioned.'+t))
   else:
    spark.sql('CREATE TABLE tpch_sf1_partitioned.'+t+' USING PARQUET LOCATION \''+HDFS+'/tpch/sf1/base/'+t+'\'')
  verify('tpch_sf1_partitioned',t)
 # Bucket layout: matching order key types and 32 buckets on both orders/lineitem.
 for t in SCHEMAS:
  if not existing('tpch_sf1_bucketed',t):
   log('Writing bucketed layout '+t)
   if t in BUCKETS:
    key=BUCKETS[t]
    (spark.table('tpch_sf1.'+t).repartition(32,F.col(key)).write.mode('errorifexists')
      .format('parquet').option('compression','snappy').option('path',HDFS+'/tpch/sf1/bucketed/'+t)
      .bucketBy(32,key).sortBy(key).saveAsTable('tpch_sf1_bucketed.'+t))
   else:
    spark.sql('CREATE TABLE tpch_sf1_bucketed.'+t+' USING PARQUET LOCATION \''+HDFS+'/tpch/sf1/base/'+t+'\'')
  verify('tpch_sf1_bucketed',t)
 for db in ['tpch_sf1','tpch_sf1_partitioned','tpch_sf1_bucketed']:
  for t in SCHEMAS:stats(db,t)
 # Inspect plans for preparation validation; do not execute TPC-H benchmark queries.
 def save_plan(name,sql):
  query=spark.sql(sql)
  text=query._jdf.queryExecution().executedPlan().toString()
  path=ROOT/'results'/('plan-sf1-'+name+'.txt');path.write_text(text+'\n')
  return text
 broadcast=save_plan('broadcast','SELECT /*+ BROADCAST(n) */ s.s_suppkey, n.n_name FROM tpch_sf1.supplier s JOIN tpch_sf1.nation n ON s.s_nationkey=n.n_nationkey')
 assert 'BroadcastHashJoin' in broadcast,broadcast
 pruning=save_plan('partition-pruning',"SELECT l_orderkey FROM tpch_sf1_partitioned.lineitem WHERE l_ship_year=1995 AND l_shipdate>=DATE '1995-01-01' AND l_shipdate<DATE '1996-01-01'")
 assert 'PartitionFilters:' in pruning and 'l_ship_year' in pruning,pruning
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
 spark.conf.set('spark.sql.adaptive.enabled','false')
 bucket=save_plan('bucket-join','SELECT /*+ MERGE(o,l) */ o.o_orderkey,l.l_linenumber FROM tpch_sf1_bucketed.orders o JOIN tpch_sf1_bucketed.lineitem l ON o.o_orderkey=l.l_orderkey')
 assert 'Bucketed: true' in bucket,bucket
 assert 'Exchange hashpartitioning' not in bucket,bucket
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold',10485760)
 spark.conf.set('spark.sql.adaptive.enabled','false')
 report['validation']={'broadcast_hash_join_plan':True,'partition_pruning_plan':True,'bucket_join_without_shuffle_plan':True,'benchmark_queries_executed':False}
 report['finished_at_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 report['status']='complete';checkpoint();log('PREPARATION_COMPLETE (no benchmark queries executed)')
except BaseException as e:
 report['status']='failed';report['error']=repr(e);checkpoint();traceback.print_exc();raise
finally:spark.stop()
