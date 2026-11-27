from pathlib import Path
import json,datetime,fcntl
from pyspark.sql import SparkSession,functions as F,types as T
import argparse
p=argparse.ArgumentParser();p.add_argument('--case',choices=['no_partition','no_bucket','baseline'],required=True);a=p.parse_args()
ROOT=Path('/data/woosang/TPC-H');RAW=ROOT/'data/raw/sf100'
SUFFIX={'no_partition':'bucketed','no_bucket':'ablation_partitioned','baseline':'ablation_plain'}[a.case]

lock=(ROOT/'run/sf100-preparation.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
manifest=json.loads((RAW/'MANIFEST.json').read_text())
source=(ROOT/'jobs/schema-source.py').read_text();start=source.index('D=T.DecimalType');end=source.index('\nFILES=',start);exec(source[start:end])
spark=SparkSession.builder.master('local[8]').appName('TPC-H-SF100-direct-Parquet-preparation').config('spark.sql.adaptive.enabled','false').config('spark.sql.warehouse.dir',str(ROOT/'metadata/sf100-warehouse')).config('spark.local.dir',str(ROOT/'tmp/sf100')).enableHiveSupport().getOrCreate();spark.sparkContext.setLogLevel('WARN')
DB='tpch_sf100_'+SUFFIX;spark.sql('CREATE DATABASE IF NOT EXISTS '+DB)
keys={'supplier':'s_suppkey','customer':'c_custkey','part':'p_partkey','partsupp':'ps_partkey','orders':'o_orderkey','lineitem':'l_orderkey'};dates={'orders':('o_orderdate','o_order_year'),'lineitem':('l_shipdate','l_ship_year')}
if a.case!='no_partition':keys={}
if a.case!='no_bucket':dates={}
report={'case':a.case,'status':'running','scale_factor':100,'raw_location':str(RAW),'raw_uploaded_to_hdfs':False,'tables':{},'started_at':datetime.datetime.now().isoformat()};target=ROOT/('results/preparation-sf100-'+a.case+'-layout.json');ddls=[]
def save():target.write_text(json.dumps(report,indent=2))
try:
 for t,fields in SCHEMAS.items():
  name=DB+'.'+t
  if not spark.catalog.tableExists(name):
   schema=T.StructType([T.StructField(n,ty,True) for n,ty in fields]+[T.StructField('_trailing',S,True)])
   df=spark.read.schema(schema).option('sep','|').option('quote','').option('mode','FAILFAST').csv('file://'+str(RAW/(t+'.tbl'))).drop('_trailing')
   if t in dates:df=df.withColumn(dates[t][1],F.year(F.col(dates[t][0])))
   df=df.coalesce(1) if t in ['region','nation'] else df.repartition(32,F.col(keys[t])) if t in keys else df.repartition(32)
   w=df.write.mode('errorifexists').format('parquet').option('compression','snappy').option('path','hdfs://192.168.10.80:9000/tpch/sf100/ablation-scratch/'+a.case+'/'+t)
   if t in dates:w=w.partitionBy(dates[t][1])
   if t in keys:w=w.bucketBy(32,keys[t]).sortBy(keys[t])
   w.saveAsTable(name)
  df=spark.table(name);count=df.count();assert count==manifest['tables'][t]['rows'],(t,count)
  nulls=df.select([F.sum(F.col(c).isNull().cast('long')).alias(c) for c in df.columns]).first().asDict();assert all(v==0 for v in nulls.values()),(t,nulls)
  report['tables'][name]={'rows':count,'row_count_verified':True,'nulls_verified':True};save()
  ddls.append(spark.sql('SHOW CREATE TABLE '+name).first()[0]);(ROOT/('results/sf100-'+a.case+'-ddls.json')).write_text(json.dumps(ddls))
  print('VERIFIED',name,count,flush=True)
 report.update(status='complete',finished_at=datetime.datetime.now().isoformat());save()
except BaseException as e:report.update(status='failed',error=repr(e));save();raise
finally:spark.stop()
