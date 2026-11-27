#!/usr/bin/env python3
"""SF10 matched unbucketed layouts: same writer parallelism, no added date sorting."""
import json,datetime,traceback
from pathlib import Path
from pyspark.sql import SparkSession,functions as F
ROOT=Path('/home/junhee/woosang/TPC-H');SF=10;HDFS='hdfs://192.168.10.80:9000'
TABLES=['region','nation','supplier','customer','part','partsupp','orders','lineitem'];DATES={'orders':('o_orderdate','o_order_year'),'lineitem':('l_shipdate','l_ship_year')}
original=json.loads((ROOT/'results/preparation.json').read_text());assert original['status']=='complete'
COUNTS={t:original['tables']['tpch_sf10.'+t]['rows'] for t in TABLES}
REPORT=ROOT/'results/preparation-sf10-ablation-layouts.json';report={'sf':10,'status':'running','started_at':datetime.datetime.now().isoformat(),'writer_partitions':32,'sort_by_date':False,'sort_by_join_key':False,'tables':{}}
spark=SparkSession.builder.appName('TPC-H-SF10-matched-ablation-layouts').config('spark.sql.adaptive.enabled','false').enableHiveSupport().getOrCreate();spark.sparkContext.setLogLevel('WARN')
def save():(REPORT).write_text(json.dumps(report,indent=2))
try:
 for partitioned in [True,False]:
  suffix='ablation_partitioned' if partitioned else 'ablation_plain';db='tpch_sf10_'+suffix
  spark.sql('CREATE DATABASE IF NOT EXISTS '+db)
  for table in TABLES:
   name=db+'.'+table
   if not spark.catalog.tableExists(name):
    print('WRITING',name,flush=True);df=spark.table('tpch_sf10.'+table)
    if partitioned and table in DATES:
     datecol,yearcol=DATES[table];df=df.withColumn(yearcol,F.year(F.col(datecol)))
    df=df.coalesce(1) if table in ['nation','region'] else df.repartition(32)
    writer=df.write.mode('errorifexists').format('parquet').option('compression','snappy').option('path',HDFS+'/tpch/sf10/'+suffix+'/'+table)
    if partitioned and table in DATES:writer=writer.partitionBy(DATES[table][1])
    writer.saveAsTable(name)
   rows=spark.table(name).count();assert rows==COUNTS[table],(name,rows)
   info={r['col_name'].strip():r['data_type'] for r in spark.sql('DESCRIBE EXTENDED '+name).collect() if r['col_name']}
   assert 'Num Buckets' not in info or info['Num Buckets'] in ['0','-1'],(name,info.get('Num Buckets'))
   if partitioned and table in DATES:assert 1<=spark.sql('SHOW PARTITIONS '+name).count()<=10
   spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS');spark.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS FOR ALL COLUMNS')
   report['tables'][name]={'rows':rows,'row_count_verified':True,'statistics_collected':True,'bucketed':False,'year_partitioned':partitioned and table in DATES};save();print('VERIFIED',name,rows,flush=True)
 report.update(status='complete',finished_at=datetime.datetime.now().isoformat());save();print('MATCHED_ABLATION_LAYOUTS_COMPLETE',flush=True)
except BaseException as error:report.update(status='failed',error=repr(error));save();traceback.print_exc();raise
finally:spark.stop()
