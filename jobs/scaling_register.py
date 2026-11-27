from pathlib import Path
import json,datetime
from pyspark.sql import SparkSession
ROOT=Path('/mnt/data/woosang/TPC-H/node-scaling');URI='hdfs://tpch-scale-43:39100'
s=SparkSession.builder.appName('TPC-H-SF100-scaling-register-and-statistics').enableHiveSupport().getOrCreate();s.sparkContext.setLogLevel('WARN')
report={'status':'running','tables':{},'started_at':datetime.datetime.now().isoformat()};out=ROOT/'results/preparation.json'
try:
 s.sql('CREATE DATABASE IF NOT EXISTS tpch_sf100_optimized')
 for ddl in json.loads((ROOT/'results/source-ddls.json').read_text()):
  ddl=ddl.replace('hdfs://192.168.10.80:9000',URI).replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ',1)
  s.sql(ddl)
 for t in ['orders','lineitem']:s.sql('MSCK REPAIR TABLE tpch_sf100_optimized.'+t)
 counts=json.loads((ROOT/'results/source-counts.json').read_text())
 for t,n in counts.items():
  name='tpch_sf100_optimized.'+t;df=s.table(name);actual=df.count();assert actual==n,(t,actual,n)
  info={r['col_name'].strip():r['data_type'] for r in s.sql('DESCRIBE EXTENDED '+name).collect() if r['col_name']}
  if t not in ['nation','region']:assert info['Num Buckets']=='32',(t,info)
  s.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS');s.sql('ANALYZE TABLE '+name+' COMPUTE STATISTICS FOR ALL COLUMNS')
  report['tables'][t]={'rows':actual,'statistics_collected':True,'bucket_metadata':info.get('Bucket Columns'),'location':info['Location']};out.write_text(json.dumps(report,indent=2));print('VERIFIED',t,actual,flush=True)
 report.update(status='complete',finished_at=datetime.datetime.now().isoformat());out.write_text(json.dumps(report,indent=2))
except BaseException as e:report.update(status='failed',error=repr(e));out.write_text(json.dumps(report,indent=2));raise
finally:s.stop()
