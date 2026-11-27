"""Explicit four-factor ablation policy; same SQL and engine defaults otherwise."""
import json,hashlib,re
from pathlib import Path
from optimized_views import YEAR_FILTERS
CASES={
 'all_on':{'partitioning':True,'bucketing':True,'broadcasting':True,'cbo':True,'layout':'optimized'},
 'no_partition':{'partitioning':False,'bucketing':True,'broadcasting':True,'cbo':True,'layout':'bucketed'},
 'no_bucket':{'partitioning':True,'bucketing':False,'broadcasting':True,'cbo':True,'layout':'ablation_partitioned'},
 'no_broadcast':{'partitioning':True,'bucketing':True,'broadcasting':False,'cbo':True,'layout':'optimized'},
 'no_cbo':{'partitioning':True,'bucketing':True,'broadcasting':True,'cbo':False,'layout':'optimized'},
 'baseline':{'partitioning':False,'bucketing':False,'broadcasting':False,'cbo':False,'layout':'ablation_plain'},
}
ROOT=Path('/home/junhee/woosang/TPC-H')
def database(sf,case):
 suffix={'base':'','partitioned':'_partitioned','bucketed':'_bucketed','optimized':'_optimized','ablation_partitioned':'_ablation_partitioned','ablation_plain':'_ablation_plain'}[CASES[case]['layout']]
 return 'tpch_sf%d'%sf+suffix

def sql_settings(case):
 c=CASES[case];boolean=lambda x:str(x).lower()
 return {'spark.sql.adaptive.enabled':'false','spark.sql.cbo.enabled':boolean(c['cbo']),'spark.sql.cbo.joinReorder.enabled':boolean(c['cbo']),'spark.sql.cbo.planStats.enabled':boolean(c['cbo']),'spark.sql.sources.bucketing.enabled':boolean(c['bucketing']),'spark.sql.autoBroadcastJoinThreshold':'10485760' if c['broadcasting'] else '-1','spark.sql.adaptive.autoBroadcastJoinThreshold':'10485760' if c['broadcasting'] else '-1','spark.sql.optimizer.dynamicPartitionPruning.enabled':boolean(c['partitioning'])}

def apply_settings(spark,sf,case):
 for k,v in sql_settings(case).items():spark.conf.set(k,v)
 spark.catalog.setCurrentDatabase(database(sf,case))
 effective={k:spark.conf.get(k) for k in sql_settings(case)}
 assert effective==sql_settings(case),(case,effective)
 return effective

def query_sql(sf,q):
 text=(ROOT/'sql'/('q%02d.sql'%q)).read_text()
 if q==11:
  assert text.count('0.0001000000')==1
  text=text.replace('0.0001000000','(0.0001000000 / %d)'%sf)
 if q==16:
  # TPC-H keys are NOT NULL. Verify the data guard in preflight before using
  # this equivalent anti-join form for EVERY case, including all_on.
  # Spark's specialized single-column NOT IN path unconditionally broadcasts.
  pattern=r"ps_suppkey\s+not\s+in\s*\(\s*select\s+s_suppkey\s+from\s+supplier\s+where\s+s_comment\s+like\s+'%Customer%Complaints%'\s*\)"
  text,count=re.subn(pattern,"not exists (select 1 from supplier where s_suppkey = ps_suppkey and s_comment like '%Customer%Complaints%')",text,flags=re.IGNORECASE)
  assert count==1,'Unexpected Q16 source SQL'
 return text

def install_views(spark,sf,case,q):
 c=CASES[case];db=database(sf,case);definitions={}
 for table in ['orders','lineitem','nation','region']:
  condition=YEAR_FILTERS.get(q,{}).get(table) if c['partitioning'] else None
  df=spark.table(db+'.'+table)
  if condition:df=df.where(condition)
  hint=c['broadcasting'] and table in ['nation','region']
  if hint:df=df.hint('broadcast')
  df.createOrReplaceTempView(table)
  definitions[table]={'source':db+'.'+table,'year_predicate':condition,'broadcast_hint':hint}
 return definitions

def features(plan):
 filters=re.findall(r'PartitionFilters:\s*(\[[^\n]*?\])',plan)
 return {'broadcast_hash_joins':plan.count('BroadcastHashJoin'),'broadcast_nested_loop_joins':plan.count('BroadcastNestedLoopJoin'),'broadcast_exchanges':plan.count('BroadcastExchange'),'bucketed_scans':plan.count('Bucketed: true'),'year_partition_filter_present':any('l_ship_year' in f or 'o_order_year' in f for f in filters),'partition_filters':filters}

def check_plan(case,q,plan):
 c=CASES[case];f=features(plan)
 if not c['broadcasting']:assert f['broadcast_hash_joins']==0 and f['broadcast_nested_loop_joins']==0 and f['broadcast_exchanges']==0,(case,q,'broadcast remains',plan)
 if not c['bucketing']:assert f['bucketed_scans']==0,(case,q,'bucketing remains',plan)
 if not c['partitioning']:assert not f['year_partition_filter_present'],(case,q,'year partition filter remains',plan)
 return f

def multiset_hash(rows):
 serialized=sorted(json.dumps(row,sort_keys=True,default=str,separators=(',',':')) for row in rows)
 h=hashlib.sha256()
 for row in serialized:h.update(row.encode());h.update(b'\n')
 return h.hexdigest()
