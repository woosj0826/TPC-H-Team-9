import sys,json,re,collections
stage_group={};acc={};hosts={};reasons=collections.Counter()
def entry():return {'tasks':0,'run_seconds':0.,'cpu_seconds':0.,'gc_seconds':0.,'fetch_wait_seconds':0.,'remote_shuffle_gib':0.,'local_shuffle_gib':0.,'input_gib':0.,'spill_gib':0.,'locality':{}}
def add(a,m,info):
 a['tasks']+=1;a['run_seconds']+=m.get('Executor Run Time',0)/1000;a['cpu_seconds']+=m.get('Executor CPU Time',0)/1e9;a['gc_seconds']+=m.get('JVM GC Time',0)/1000
 sr=m.get('Shuffle Read Metrics',{});a['fetch_wait_seconds']+=sr.get('Fetch Wait Time',0)/1000;a['remote_shuffle_gib']+=sr.get('Remote Bytes Read',0)/2**30;a['local_shuffle_gib']+=sr.get('Local Bytes Read',0)/2**30;a['input_gib']+=m.get('Input Metrics',{}).get('Bytes Read',0)/2**30;a['spill_gib']+=m.get('Disk Bytes Spilled',0)/2**30
 k=info.get('Locality','unknown');a['locality'][k]=a['locality'].get(k,0)+1
for line in sys.stdin:
 try:e=json.loads(line)
 except ValueError:continue
 if e.get('Event')=='SparkListenerJobStart':
  group=(e.get('Properties') or {}).get('spark.jobGroup.id','');match=re.match(r'scaling-n(\d+)-r(\d+)-q(\d+)',group)
  for stage in e.get('Stage IDs',[]):stage_group[stage]=tuple(map(int,match.groups())) if match else None
 if e.get('Event')!='SparkListenerTaskEnd':continue
 group=stage_group.get(e['Stage ID'])
 if not group or group[1]==0:continue
 m=e.get('Task Metrics') or {};info=e.get('Task Info') or {};reason=(e.get('Task End Reason') or {}).get('Reason','unknown');reasons[reason]+=1
 a=acc.setdefault(str(group[2]),entry());add(a,m,info);h=hosts.setdefault(info.get('Host','unknown'),entry());add(h,m,info)
print(json.dumps({'queries':acc,'hosts':hosts,'task_end_reasons':dict(reasons)},indent=2))
