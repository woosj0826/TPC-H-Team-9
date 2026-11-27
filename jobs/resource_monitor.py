#!/usr/bin/env python3
"""One-second host and TPC-H-process samples. Never emits process command lines."""
from pathlib import Path
import argparse,os,json,time,re,signal,resource
p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--stop-file',required=True);p.add_argument('--cpus',required=True);p.add_argument('--interval',type=float,default=1.0);p.add_argument('--max-seconds',type=float,default=43200);a=p.parse_args()
MEMORY_LIMIT=128*1024*1024
resource.setrlimit(resource.RLIMIT_AS,(MEMORY_LIMIT,MEMORY_LIMIT))
OUT=Path(a.output);OUT.parent.mkdir(parents=True,exist_ok=True);STOP=Path(a.stop_file)
HZ=os.sysconf('SC_CLK_TCK');PAGE=os.sysconf('SC_PAGE_SIZE');SELF=os.getpid();RUNNING=True
CPUS=[]
for chunk in a.cpus.split(','):
 if '-' in chunk:
  lo,hi=map(int,chunk.split('-'));CPUS.extend(range(lo,hi+1))
 else:CPUS.append(int(chunk))
def end(*_):
 global RUNNING;RUNNING=False
signal.signal(signal.SIGTERM,end);signal.signal(signal.SIGINT,end)
def get_cpu():
 return {line.split()[0]:list(map(int,line.split()[1:9])) for line in Path('/proc/stat').read_text().splitlines() if line.startswith('cpu')}
CACHED_PIDS={}
LAST_DISCOVERY=0.0
def processes():
 global CACHED_PIDS,LAST_DISCOVERY
 now=time.monotonic();discover=now-LAST_DISCOVERY>=5
 out={}
 paths=list(Path('/proc').iterdir()) if discover else [Path('/proc')/str(pid) for pid in CACHED_PIDS]
 for path in paths:
  if not path.name.isdigit():continue
  try:
   raw=(path/'stat').read_text();fields=raw[raw.rindex(')')+2:].split();pid=int(path.name)
   if discover:
    cmd=(path/'cmdline').read_bytes();experiment=pid!=SELF and b'/TPC-H/' in cmd
   else:
    experiment=fields[19]==CACHED_PIDS[pid]
   out[pid]={'parent':int(fields[1]),'ticks':int(fields[11])+int(fields[12]),'start':fields[19],'rss':int(fields[21])*PAGE,'io_read':0,'io_write':0,'experiment':experiment}
  except (OSError,ValueError,IndexError):continue
 if discover:
  changed=True
  while changed:
   changed=False
   for pid,proc in out.items():
    if pid!=SELF and not proc['experiment'] and out.get(proc['parent'],{}).get('experiment'):
     proc['experiment']=True;changed=True
  CACHED_PIDS={pid:info['start'] for pid,info in out.items() if info['experiment']}
  LAST_DISCOVERY=now
 selected={pid:info for pid,info in out.items() if info['experiment']}
 for pid,info in selected.items():
  try:
   io={k:int(v) for k,v in (line.split(':') for line in (Path('/proc')/str(pid)/'io').read_text().splitlines())}
   info['io_read']=io.get('read_bytes',0);info['io_write']=io.get('write_bytes',0)
  except (OSError,ValueError):pass
 return selected

def disks():
 out={}
 for line in Path('/proc/diskstats').read_text().splitlines():
  f=line.split()
  if re.fullmatch(r'sd[a-z]+|nvme\d+n\d+',f[2]):out[f[2]]=list(map(int,f[3:14]))
 return out
def network():
 out={}
 for path in Path('/sys/class/net').iterdir():
  if not path.name.startswith(('eno','enp')):continue
  try:
   if (path/'operstate').read_text().strip()!='up':continue
   out[path.name]=[int((path/'statistics/rx_bytes').read_text()),int((path/'statistics/tx_bytes').read_text())]
  except OSError:pass
 return out
def vm():return dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
def snap():
 start=time.monotonic();result={'epoch':time.time(),'monotonic':start,'cpu':get_cpu(),'processes':processes(),'disks':disks(),'network':network(),'vm':vm()};result['collection_seconds']=time.monotonic()-start;return result
def cpu_percent(old,new,names):
 delta=[0]*8
 for name in names:
  if name in old and name in new:
   delta=[x+b-a for x,a,b in zip(delta,old[name],new[name])]
 total=sum(delta)
 return {'busy_percent':100*(total-delta[3]-delta[4])/total if total else 0,'iowait_percent':100*delta[4]/total if total else 0}
previous=snap();started=time.monotonic()
with OUT.open('a',buffering=1) as stream:
 while RUNNING and not STOP.exists() and time.monotonic()-started<a.max_seconds:
  time.sleep(a.interval);current=snap();dt=current['monotonic']-previous['monotonic']
  row={'host':os.uname().nodename,'start_epoch':previous['epoch'],'end_epoch':current['epoch'],'sample_seconds':dt,'logical_cpus':os.cpu_count(),'experiment_cpus':a.cpus,'host_cpu':cpu_percent(previous['cpu'],current['cpu'],['cpu']),'restricted_cpu_pool':cpu_percent(previous['cpu'],current['cpu'],['cpu%d'%i for i in CPUS])}
  ticks=read=write=0
  for pid,info in current['processes'].items():
   old=previous['processes'].get(pid)
   if old and old['start']==info['start']:
    ticks+=max(0,info['ticks']-old['ticks']);read+=max(0,info['io_read']-old['io_read']);write+=max(0,info['io_write']-old['io_write'])
  row['experiment_processes']={'cpu_equivalent_cores':ticks/HZ/dt,'rss_gib_sum':sum(v['rss'] for v in current['processes'].values())/2**30,'count':len(current['processes']),'read_mib_s':read/2**20/dt,'write_mib_s':write/2**20/dt}
  mem={line.split(':')[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.split(':')[0] in ['MemTotal','MemAvailable','SwapTotal','SwapFree']}
  row['memory_gib']={k:v/1024**2 for k,v in mem.items()}
  row['swap_mib_s']={k:(int(current['vm'][k])-int(previous['vm'][k]))*PAGE/2**20/dt for k in ['pswpin','pswpout']}
  row['network_mib_s']={name:{'rx':max(0,c[0]-previous['network'][name][0])/2**20/dt,'tx':max(0,c[1]-previous['network'][name][1])/2**20/dt} for name,c in current['network'].items() if name in previous['network']}
  row['disks']={}
  for name,c in current['disks'].items():
   if name not in previous['disks']:continue
   delta=[b-x for x,b in zip(previous['disks'][name],c)];ops=delta[0]+delta[4]
   row['disks'][name]={'read_mib_s':delta[2]*512/2**20/dt,'write_mib_s':delta[6]*512/2**20/dt,'await_ms':(delta[3]+delta[7])/ops if ops else 0,'busy_percent':delta[9]/dt/10}
  row['pressure']={}
  for kind in ['cpu','memory','io']:
   try:row['pressure'][kind]=Path('/proc/pressure/'+kind).read_text().strip()
   except OSError:pass
  row['monitor_cpu_seconds_total']=sum(os.times()[:2])
  row['monitor_rss_mib']=int(Path('/proc/self/stat').read_text().split(')')[-1].split()[21])*PAGE/2**20
  row['monitor_collection_seconds']=current['collection_seconds']
  row['process_discovery_interval_seconds']=5
  row['monitor_allowed_cpus']=sorted(os.sched_getaffinity(SELF))
  row['monitor_virtual_memory_limit_mib']=MEMORY_LIMIT/2**20
  stream.write(json.dumps(row,separators=(',',':'))+'\n');previous=current
OUT.with_suffix('.done').write_text(json.dumps({'finished_at_epoch':time.time()}))
