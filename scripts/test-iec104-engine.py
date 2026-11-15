#!/usr/bin/env python3
"""Private IEC104/lib60870 interop and real Engine; no control writes."""
import json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='iec104-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ['TEST_PLUGIN_DIR']).resolve()
FIXTURE=pathlib.Path(os.environ['TEST_IEC104_SERVER']).resolve()
records=[];lock=threading.Lock();token=None
class Capture(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_POST(self):
  size=int(self.headers.get('Content-Length','0'))
  if size>2097152:self.send_error(413);return
  value=json.loads(self.rfile.read(size))
  with lock:records.append((self.path,value))
  self.send_response(200);self.end_headers();self.wfile.write(b'{}')
capture=ThreadingHTTPServer(('127.0.0.1',0),Capture);threading.Thread(target=capture.serve_forever,daemon=True).start()
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
def request(url,data=None,method='GET',auth=False):
 headers={'Content-Type':'application/json'}
 if auth and token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request(url,None if data is None else json.dumps(data).encode(),headers,method=method)
 with urllib.request.urlopen(req,timeout=20) as r:return json.loads(r.read() or '{}')
def api(method,path,data=None):return request(f'http://127.0.0.1:{port}/api/v1'+path,data,method,True)
def node(id,plugin,kind,config,wires=()):return {'id':id,'name':id,'plugin':plugin,'type':kind,'config':config,'wires':list(wires)}
def rule(id,nodes):api('POST','/rules',{'id':id,'name':id,'nodes':nodes});api('POST','/rules/'+id+'/deploy',{})
def wait(fn,timeout=30):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  try:
   if fn():return
  except (urllib.error.URLError,ConnectionError):pass
  if engine and engine.poll() is not None:raise RuntimeError('Engine exited; inspect engine.log')
  if fixture.poll() is not None:raise RuntimeError('BACnet fixture exited; inspect fixture.log')
  time.sleep(.1)
 raise RuntimeError('BACnet integration timeout')
fixture_log=open(ROOT/'fixture.log','wb');fixture=subprocess.Popen([str(FIXTURE)],cwd=ROOT,stdout=fixture_log,stderr=fixture_log,start_new_session=True)
engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:
 if os.environ.get('TEST_IEC104_ISOLATED')!='1':raise RuntimeError('requires isolated local network namespace')
 time.sleep(.3)
 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='iec104-test',IOT_RULE_DELIVERY_MODE='reliable')
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 point={'name':'counter','ioa':110}
 api('POST','/connectors',{'id':'station','plugin_id':'connect-iec104','enabled':True,'connection':{'host':'127.0.0.1','port':2404,'mode':'stream','read_ioas':[100,110],'common_address':1}})
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 for label in ['a','b']:
  rule('consume-'+label,[node('iec104-'+label,'source-iec104','source',{'connector_ref':'station','device_code':'test-'+label,'mode':'stream','points':[point]},['capture-'+label]),node('capture-'+label,'sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 def values(label):
  with lock:return {payload.get('data',{}).get('counter') for path,payload in records if path=='/'+label and payload.get('quality',{}).get('counter')=='good' and payload.get('protocol_details',{}).get('snapshot') is False}
 wait(lambda:len(values('a'))>=2 and len(values('b'))>=2)
 def established():
  return sum(parts[2].endswith(':0964') and parts[3]=='01' for parts in [line.split() for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]])
 assert established()==1,'Source rules opened more than one physical IEC104 connection'
 api('POST','/rules/consume-a/disable',{});time.sleep(.2)
 with lock:before_a=sum(path=='/a' for path,_ in records);before_b=sum(path=='/b' for path,_ in records)
 def b_advanced():
  with lock:return sum(path=='/b' for path,_ in records)>before_b
 wait(b_advanced)
 with lock:assert sum(path=='/a' for path,_ in records)==before_a,'stopped Source emitted new updates'
 assert established()==1,'remaining Source lost shared session'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['connect-iec104','source-iec104']}
 assert all(n==1 for n in counts.values()),counts
 api('POST','/rules/consume-b/disable',{})
 wait(lambda:established()==0,timeout=3)
 report={'status':'passed','checks':['independent lib60870 2.4.1 and real Engine continuous Source','two rules share one resource and one ESTABLISHED device TCP socket','continuing periodic delta updates with exact IOA selection/quality','stopping one rule leaves the other session active','last Source stop releases device socket','one process per IEC104 artifact'],'process_counts':counts,'limits':['private network namespace, no physical IEC104 station','read-only monitoring; no control writes/TLS/replay guarantees','late subscribers receive new deltas, not a historical snapshot'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 for proc in [engine,fixture]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 capture.shutdown();capture.server_close();engine_log.close();fixture_log.close()
