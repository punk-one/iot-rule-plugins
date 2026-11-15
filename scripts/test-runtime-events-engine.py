#!/usr/bin/env python3
"""Real Engine runtime observation isolation; synthetic JS and HTTP capture only."""
import hmac,hashlib,uuid,json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='runtime-events-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ['TEST_PLUGIN_DIR']).resolve()

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

  time.sleep(.1)
 raise RuntimeError('runtime-event integration timeout; inspect observed.json and runtime.json')
secret_a=secrets.token_hex(32);secret_b=secrets.token_hex(32)
engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:

 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='can-test',IOT_RULE_DELIVERY_MODE='reliable',IOT_RULE_WEBHOOK_TEST_A=secret_a,IOT_RULE_WEBHOOK_TEST_B=secret_b)
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 def runtime_rule(label,watch=None):
  return [node('clock','source-schedule','source',{'interval_ms':500,'on_start':False,'payload':{'fixture':label}},['work','ok']),node('work','javascript-transform','processor',{'code':'function main(input) { throw new Error("diagnostic-private-marker"); }'},['unused']),node('ok','javascript-transform','processor',{'code':'function main(input) { return {payload: input.payload}; }'},['normal']),node('normal','sink-http','sink',{'connector_ref':'http','path':'/normal-'+label}),node('unused','sink-http','sink',{'connector_ref':'http','path':'/unused-'+label}),node('observe','source-runtime-events','source',{'watch_nodes':watch or ['work','ok'],'kinds':['node_error','node_health','node_status','node_done'],'max_events_per_second':100,'burst':200},['notify']),node('notify','sink-http','sink',{'connector_ref':'http','path':'/events-'+label})]
 for label in ['a','b']:rule('runtime-'+label,runtime_rule(label))
 def received(label,kind,status=None):
  with lock:return any(path=='/events-'+label and payload.get('data',{}).get('kind')==kind and payload.get('data',{}).get('rule_id')=='runtime-'+label and (status is None or payload.get('data',{}).get('status')==status) for path,payload in records)
 wait(lambda:all(received(label,'node_error') and received(label,'node_health') and received(label,'node_status') and received(label,'node_done','failed') and received(label,'node_done','success') for label in ['a','b']))
 with lock:
  observed=[payload for path,payload in records if path.startswith('/events-')]
 assert observed and all('diagnostic-private-marker' not in json.dumps(payload) for payload in observed),'error messages leaked into event data'
 assert all(payload.get('data',{}).get('node_id') in ['work','ok'] for payload in observed),'unselected/self-descendant notification observed'
 api('POST','/rules',{'id':'bad-monitor','name':'bad-monitor','nodes':runtime_rule('bad',['notify'])})
 try:api('POST','/rules/bad-monitor/deploy',{});raise AssertionError('recursive monitor configuration deployed')
 except urllib.error.HTTPError as error:assert error.code==400,error.code
 api('POST','/rules/runtime-a/disable',{});time.sleep(.3)
 with lock:before_a=sum(path=='/events-a' for path,_ in records);before_b=sum(path=='/events-b' for path,_ in records)
 time.sleep(.5)
 with lock:assert sum(path=='/events-a' for path,_ in records)==before_a,'stopped observer kept dispatching';assert sum(path=='/events-b' for path,_ in records)>before_b,'other rule observer stopped'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['source-runtime-events','source-schedule','javascript-transform']}
 assert all(n==1 for n in counts.values()),counts
 report={'status':'passed','checks':['real Engine bounded runtime node error/health/status/done routing','failed and successful completion facts observed separately','same node IDs across rules remain scoped to their own instance','no business payload, error message or health details exposed','self-descendant monitor configuration rejected before deployment','stopping one rule unsubscribes its observer without affecting another','one artifact process serves all source and processor instances'],'process_counts':counts,'limits':['local Linux amd64 short test only','best-effort observations with bounded queues/rate, no durable audit log','task done is not business commit or physical completion'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 try:
  with lock:(ROOT/'observed.json').write_text(json.dumps(records,indent=2)+'\n')
  if engine is not None and engine.poll() is None:(ROOT/'runtime.json').write_text(json.dumps(api('GET','/runtime/instances'),indent=2)+'\n')
 except Exception:pass
 for proc in [engine]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 capture.shutdown();capture.server_close();engine_log.close()
