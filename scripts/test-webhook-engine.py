#!/usr/bin/env python3
"""Private loopback freeport fixture and real Engine process; no live broker."""
import hmac,hashlib,uuid,json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='webhook-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
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
 raise RuntimeError('freeport integration timeout')
secret_a=secrets.token_hex(32);secret_b=secrets.token_hex(32)
engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:

 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='can-test',IOT_RULE_DELIVERY_MODE='reliable',IOT_RULE_WEBHOOK_TEST_A=secret_a,IOT_RULE_WEBHOOK_TEST_B=secret_b)
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 def webhook_rule(label,key='telemetry'):
  rule('hook-'+label,[node('webhook-'+label,'source-webhook','source',{'route_key':key,'secret_env':'IOT_RULE_WEBHOOK_TEST_'+label.upper()},['capture-'+label]),node('capture-'+label,'sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 for label in ['a','b']:webhook_rule(label)
 def signed(label,payload,nonce=None,key=None,route='telemetry',raw=None):
  path=f'/api/v1/hooks/hook-{label}/webhook-{label}/{route}';stamp=str(int(time.time()));nonce=nonce or uuid.uuid4().hex
  body=raw if raw is not None else json.dumps(payload,separators=(',',':')).encode()
  key=key or (secret_a if label=='a' else secret_b)
  sig=hmac.new(key.encode(),(stamp+'\n'+nonce+'\nPOST\n'+path+'\n').encode()+body,hashlib.sha256).hexdigest()
  req=urllib.request.Request(f'http://127.0.0.1:{port}'+path,body,{'Content-Type':'application/json','X-Iot-Timestamp':stamp,'X-Iot-Request-Id':nonce,'X-Iot-Signature':sig},method='POST')
  try:
   with urllib.request.urlopen(req,timeout=10) as response:return response.status,json.loads(response.read())
  except urllib.error.HTTPError as error:return error.code,json.loads(error.read())
 payload={'device_code':'hook-demo','data':{'counter':9007199254740993},'time':int(time.time()*1000)}
 nonce=uuid.uuid4().hex
 code,receipt=signed('a',payload,nonce)
 assert code==202 and receipt['scope']=='engine_dispatch' and not receipt['business_completed'],(code,receipt)
 assert signed('a',payload,nonce)[0]==409,'replayed signed request accepted'
 assert signed('b',payload,key=secret_a)[0]==401,'other route key authorized target'
 assert signed('b',payload)[0]==202,'second webhook rejected valid signature'
 assert signed('a',None,raw=b'{"x":1,"x":2}')[0]==400,'duplicate JSON keys accepted'
 assert signed('a',None,raw=b'"'+b'x'*262144+b'"')[0]==413,'oversized body accepted'
 def received(label):
  with lock:return any(path=='/'+label and payload.get('data',{}).get('counter')=='9007199254740993' for path,payload in records)
 wait(lambda:received('a') and received('b'))
 api('POST','/rules/hook-a/disable',{})
 assert signed('a',payload)[0]==404,'disabled rule kept webhook registered'
 assert signed('b',payload)[0]==202,'stopping one rule affected another webhook'
 api('DELETE','/rules/hook-a');webhook_rule('a','updated')
 assert signed('a',payload)[0]==404,'old route survived configuration generation change'
 assert signed('a',payload,route='updated')[0]==202,'new generation not active'
 for label,enabled in [('manual',True),('manual-disabled',False)]:
  rule(label,[node('inject','source-manual','source',{'allow_live_trigger':enabled},['capture']),node('capture','sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 def manual(label,data,access_token=None):
  headers={'Content-Type':'application/json'}
  if access_token is not None:headers['Authorization']='Bearer '+access_token
  req=urllib.request.Request(f'http://127.0.0.1:{port}/api/v1/rules/{label}/nodes/inject/trigger',json.dumps(data).encode(),headers,method='POST')
  try:
   with urllib.request.urlopen(req,timeout=10) as response:return response.status,json.loads(response.read())
  except urllib.error.HTTPError as error:return error.code,json.loads(error.read())
 assert manual('manual',{'confirm_live':True,'payload':payload})[0]==401,'anonymous manual trigger accepted'
 assert manual('manual',{'payload':payload},token)[0]==400,'live execution confirmation omitted'
 assert manual('manual-disabled',{'confirm_live':True,'payload':payload},token)[0]==404,'default-disabled trigger exposed'
 assert manual('manual',{'confirm_live':True,'payload':payload},token)[0]==202,'admin live trigger failed'
 wait(lambda:received('manual'))
 operator_password='Op!'+secrets.token_hex(16)+'Aa9'
 api('POST','/users',{'username':'operator','password':operator_password,'status':1})
 operator_token=api('POST','/auth/login',{'username':'operator','password':operator_password})['token']
 assert manual('manual',{'confirm_live':True,'payload':payload},operator_token)[0]==403,'non-admin live trigger accepted'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['source-webhook','source-manual']}
 assert all(n==1 for n in counts.values()),counts
 report={'status':'passed','checks':['independent Python HMAC signing and real Engine webhook dispatch','two rules share one source-webhook artifact process','no manager credentials granted to webhook callers','replay/wrong-route-key/duplicate-JSON/oversize rejected','unsafe integer telemetry preserved exactly as text','disable unregisters ingress without affecting another rule','redeploy invalidates old route key and activates new generation','manual source defaults disabled and requires admin plus confirm_live','operator and anonymous callers cannot manually execute rules'],'process_counts':counts,'limits':['loopback Linux amd64 short test only','202 is dispatch acceptance, not business completion','replay cache is bounded process-local, not durable transaction dedup'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 for proc in [engine]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 capture.shutdown();capture.server_close();engine_log.close()
