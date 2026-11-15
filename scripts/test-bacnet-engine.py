#!/usr/bin/env python3
"""Private loopback BACnet fixture and real Engine process; no live broker."""
import json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='bacnet-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ['TEST_PLUGIN_DIR']).resolve()
FIXTURE=pathlib.Path(os.environ['TEST_BACNET_SERVER']).resolve()
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
fixture_log=open(ROOT/'fixture.log','wb');fixture=subprocess.Popen([str(FIXTURE),'1234','Test BACnet Server'],cwd=ROOT,env=dict(os.environ,BACNET_DATALINK='bip',BACNET_IFACE='lo',BACNET_IP_PORT='47808'),stdout=fixture_log,stderr=fixture_log,start_new_session=True)
engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:
 if os.environ.get('TEST_BACNET_ISOLATED')!='1':raise RuntimeError('requires isolated local network namespace')
 time.sleep(.3)
 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='can-test',IOT_RULE_DELIVERY_MODE='reliable')
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 point={'name':'value','object_type':2,'instance':1,'property':85,'valueType':'Float32'}
 missing=dict(point,name='missing',instance=4194000)
 api('POST','/connectors',{'id':'bacnet','plugin_id':'connect-bacnet','enabled':True,'connection':{'host':'127.0.0.1','port':47808,'expected_device_instance':1234,'read_allowlist':[point,missing],'write_allowlist':[point],'allow_write':True}})
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 for label in ['a','b']:
  rule('consume-'+label,[node('bacnet-'+label,'source-bacnet','source',{'connector_ref':'bacnet','device_code':'test-'+label,'interval_ms':100,'points':[point,missing]},['capture-'+label]),node('capture-'+label,'sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 def received(label,value=None):
  with lock:return any(path=='/'+label and payload.get('quality',{}).get('value')=='good' and payload.get('quality',{}).get('missing','').startswith('bad:') and payload.get('data',{}).get('missing') is None and (value is None or payload.get('data',{}).get('value')==value) for path,payload in records)
 wait(lambda:received('a') and received('b'))
 now=int(time.time()*1000)
 guard=node('guard','command-guard','processor',{'device_code':'bacnet-target','allowed_operations':['write'],'state_dir':str(ROOT/'guard')},['bacnet-out','rejected'])
 guard['routes']=[{'label':'accepted','to':'bacnet-out'},{'label':'rejected','to':'rejected'}]
 rule('publish',[node('schedule','source-schedule','source',{'interval_ms':100,'on_start':True,'payload':{'device_code':'bacnet-target','command_id':'one-test-command','operation':'write','issued_at':now,'expires_at':now+10000,'arguments':{'value':42.75},'data':{'value':42.75}}},['guard']),guard,node('bacnet-out','sink-bacnet','sink',{'connector_ref':'bacnet','device_code':'bacnet-target','points':[point]}),node('rejected','sink-http','sink',{'connector_ref':'http','path':'/rejected'})])
 wait(lambda:received('a',42.75) and received('b',42.75))
 api('POST','/rules/consume-a/disable',{})
 time.sleep(.2)
 with lock:before=sum(path=='/a' for path,_ in records);before_b=sum(path=='/b' for path,_ in records)
 time.sleep(.5)
 with lock:assert sum(path=='/a' for path,_ in records)==before,'stopped source emitted new record';assert sum(path=='/b' for path,_ in records)>before_b,'remaining Source stopped'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['connect-bacnet','source-bacnet','sink-bacnet']}
 assert all(n==1 for n in counts.values()),counts
 report={'status':'passed','checks':['independent bacnet-stack 1.7.0 and real Engine Source/Sink','two Sources share one resource with serialized batches','good value and missing-object bad quality preserved','controlled property write read back as 42.75','stopping one Source leaves another active','one process per BACnet artifact'],'process_counts':counts,'limits':['private user/network namespace, no physical BACnet device','direct IPv4 unicast scalars only; no routed/segmented/COV stack','Linux amd64 runtime; ARM64 compilation tracked separately'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 for proc in [engine,fixture]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 capture.shutdown();capture.server_close();engine_log.close();fixture_log.close()
