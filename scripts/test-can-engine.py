#!/usr/bin/env python3
"""Private loopback CAN fixture and real Engine process; no live broker."""
import json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='can-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ['TEST_PLUGIN_DIR']).resolve()
FIXTURE=pathlib.Path(os.environ['TEST_CAN_FIXTURE']).resolve()
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
  if fixture.poll() is not None:raise RuntimeError('CAN fixture exited; inspect fixture.log')
  time.sleep(.1)
 raise RuntimeError('CAN integration timeout')
fixture_log=open(ROOT/'fixture.log','wb');fixture=subprocess.Popen([str(FIXTURE)],cwd=ROOT,stdout=subprocess.PIPE,stderr=fixture_log,start_new_session=True)
engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:
 ready=json.loads(fixture.stdout.readline());control=ready['control_url']
 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='can-test',IOT_RULE_DELIVERY_MODE='reliable')
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 api('POST','/connectors',{'id':'can','plugin_id':'connect-can','enabled':True,'connection':{'interface':os.environ['TEST_CAN_IFACE'],'read_ids':[{'id':'0x123'}],'write_ids':[{'id':'0x321'}],'can_fd':True,'allow_write':True}})
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 for label in ['a','b']:
  rule('consume-'+label,[node('can-'+label,'source-can','source',{'connector_ref':'can','device_code':'test-'+label,'ids':[{'id':'0x123'}]},['capture-'+label]),node('capture-'+label,'sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 now=int(time.time()*1000)
 guard=node('guard','command-guard','processor',{'device_code':'vcan-target','allowed_operations':['write'],'state_dir':str(ROOT/'guard')},['can-out','rejected'])
 guard['routes']=[{'label':'accepted','to':'can-out'},{'label':'rejected','to':'rejected'}]
 rule('publish',[node('schedule','source-schedule','source',{'interval_ms':100,'on_start':True,'payload':{'device_code':'vcan-target','command_id':'one-test-command','operation':'write','issued_at':now,'expires_at':now+10000,'arguments':{'raw_base64':'CQg='}}},['guard']),guard,node('can-out','sink-can','sink',{'connector_ref':'can','device_code':'vcan-target','id':'0x321','data_path':'arguments.raw_base64'}),node('rejected','sink-http','sink',{'connector_ref':'http','path':'/rejected'})])
 request(control+'/inject',{'id':291,'data':'AQI='},'POST')
 def received(label,value):
  with lock:return any(path=='/'+label and payload.get('data',{}).get('raw_base64')==value and payload.get('quality',{}).get('frame')=='good' for path,payload in records)
 wait(lambda:received('a','AQI=') and received('b','AQI='))
 wait(lambda:any(x.get('id')=='0x321' and x.get('raw_base64')=='CQg=' for x in request(control+'/frames')))
 time.sleep(.6)
 assert sum(x.get('id')=='0x321' for x in request(control+'/frames'))==1,'repeated command produced duplicate CAN writes'
 api('POST','/rules/consume-a/disable',{})
 with lock:before=sum(path=='/a' for path,_ in records)
 request(control+'/inject',{'id':291,'data':'AwQ='},'POST');wait(lambda:received('b','AwQ='));time.sleep(.2)
 with lock:assert sum(path=='/a' for path,_ in records)==before,'stopped source emitted new record'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['connect-can','source-can','sink-can']}
 assert all(n==1 for n in counts.values()),counts
 report={'status':'passed','checks':['real Linux vcan kernel with real Engine CAN Source/Sink','two Sources share one connector resource and raw socket','frame bytes and good quality preserved','command-guard forwards repeated command only once to virtual bus','stopping one Source leaves another active','one process per CAN artifact'],'process_counts':counts,'limits':['private user/network namespace and virtual CAN only; no real ECU','Linux amd64 only'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 for proc in [engine,fixture]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 capture.shutdown();capture.server_close();engine_log.close();fixture_log.close()
