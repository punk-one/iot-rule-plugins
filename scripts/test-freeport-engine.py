#!/usr/bin/env python3
"""Private loopback freeport fixture and real Engine process; no live broker."""
import json,os,pathlib,secrets,signal,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='freeport-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
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
peers=[]
listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(4);listener.settimeout(.2);peer_stop=threading.Event()
def accept_peers():
 while not peer_stop.is_set():
  try:
   connection,_=listener.accept()
   with lock:peers.append(connection)
  except socket.timeout:pass
  except OSError:return
threading.Thread(target=accept_peers,daemon=True).start()

engine=None;engine_log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:

 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='can-test',IOT_RULE_DELIVERY_MODE='reliable')
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=engine_log,stderr=engine_log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':password})['token']
 frame={'mode':'delimiter','delimiter':'IQ=='}
 api('POST','/connectors',{'id':'tcp','plugin_id':'connect-tcp','enabled':True,'connection':{'host':'127.0.0.1','port':listener.getsockname()[1],'mode':'stream','stream_frame':frame,'timeout_ms':500}})
 api('POST','/connectors',{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{capture.server_address[1]}'}})
 for label in ['a','b']:
  rule('consume-'+label,[node('raw-'+label,'source-freeport','source',{'connector_ref':'tcp','device_code':'test-'+label,'mode':'stream','frame':frame},['capture-'+label]),node('capture-'+label,'sink-http','sink',{'connector_ref':'http','path':'/'+label})])
 wait(lambda:len(peers)==1)
 time.sleep(.2)
 peers[0].sendall(b'first!second!')
 def received(label,value):
  with lock:return any(path=='/'+label and payload.get('data',{}).get('raw')==value and payload.get('quality',{}).get('frame')=='good' and payload.get('device_code')=='test-'+label for path,payload in records)
 wait(lambda:all(received(label,value) for label in ['a','b'] for value in ['first','second']))
 assert len(peers)==1,'multiple TCP sessions for shared resource'
 api('POST','/rules/consume-a/disable',{});time.sleep(.2)
 with lock:before=sum(path=='/a' for path,_ in records)
 peers[0].sendall(b'only-b!');wait(lambda:received('b','only-b'))
 peers[0].shutdown(socket.SHUT_RDWR);peers[0].close()
 wait(lambda:len(peers)==2)
 peers[-1].sendall(b'recovered!');wait(lambda:received('b','recovered'))
 with lock:assert sum(path=='/a' for path,_ in records)==before,'stopped source emitted more frames'
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and ('/'+name+'-') in line for line in args) for name in ['connect-tcp','source-freeport']}
 assert all(n==1 for n in counts.values()),counts
 report={'status':'passed','checks':['real TCP peer and Engine continuous freeport Source','two Sources share one resource and one physical TCP session','multiple coalesced frames preserved and copied per source','slow-source behavior covered by native transport tests','stopping one Source leaves another active','remote disconnect recovers on bounded actual demand','one process per artifact'],'process_counts':counts,'limits':['loopback Linux amd64 only; no physical serial device','best-effort delivery, no durable frame replay or business ACK'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 for proc in [engine]:
  if proc is not None and proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 peer_stop.set();listener.close()
 for peer in peers:
  try:peer.close()
  except OSError:pass
 capture.shutdown();capture.server_close();engine_log.close()
