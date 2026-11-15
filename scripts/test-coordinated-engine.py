#!/usr/bin/env python3
"""Real Engine + private Modbus TCP simulator; writes affect memory only."""
import json,os,pathlib,secrets,signal,socket,struct,subprocess,tempfile,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='coordinated-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve();PLUGINS=pathlib.Path(os.environ['TEST_PLUGIN_DIR']).resolve()
records=[];lock=threading.Lock();token=None;engine=None;peers=[];opens=0;closes=0;reads=0;writes=0;value=123;drop_next_write=False
class Capture(BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def do_POST(self):
  v=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  with lock:records.append((self.path,v))
  self.send_response(200);self.end_headers();self.wfile.write(b'{}')
capture=ThreadingHTTPServer(('127.0.0.1',0),Capture);threading.Thread(target=capture.serve_forever,daemon=True).start()
listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(8);listener.settimeout(.2);stop=threading.Event()
def exact(c,n):
 b=b''
 while len(b)<n:
  x=c.recv(n-len(b))
  if not x:raise EOFError()
  b+=x
 return b
def serve(c):
 global reads,writes,value,closes,drop_next_write
 try:
  while not stop.is_set():
   head=exact(c,7);tid,pid,length,unit=struct.unpack('>HHHB',head);pdu=exact(c,length-1);fc=pdu[0]
   if fc==3:
    address,count=struct.unpack('>HH',pdu[1:]);assert address==0 and count==1
    with lock:reads+=1;v=value
    response=bytes([3,2])+struct.pack('>H',v)
   elif fc==6:
    address,v=struct.unpack('>HH',pdu[1:]);assert address==0
    with lock:writes+=1;value=v;drop=drop_next_write;drop_next_write=False
    if drop:return
    response=pdu
   else:raise AssertionError('unexpected Modbus function')
   c.sendall(struct.pack('>HHHB',tid,pid,len(response)+1,unit)+response)
 except (EOFError,OSError):pass
 finally:
  c.close()
  with lock:closes+=1
def accept():
 global opens
 while not stop.is_set():
  try:
   c,_=listener.accept()
   with lock:opens+=1;peers.append(c)
   threading.Thread(target=serve,args=(c,),daemon=True).start()
  except socket.timeout:pass
  except OSError:return
threading.Thread(target=accept,daemon=True).start()
probe=socket.socket();probe.bind(('127.0.0.1',0));port=probe.getsockname()[1];probe.close()
def api(method,path,data=None):
 h={'Content-Type':'application/json'}
 if token:h['Authorization']='Bearer '+token
 request=urllib.request.Request(f'http://127.0.0.1:{port}/api/v1'+path,None if data is None else json.dumps(data).encode(),h,method=method)
 with urllib.request.urlopen(request,timeout=15) as r:return json.loads(r.read() or '{}')
def node(id,plugin,kind,config,wires=()):return dict(id=id,name=id,plugin=plugin,type=kind,config=config,wires=list(wires))
def deploy(id,nodes):api('POST','/rules',dict(id=id,name=id,nodes=nodes));api('POST',f'/rules/{id}/deploy',{})
def wait(fn,seconds=15):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  try:
   if fn():return
  except (urllib.error.URLError,ConnectionError):pass
  if engine.poll() is not None:raise RuntimeError('engine exited')
  time.sleep(.025)
 raise RuntimeError('coordinated scan integration timeout')
def count(path):
 with lock:return sum(p==path for p,_ in records)
log=open(ROOT/'engine.log','wb');password='Test!'+secrets.token_hex(16)+'Aa9'
try:
 env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=password,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='coordinated-test',IOT_RULE_DELIVERY_MODE='reliable')
 engine=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=log,stderr=log,start_new_session=True)
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',dict(username='admin',password=password))['token']
 api('POST','/connectors',dict(id='plc',plugin_id='connect-modbus',enabled=True,connection=dict(host='127.0.0.1',port=listener.getsockname()[1],timeout_ms=500)))
 api('POST','/connectors',dict(id='http',plugin_id='connect-http',enabled=True,connection=dict(base_url=f'http://127.0.0.1:{capture.server_address[1]}')))
 for label,period in [('fast',100),('normal',500),('slow',20000)]:
  deploy('read-'+label,[node('read','source-modbus','source',dict(connector_ref='plc',device_code=label,scan_mode='coordinated',interval_ms=period,timeout_ms=500,points=[dict(name=label,area='holding',address=0,valueType='Uint16',scale=0.1)]),['capture']),node('capture','sink-http','sink',dict(connector_ref='http',path='/'+label))])
 wait(lambda:count('/fast')>=12 and count('/normal')>=3)
 with lock:
  assert opens==1,('sessions',opens)
  fast_count=sum(p=='/fast' for p,_ in records)
  assert reads<=fast_count+3,('unmerged reads',reads,fast_count)
  for path,payload in records:assert payload['data'][path[1:]]==12.3 and payload['quality'][path[1:]]=='good',payload
 deploy('write',[node('manual','source-manual','source',dict(allow_live_trigger=True),['write']),node('write','sink-modbus','sink',dict(connector_ref='plc',device_code='test-target',points=[dict(name='command',area='holding',address=0,valueType='Uint16')]))])
 started=time.monotonic();api('POST','/rules/write/nodes/manual/trigger',dict(confirm_live=True,payload=dict(device_code='test-target',expires_at=int(time.time()*1000)+5000,data=dict(command=321))))
 wait(lambda:writes==1 and any(p=='/fast' and v['data']['fast']==32.1 for p,v in records));write_seconds=time.monotonic()-started;assert write_seconds<2

 with lock:drop_next_write=True
 api('POST','/rules/write/nodes/manual/trigger',dict(confirm_live=True,payload=dict(device_code='test-target',expires_at=int(time.time()*1000)+5000,data=dict(command=456))))
 wait(lambda:writes==2)
 time.sleep(2.2)
 assert writes==2,'ambiguous device write automatically repeated after a lost reply'
 api('POST','/rules/read-fast/disable',{});api('POST','/rules/read-normal/disable',{});api('POST','/rules/write/disable',{})
 wait(lambda:closes==opens)
 with lock:before=reads;opens_before=opens;slow_before=sum(p=='/slow' for p,_ in records)
 time.sleep(.5)
 assert reads==before,'obsolete scans continued after fast/normal rules stopped'
 wait(lambda:count('/slow')>slow_before,seconds=23)
 wait(lambda:closes==opens)
 assert opens==opens_before+1,('slow-only batch did not open exactly one session',opens,opens_before)
 api('POST','/rules/read-slow/disable',{});time.sleep(.2)
 args=subprocess.check_output(['ps','-eo','args'],text=True).splitlines();counts={name:sum(str(PLUGINS) in line and '/'+name+'-' in line for line in args) for name in ['connect-modbus','source-modbus']};assert all(v==1 for v in counts.values()),counts
 report=dict(status='passed',checks=['real Engine SDK v1 coordinated scans across three rules','100ms/500ms/20s periods share one Connector Resource','equal-time duplicate native points read once and projected to local aliases','per-Source engineering scaling and quality preserved','write executes during high-frequency reads without starvation','applied write with lost reply is not automatically executed again','removing fast demand releases session while slow Source remains','20s-only demand opens and closes one batch session','no stopped-rule scan backlog','one process per artifact'],write_latency_seconds=write_seconds,reads=reads,session_opens=opens,session_closes=closes,process_counts=counts,limits=['loopback simulator, Linux amd64 only','best-effort latest samples; no business or physical completion ACK','hardware/ARM/long-run acceptance remains separate'],run_directory=str(ROOT))
 (ROOT/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 if engine and engine.poll() is None:
  os.killpg(engine.pid,signal.SIGTERM)
  try:engine.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(engine.pid,signal.SIGKILL);engine.wait()
 stop.set();listener.close()
 for peer in peers:
  try:peer.close()
  except OSError:pass
 capture.shutdown();capture.server_close();log.close()
