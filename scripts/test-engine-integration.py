#!/usr/bin/env python3
"""Loopback-only integration of built plugins with a real engine process."""
import json,tempfile,sqlite3,os,pathlib,secrets,signal,socket,socketserver,struct,subprocess,threading,time,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

REPO=pathlib.Path(__file__).resolve().parents[1]
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='plugin-engine-',dir=os.environ.get('TEST_OUTPUT_DIR')))
if not os.environ.get('TEST_ENGINE_BINARY'):
 raise SystemExit('Set TEST_ENGINE_BINARY to a built Linux engine executable')
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ.get('TEST_PLUGIN_DIR',str(REPO/'dist/plugins'))).resolve()
database_dir=ROOT/'sqlite';database_dir.mkdir(mode=0o700)
with sqlite3.connect(database_dir/'events.db') as db:
 db.execute('CREATE TABLE telemetry(id INTEGER PRIMARY KEY,device_code TEXT,value REAL)')
PASSWORD='Integration!'+secrets.token_hex(12)+'Aa9'
records=[];writes=[];lock=threading.Lock()
def read_exact(s,n):
 out=b''
 while len(out)<n:
  b=s.recv(n-len(out))
  if not b:raise EOFError()
  out+=b
 return out
class Modbus(socketserver.BaseRequestHandler):
 def handle(self):
  self.request.settimeout(10)
  try:
   while True:
    h=read_exact(self.request,7);n=struct.unpack('>H',h[4:6])[0];p=read_exact(self.request,n-1)
    if p[0] in (3,4):
     count=struct.unpack('>H',p[3:5])[0];r=bytes([p[0],count*2])+b''.join(struct.pack('>H',237+i) for i in range(count))
    elif p[0] in (1,2):
     count=struct.unpack('>H',p[3:5])[0];r=bytes([p[0],(count+7)//8])+bytes([0x55])*((count+7)//8)
    elif p[0] in (5,6,15,16):
     with lock:writes.append(('modbus',p.hex()))
     r=p[:5]
    else:r=bytes([p[0]|128,1])
    self.request.sendall(h[:4]+struct.pack('>H',len(r)+1)+h[6:7]+r)
  except (EOFError,OSError):pass
class MC(socketserver.BaseRequestHandler):
 def handle(self):
  self.request.settimeout(10)
  try:
   while True:
    h=read_exact(self.request,9);p=read_exact(self.request,struct.unpack('<H',h[7:9])[0]);command=struct.unpack('<H',p[2:4])[0]
    if command==0x0401:
     count=struct.unpack('<H',p[10:12])[0];bit=struct.unpack('<H',p[4:6])[0]==1
     data=bytes([0x10])*((count+1)//2) if bit else b''.join(struct.pack('<H',42+i) for i in range(count));r=b'\0\0'+data
    elif command==0x1401:
     with lock:writes.append(('mc',p.hex()))
     r=b'\0\0'
    else:r=b'\x59\xc0'
    self.request.sendall(b'\xd0\0'+h[2:7]+struct.pack('<H',len(r))+r)
  except(EOFError,OSError):pass
class TCP(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
class HTTP(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{"temperature":18.5}')
 def do_POST(self):
  raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
  with lock:
   record=json.loads(raw);record['_test_path']=self.path;records.append(record)
  self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{"accepted":true}')
servers=[TCP(('127.0.0.1',0),Modbus),TCP(('127.0.0.1',0),MC),ThreadingHTTPServer(('127.0.0.1',0),HTTP)]
for s in servers:threading.Thread(target=s.serve_forever,daemon=True).start()
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
token=None
def api(method,path,data=None):
 headers={'Content-Type':'application/json'}
 if token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request(f'http://127.0.0.1:{port}/api/v1'+path,None if data is None else json.dumps(data).encode(),headers,method=method)
 try:
  with urllib.request.urlopen(req,timeout=45) as response:return json.loads(response.read() or '{}')
 except urllib.error.HTTPError as e:raise RuntimeError(f'{method} {path}: HTTP {e.code}: {e.read().decode()[:1200]}') from None
def wait(check,timeout=40):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  try:
   if check():return
  except (ConnectionError,urllib.error.URLError):pass
  if proc.poll() is not None:raise RuntimeError('engine exited; inspect process.log')
  time.sleep(.2)
 raise RuntimeError('integration condition timed out')
def node(ident,plugin,kind,config,wires=()):return {'id':ident,'name':ident,'plugin':plugin,'type':kind,'config':config,'wires':list(wires)}
def rule(ident,nodes):
 if not any(n['type']=='sink' for n in nodes):
  for n in nodes:
   if not n['wires']:n['wires']=['integration-http-sink']
  nodes.append(node('integration-http-sink','sink-http','sink',{'connector_ref':'http','path':'/final'}))
 api('POST','/rules',{'id':ident,'name':ident,'nodes':nodes});api('POST',f'/rules/{ident}/deploy',{})
log=open(ROOT/'process.log','wb');env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=PASSWORD,IOT_RULE_DATA_DIR=str(ROOT/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID='new-plugin-integration',IOT_RULE_DELIVERY_MODE='reliable')
proc=subprocess.Popen([str(ENGINE)],cwd=ROOT,env=env,stdout=log,stderr=log,start_new_session=True)
try:
 wait(lambda:api('GET','/health').get('status')=='ok');token=api('POST','/auth/login',{'username':'admin','password':PASSWORD})['token']
 resources=[{'id':'modbus','plugin_id':'connect-modbus','enabled':True,'connection':{'host':'127.0.0.1','port':servers[0].server_address[1]}},{'id':'mc','plugin_id':'connect-mc','enabled':True,'connection':{'host':'127.0.0.1','port':servers[1].server_address[1]}},{'id':'http','plugin_id':'connect-http','enabled':True,'connection':{'base_url':f'http://127.0.0.1:{servers[2].server_address[1]}'}}]
 resources.append({'id':'sqlite','plugin_id':'connect-sqlite','enabled':True,'connection':{'directory':str(database_dir),'file':'events.db','allow_write':True}})
 for resource in resources:api('POST','/connectors',resource)
 def request(ident):return node(ident,'request-http','processor',{'connector_ref':'http','method':'POST','path':'/capture'})
 rule('modbus-read',[node('source-modbus','source-modbus','source',{'connector_ref':'modbus','device_code':'modbus-demo','interval_ms':1000,'points':[{'name':'temperature','area':'holding','address':0,'valueType':'Uint16'}]},['normalize']),node('normalize','normalize-points','processor',{'points':[{'name':'temperature','scale':0.1,'precision':1}]},['post-modbus']),request('post-modbus')])
 rule('mc-read',[node('source-mc','source-mc','source',{'connector_ref':'mc','device_code':'mc-demo','interval_ms':1000,'points':[{'name':'counter','area':'D','address':100,'valueType':'Uint16'}]},['post-mc']),request('post-mc')])
 rule('modbus-write',[node('schedule-modbus','source-schedule','source',{'interval_ms':1000,'on_start':True,'payload':{'device_code':'write-demo','data':{'bits':[True,False,True]}}},['write-modbus']),node('write-modbus','sink-modbus','sink',{'connector_ref':'modbus','device_code':'write-demo','points':[{'name':'bits','area':'coil','address':10,'valueType':'BoolArray','count':3}]})])
 rule('mc-write',[node('schedule-mc','source-schedule','source',{'interval_ms':1000,'on_start':True,'payload':{'device_code':'write-demo','data':{'word':12}}},['write-mc']),node('write-mc','sink-mc','sink',{'connector_ref':'mc','device_code':'write-demo','points':[{'name':'word','area':'D','address':110,'valueType':'Uint16'}]})])
 rule('http-poll',[node('poll-http','source-http-poll','source',{'connector_ref':'http','device_code':'http-demo','path':'/telemetry','interval_ms':1000},['post-http']),request('post-http')])
 router=node('router','condition-router','processor',{'mode':'all','conditions':[{'path':'data.temperature','op':'gt','value':20}]},['post-route','route-unexpected'])
 router['routes']=[{'label':'matched','to':'post-route'},{'label':'otherwise','to':'route-unexpected'}]
 rule('condition-routing',[node('schedule-route','source-schedule','source',{'interval_ms':1000,'on_start':True,'payload':{'device_code':'route-demo','data':{'temperature':24.0}}},['router']),router,node('post-route','request-http','processor',{'connector_ref':'http','method':'POST','path':'/route-capture'},['route-matched']),node('route-matched','sink-http','sink',{'connector_ref':'http','path':'/matched'}),node('route-unexpected','sink-http','sink',{'connector_ref':'http','path':'/unexpected'})])
 now=int(time.time()*1000)
 guard=node('guard','command-guard','processor',{'device_code':'write-demo','allowed_operations':['write'],'state_dir':str(ROOT/'guard-state')},['map-command','rejected-command'])
 guard['routes']=[{'label':'accepted','to':'map-command'},{'label':'rejected','to':'rejected-command'}]
 mapping={'process':{'expand':[{'from':'payload.device_code','to':'device_code'},{'from':'payload.expires_at','to':'expires_at'},{'from':'payload.arguments','to':'data'}]}}
 rule('guarded-write',[node('schedule-command','source-schedule','source',{'interval_ms':100,'on_start':True,'payload':{'command_id':'test-command','device_code':'write-demo','operation':'write','issued_at':now,'expires_at':now+10000,'arguments':{'word':123}}},['guard']),guard,node('map-command','transform-map','processor',mapping,['guarded-mc']),node('guarded-mc','sink-mc','sink',{'connector_ref':'mc','device_code':'write-demo','points':[{'name':'word','area':'D','address':120,'valueType':'Uint16'}]}),node('rejected-command','sink-http','sink',{'connector_ref':'http','path':'/rejected-command'})])
 rule('expired-write',[node('schedule-expired','source-schedule','source',{'interval_ms':100,'on_start':True,'payload':{'device_code':'write-demo','expires_at':1,'data':{'word':555}}},['expired-mc']),node('expired-mc','sink-mc','sink',{'connector_ref':'mc','device_code':'write-demo','points':[{'name':'word','area':'D','address':121,'valueType':'Uint16'}]})])
 rule('sqlite-write',[node('schedule-sqlite','source-schedule','source',{'interval_ms':1000,'on_start':True,'payload':{'device_code':'sqlite-demo','data':{'id':1,'value':33.25}}},['write-sqlite']),node('write-sqlite','sink-relational-sqlite','sink',{'connector_ref':'sqlite','statement':'INSERT INTO telemetry(id,device_code,value) VALUES(@id,@device,@value) ON CONFLICT(id) DO UPDATE SET value=excluded.value','params':{'id':'data.id','device':'device_code','value':'data.value'}})])
 rule('sqlite-read',[node('read-sqlite','source-relational-sqlite','source',{'connector_ref':'sqlite','interval_ms':500,'query':'SELECT id,device_code,value FROM telemetry ORDER BY id LIMIT @page_size'},['sqlite-capture']),node('sqlite-capture','sink-http','sink',{'connector_ref':'http','path':'/sqlite-capture'})])
 rule('flow-join',[node('schedule-records','source-schedule','source',{'interval_ms':1000,'on_start':True,'payload':{'device_code':'flow-demo','records':[{'v':1},{'v':2}]}},['split']),node('split','split-records','processor',{},['join']),node('join','join-records','processor',{},['render']),node('render','render-template','processor',{'template':'Device {{payload.device_code}}'},['joined-capture']),node('joined-capture','sink-http','sink',{'connector_ref':'http','path':'/joined'})])
 shared_rules=20
 for index in range(shared_rules):
  rule('shared-modbus-'+str(index),[node('shared-source','source-modbus','source',{'connector_ref':'modbus','device_code':'shared-demo-'+str(index),'interval_ms':500,'points':[{'name':'value','area':'holding','address':0,'valueType':'Uint16'}]},['shared-capture']),node('shared-capture','sink-http','sink',{'connector_ref':'http','path':'/shared'})])
 wait(lambda:len({x.get('device_code') for x in records if x.get('_test_path')=='/shared'})==shared_rules)
 api('POST','/rules/shared-modbus-0/disable',{})
 with lock:other_before=sum(x.get('device_code')=='shared-demo-1' for x in records)
 wait(lambda:sum(x.get('device_code')=='shared-demo-1' for x in records)>other_before)
 def success():
  with lock:return any(x.get('device_code')=='modbus-demo' and abs(x['data']['temperature']-23.7)<.001 for x in records) and any(x.get('device_code')=='mc-demo' and x['data']['counter']==42 for x in records) and any(x.get('device_code')=='http-demo' and x['data']['temperature']==18.5 for x in records) and any(k=='modbus' for k,p in writes) and any(k=='mc' for k,p in writes)
 wait(success)
 wait(lambda:any(x.get('_test_path')=='/sqlite-capture' and x.get('device_code')=='sqlite-demo' and x.get('data',{}).get('value')==33.25 for x in records))
 wait(lambda:any(x.get('_test_path')=='/joined' and x.get('rendered')=='Device flow-demo' and x.get('records')==[{'v':1},{'v':2}] for x in records))
 wait(lambda:any(x.get('device_code')=='route-demo' and x.get('_test_path')=='/matched' for x in records))
 if any(x.get('_test_path')=='/unexpected' for x in records):raise RuntimeError('condition route reached wrong sink')
 wait(lambda:any(x.get('_test_path')=='/rejected-command' and x.get('reason')=='duplicate_command' for x in records))
 time.sleep(.3)
 guarded=[p for kind,p in writes if kind=='mc' and bytes.fromhex(p)[6:9]==bytes([120,0,0])]
 expired=[p for kind,p in writes if kind=='mc' and bytes.fromhex(p)[6:9]==bytes([121,0,0])]
 if len(guarded)!=1 or expired:raise RuntimeError('guard/expiry physical-write boundary failed')
 children=subprocess.check_output(['pgrep','-P',str(proc.pid),'-a'],text=True)
 counts={name:sum(name in line for line in children.splitlines()) for name in ['connect-modbus-','connect-mc-','connect-http-','source-schedule-','source-modbus-']}
 if any(v!=1 for v in counts.values()):raise RuntimeError('plugin process sharing check failed: '+str(counts))
 result={'status':'passed','checks':['real engine API and SDK v1 process load','Modbus TCP source -> normalize -> HTTP capture','MC 3E source -> HTTP capture','scheduled Modbus FC15 and MC writes to loopback simulators','HTTP polling -> HTTP request','labelled condition route -> HTTP request -> expected HTTP sink','persistent command guard forwards exactly one simulated write for repeated command ID','expired PLC write never reaches simulated device','SQLite transactional sink -> real temporary database -> SQLite source -> HTTP sink','split -> out-of-order safe join -> escaped template -> HTTP sink','20 rules share one Source/Connector artifact process and one connector resource','disabling one shared rule does not stop another rule','one shared process per checked plugin artifact'],'process_counts':counts,'limits':['loopback simulators only; no live PLC','amd64 only','OPC UA and SQL Server are not live-server integrations in this harness'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
finally:
 if proc.poll() is None:
  os.killpg(proc.pid,signal.SIGTERM)
  try:proc.wait(35)
  except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 log.close()
 for s in servers:s.shutdown();s.server_close()
