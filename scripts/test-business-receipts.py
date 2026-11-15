#!/usr/bin/env python3
"""Loopback MQTT v3/QoS1 fixture + two real engines + a real SQLite database.

The fixture intentionally has no persistence/TLS/QoS2; it is not a production broker.
"""
import collections,json,os,pathlib,secrets,signal,socket,socketserver,sqlite3,struct,subprocess,tempfile,threading,time,urllib.request,urllib.error

ROOT=pathlib.Path(tempfile.mkdtemp(prefix='business-ack-',dir=os.environ.get('TEST_OUTPUT_DIR')))
if not os.environ.get('TEST_ENGINE_BINARY'):raise SystemExit('Set TEST_ENGINE_BINARY')
ENGINE=pathlib.Path(os.environ['TEST_ENGINE_BINARY']).resolve()
PLUGINS=pathlib.Path(os.environ.get('TEST_PLUGIN_DIR',str(pathlib.Path(__file__).resolve().parents[1]/'dist/plugins'))).resolve()
SECRET=secrets.token_hex(32)
TOPIC='test/business/'+secrets.token_hex(6)

def exact(stream,n):
 data=b''
 while len(data)<n:
  part=stream.recv(n-len(data))
  if not part:raise EOFError()
  data+=part
 return data
def packet(stream):
 header=exact(stream,1)[0];size=0;factor=1
 for _ in range(4):
  value=exact(stream,1)[0];size+=(value&127)*factor
  if value&128==0:break
  factor*=128
 else:raise ValueError('invalid MQTT length')
 if size>2097152:raise ValueError('oversized MQTT fixture packet')
 return header,exact(stream,size)
def encode(header,body):
 size=len(body);length=b''
 while True:
  value=size%128;size//=128
  length+=bytes([value|(128 if size else 0)])
  if not size:break
 return bytes([header])+length+body
def utf(text):raw=text.encode();return struct.pack('>H',len(raw))+raw
def match(pattern,topic):
 p=pattern.split('/');t=topic.split('/')
 for i,v in enumerate(p):
  if v=='#':return i==len(p)-1
  if i>=len(t) or v not in ('+',t[i]):return False
 return len(p)==len(t)
class Broker(socketserver.ThreadingTCPServer):
 allow_reuse_address=True;daemon_threads=True
 def __init__(self):
  super().__init__(('127.0.0.1',0),Client);self.mu=threading.Lock();self.clients=set();self.counts=collections.Counter();self.last={};self.drop=set();self.delivery_counts=collections.Counter()
 def publish(self,topic,payload,qos=1):
  with self.mu:
   self.counts[topic]+=1;self.last[topic]=payload
   try:
    message=json.loads(payload);delivery=message.get('delivery',{});ident=delivery.get('id')
    if ident and topic.endswith('/telemetry'):self.delivery_counts[ident]+=1
   except (ValueError,AttributeError,TypeError):pass
   targets=[c for c in self.clients if any(match(p,topic) for p in c.topics)]
   drop=topic in self.drop
  if drop:return
  for target in targets:
   try:target.deliver(topic,payload,qos)
   except OSError:pass
 def count(self,topic):
  with self.mu:return self.counts[topic]
 def delivery_count(self,ident):
  with self.mu:return self.delivery_counts[ident]
class Client(socketserver.BaseRequestHandler):
 def setup(self):self.mu=threading.Lock();self.topics={};self.nextid=0;self.request.settimeout(30)
 def send(self,header,body):
  with self.mu:self.request.sendall(encode(header,body))
 def deliver(self,topic,payload,qos):
  qos=min(qos,max(self.topics.values(),default=0));body=utf(topic)
  with self.mu:
   if qos:self.nextid=self.nextid%65535+1;body+=struct.pack('>H',self.nextid)
   self.request.sendall(encode(0x30|(qos<<1),body+payload))
 def handle(self):
  try:
   while True:
    header,body=packet(self.request);kind=header>>4
    if kind==1:
     if not body.startswith(b'\x00\x04MQTT\x04'):raise ValueError('fixture supports MQTT3.1.1 only')
     with self.server.mu:self.server.clients.add(self)
     self.send(0x20,b'\x00\x00')
    elif kind==8:
     ident=body[:2];offset=2;codes=[]
     while offset<len(body):
      length=struct.unpack('>H',body[offset:offset+2])[0];offset+=2;topic=body[offset:offset+length].decode();offset+=length;qos=body[offset];offset+=1
      if qos>1:raise ValueError('fixture supports QoS0/1 only')
      with self.server.mu:self.topics[topic]=qos
      codes.append(qos)
     self.send(0x90,ident+bytes(codes))
    elif kind==10:
     ident=body[:2];offset=2
     while offset<len(body):
      length=struct.unpack('>H',body[offset:offset+2])[0];offset+=2;topic=body[offset:offset+length].decode();offset+=length
      with self.server.mu:self.topics.pop(topic,None)
     self.send(0xb0,ident)
    elif kind==3:
     length=struct.unpack('>H',body[:2])[0];topic=body[2:2+length].decode();offset=2+length;qos=(header>>1)&3;ident=body[offset:offset+2] if qos else b'';offset+=2 if qos else 0
     if qos>1:raise ValueError('QoS2 unsupported')
     self.server.publish(topic,body[offset:],qos)
     if qos:self.send(0x40,ident)
    elif kind==4:pass
    elif kind==12:self.send(0xd0,b'')
    elif kind==14:return
    else:raise ValueError('unsupported fixture command')
  except (EOFError,OSError,ValueError,IndexError,struct.error):pass
  finally:
   with self.server.mu:self.server.clients.discard(self)

class Engine:
 def __init__(self,name):
  self.name=name;self.root=ROOT/name;self.root.mkdir();self.token=None;self.password='Fixture!Aa9'+secrets.token_hex(12)
  sock=socket.socket();sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1];sock.close();self.log=open(self.root/'process.log','wb')
  env=dict(os.environ,IOT_RULE_ADMIN_PASSWORD=self.password,IOT_RULE_DATA_DIR=str(self.root/'data'),IOT_RULE_LISTEN=f'127.0.0.1:{self.port}',IOT_RULE_PLUGINS_DIR=str(PLUGINS),IOT_RULE_INSTANCE_ID=name,IOT_RULE_DELIVERY_MODE='reliable')
  self.env=env
  self.proc=subprocess.Popen([str(ENGINE)],cwd=self.root,env=env,stdout=self.log,stderr=self.log,start_new_session=True)
 def api(self,method,path,data=None):
  headers={'Content-Type':'application/json'}
  if self.token:headers['Authorization']='Bearer '+self.token
  request=urllib.request.Request(f'http://127.0.0.1:{self.port}/api/v1'+path,None if data is None else json.dumps(data).encode(),headers,method=method)
  try:
   with urllib.request.urlopen(request,timeout=30) as response:return json.loads(response.read() or '{}')
  except urllib.error.HTTPError as err:raise RuntimeError(f'{self.name} {path}: {err.code} '+err.read().decode()[:1000]) from None
 def ready(self):
  wait(lambda:self.api('GET','/health').get('status')=='ok');self.token=self.api('POST','/auth/login',{'username':'admin','password':self.password})['token']
 def deploy(self,ident,nodes):self.api('POST','/rules',{'id':ident,'name':ident,'nodes':nodes});self.api('POST',f'/rules/{ident}/deploy',{})
 def crash_restart(self):
  os.killpg(self.proc.pid,signal.SIGKILL);self.proc.wait(15)
  self.token=None
  self.proc=subprocess.Popen([str(ENGINE)],cwd=self.root,env=self.env,stdout=self.log,stderr=self.log,start_new_session=True)
  self.ready()
 def stop(self):
  if self.proc.poll() is None:
   os.killpg(self.proc.pid,signal.SIGTERM)
   try:self.proc.wait(35)
   except subprocess.TimeoutExpired:os.killpg(self.proc.pid,signal.SIGKILL);self.proc.wait()
  self.log.close()
engines=[]
def wait(check,timeout=35):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  try:
   if check():return
  except (ConnectionError,urllib.error.URLError):pass
  if any(e.proc.poll() is not None for e in engines):raise RuntimeError('engine exited; inspect private process.log')
  time.sleep(.1)
 raise RuntimeError('condition timed out; inspect '+str(ROOT))
def node(ident,plugin,kind,config,wires=()):return {'id':ident,'name':ident,'plugin':plugin,'type':kind,'config':config,'wires':list(wires)}
def mqtt_source(topic):return {'connector_ref':'mqtt','process':{'subscribe':{'topics':[topic],'qos':1},'decode':{'format':'json'}},'polling':{'wait_timeout_ms':500}}
def mqtt_sink(topic):return {'connector_ref':'mqtt','target':{'topic':topic,'qos':1,'retain':False}}
broker=Broker();threading.Thread(target=broker.serve_forever,daemon=True).start()
try:
 edge=Engine('edge');engines.append(edge);edge.ready();server=Engine('server');engines.append(server);server.ready()
 for engine in engines:engine.api('POST','/connectors',{'id':'mqtt','plugin_id':'connect-mqtt','enabled':True,'connection':{'broker':f'tcp://127.0.0.1:{broker.server_address[1]}','client_id':engine.name+'-'+secrets.token_hex(4),'connect_timeout_sec':2}})
 database_dir=ROOT/'sql';database_dir.mkdir()
 with sqlite3.connect(database_dir/'events.db') as database:database.execute('CREATE TABLE telemetry(delivery_id TEXT PRIMARY KEY,device_code TEXT,value REAL)')
 server.api('POST','/connectors',{'id':'sql','plugin_id':'connect-sqlite','enabled':True,'connection':{'directory':str(database_dir),'file':'events.db','allow_write':True}})
 identity={'origin_instance':'edge','key_id':'v1','secret':SECRET}
 edge.deploy('edge-rule',[
  node('collect','source-schedule','source',{'interval_ms':86400000,'on_start':True,'payload':{'device_code':'synthetic-machine','data':{'value':42.0}}},['edge-buffer']),
  node('edge-buffer','buffer-node','processor',{'ack_node_id':'business-ack','retry_ms':2000,'rate_per_second':10},['envelope']),
  node('envelope','delivery-envelope','processor',identity,['uplink']),node('uplink','sink-mqtt','sink',mqtt_sink(TOPIC+'/telemetry')),
  node('ack-source','source-mqtt','source',mqtt_source(TOPIC+'/receipts'),['business-ack']),
  node('business-ack','sink-business-ack','sink',dict(identity,committer='sql-server',buffer_node_id='edge-buffer'))])
 wait(lambda:broker.count(TOPIC+'/telemetry')>=2)
 # A broker PUBACK and an unauthenticated forged receipt must not release the edge record.
 with broker.mu:original=json.loads(broker.last[TOPIC+'/telemetry'])
 forged={'delivery':original['delivery'],'committer':'sql-server','committed_at':int(time.time()*1000),'status':'committed','signature':'0'*64}
 before=broker.count(TOPIC+'/telemetry');broker.publish(TOPIC+'/receipts',json.dumps(forged).encode());wait(lambda:broker.count(TOPIC+'/telemetry')>before)
 with broker.mu:broker.drop.add(TOPIC+'/receipts')
 server.deploy('server-rule',[
  node('intake','source-mqtt','source',mqtt_source(TOPIC+'/telemetry'),['verify']),
  node('verify','verify-delivery','processor',identity,['server-buffer']),
  node('server-buffer','buffer-node','processor',{'ack_node_id':'receipt-uplink','retry_ms':2000,'rate_per_second':10},['commit']),
  node('commit','transaction-receipt','processor',dict(identity,committer='sql-server',connector_ref='sql',statement='INSERT INTO telemetry(delivery_id,device_code,value) VALUES(@id,@device,@value) ON CONFLICT(delivery_id) DO NOTHING',params={'id':'delivery.id','device':'payload.device_code','value':'payload.data.value'}),['receipt-uplink']),
  node('receipt-uplink','sink-mqtt','sink',mqtt_sink(TOPIC+'/receipts'))])
 def rows():
  with sqlite3.connect(database_dir/'events.db',timeout=2) as database:return database.execute('SELECT count(*) FROM telemetry').fetchone()[0]
 wait(lambda:rows()==1);before=broker.count(TOPIC+'/telemetry');wait(lambda:broker.count(TOPIC+'/telemetry')>=before+2)
 if rows()!=1:raise RuntimeError('replay duplicated business rows')
 # Kill each entire Engine/plugin process group with receipts still dropped.
 # Reuse the same SQLite/NATS/buffer files, rule IDs and stable instance identity.
 before_receipts=broker.count(TOPIC+'/receipts');server.crash_restart()
 wait(lambda:broker.count(TOPIC+'/receipts')>before_receipts)
 if rows()!=1:raise RuntimeError('server restart duplicated committed telemetry')
 original_id=original['delivery']['id'];before_original=broker.delivery_count(original_id)
 edge.crash_restart();wait(lambda:broker.delivery_count(original_id)>before_original)
 # The on-start schedule may legitimately emit a new event after restart. The
 # original persisted delivery must still have exactly one committed row.
 def original_rows():
  with sqlite3.connect(database_dir/'events.db',timeout=2) as database:return database.execute('SELECT count(*) FROM telemetry WHERE delivery_id=?',(original_id,)).fetchone()[0]
 if original_rows()!=1:raise RuntimeError('original delivery lost/duplicated after engine SIGKILL')
 with broker.mu:broker.drop.discard(TOPIC+'/receipts')
 before=broker.count(TOPIC+'/receipts');wait(lambda:broker.count(TOPIC+'/receipts')>before)
 # Allow one already-scheduled send to finish, then verify the persisted record stops replaying.
 time.sleep(.8);stable=broker.count(TOPIC+'/telemetry');time.sleep(4.5)
 if broker.count(TOPIC+'/telemetry')!=stable:raise RuntimeError('valid scoped receipt did not release original edge buffer')
 report={'status':'passed','checks':['two real SDK v1 engine processes with private embedded NATS/SQLite','one edge persisted record replays after MQTT broker receipt','forged receipt cannot release edge record','server transaction commits into real SQLite before receipt','lost application receipts cause telemetry replay without duplicate SQL rows','server SIGKILL/restart preserves committed deduplication','edge SIGKILL/restart replays original durable delivery identity','valid signed scoped receipt releases edge buffer and replay stops'],'limits':['loopback MQTT v3/QoS1 fixture, not production broker certification','disk-full is tested separately at buffer boundary; combined full-chain disk exhaustion and 72h acceptance remain separate'],'run_directory':str(ROOT)}
 (ROOT/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
finally:
 for engine in reversed(engines):engine.stop()
 with broker.mu:clients=list(broker.clients)
 for client in clients:
  try:client.request.shutdown(socket.SHUT_RDWR);client.request.close()
  except OSError:pass
 broker.shutdown();broker.server_close()
