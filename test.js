'use strict';

const { spawn } = require('child_process');
const net = require('net');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const root = __dirname;
const checks = [];
function report(name, ok, details='') {
  checks.push({ name, ok });
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${details ? ` — ${details}` : ''}`);
}
function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once('error', reject);
    s.listen(0, '127.0.0.1', () => { const p=s.address().port; s.close(e=>e?reject(e):resolve(p)); });
  });
}
function startServer({ cap=3, maxi=8 }={}) {
  return freePort().then(port => new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(root,'server.js')], {
      cwd:root, env:{...process.env, PORT:String(port), CAP:String(cap), MAXI:String(maxi)}, stdio:['ignore','pipe','pipe'],
    });
    let output=''; let done=false;
    const timer=setTimeout(()=>{if(!done){done=true;child.kill('SIGTERM');reject(new Error(`server startup timeout: ${output}`));}},5000);
    child.stdout.on('data',data=>{output+=data.toString();if(output.includes(`listening on ${port}`)&&!done){done=true;clearTimeout(timer);resolve({child,port});}});
    child.stderr.on('data',data=>{output+=data.toString();});
    child.once('exit',(code)=>{if(!done){done=true;clearTimeout(timer);reject(new Error(`server exited ${code}: ${output}`));}});
  }));
}
function stopServer(child) {
  return new Promise(resolve=>{
    if(!child||child.exitCode!==null){resolve();return;}
    const timer=setTimeout(()=>{child.kill('SIGKILL');resolve();},1500);
    child.once('exit',()=>{clearTimeout(timer);resolve();});
    child.kill('SIGTERM');
  });
}
class Client {
  constructor(ws) {
    this.ws=ws; this.messages=[]; this.waiters=[]; this.closed=null;
    ws.on('message',(data,isBinary)=>{if(isBinary)return;let m;try{m=JSON.parse(data.toString());}catch{return;}this.messages.push(m);this.flush();});
    ws.on('close',(code,reason)=>{this.closed={code,reason:reason.toString()};this.flush();});
    ws.on('error',()=>{});
  }
  flush(){
    for(const waiter of [...this.waiters]){
      const idx=this.messages.findIndex(waiter.match);
      if(idx>=0){const [msg]=this.messages.splice(idx,1);this.waiters.splice(this.waiters.indexOf(waiter),1);clearTimeout(waiter.timer);waiter.resolve(msg);}
      else if(this.closed&&waiter.close){this.waiters.splice(this.waiters.indexOf(waiter),1);clearTimeout(waiter.timer);waiter.resolve(this.closed);}
    }
  }
  wait(match,ms=1600){
    const idx=this.messages.findIndex(match);if(idx>=0)return Promise.resolve(this.messages.splice(idx,1)[0]);
    return new Promise((resolve,reject)=>{const w={match,resolve,reject,close:false,timer:null};w.timer=setTimeout(()=>{this.waiters.splice(this.waiters.indexOf(w),1);reject(new Error('message timeout'));},ms);this.waiters.push(w);});
  }
  waitClose(ms=1600){if(this.closed)return Promise.resolve(this.closed);return new Promise((resolve,reject)=>{const w={match:()=>false,resolve,reject,close:true,timer:null};w.timer=setTimeout(()=>{this.waiters.splice(this.waiters.indexOf(w),1);reject(new Error('close timeout'));},ms);this.waiters.push(w);});}
  send(x){this.ws.send(typeof x==='string'?x:JSON.stringify(x));}
  async noMessage(match,ms=250){await new Promise(r=>setTimeout(r,ms));return !this.messages.some(match);}
  close(){try{this.ws.close();}catch{}}
}
function connect(port, query='') {
  return new Promise((resolve,reject)=>{
    const ws=new WebSocket(`ws://127.0.0.1:${port}/ws${query}`);
    const timer=setTimeout(()=>{ws.terminate();reject(new Error('connect timeout'));},1600);
    ws.once('open',()=>{clearTimeout(timer);resolve(new Client(ws));});
    ws.once('error',e=>{clearTimeout(timer);reject(e);});
  });
}
function get(port,pathname) {
  return new Promise((resolve,reject)=>http.get({host:'127.0.0.1',port,path:pathname},res=>{const chunks=[];res.on('data',c=>chunks.push(c));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks)}));}).on('error',reject));
}
async function main(){
  let primary=null, secondary=null; const clients=[];
  try{
    primary=await startServer({cap:3,maxi:8});
    const A=await connect(primary.port,'?room=lotus&inst=0'); clients.push(A);
    const aWelcome=await A.wait(m=>m.t==='welcome');
    const B=await connect(primary.port,'?room=lotus&inst=0'); clients.push(B);
    const bWelcome=await B.wait(m=>m.t==='welcome');
    await A.wait(m=>m.t==='join'&&m.peer.id===bWelcome.self.id);
    report('A and B join lotus instance 0; B sees A; A receives B join',bWelcome.room==='lotus'&&bWelcome.inst===0&&bWelcome.peers.some(v=>v.id===aWelcome.self.id));
    report('generated visitor names are unique and match two words plus two digits',aWelcome.self.name!==bWelcome.self.name&&/^(Quiet|Calm|Gentle|Warm|Still|Soft|Slow|Clear|Bright|Deep) (Wind|Wave|Pine|Bird|River|Cloud|Moon|Stone|Leaf|Rain) \d{2}$/.test(aWelcome.self.name)&&/^(Quiet|Calm|Gentle|Warm|Still|Soft|Slow|Clear|Bright|Deep) (Wind|Wave|Pine|Bird|River|Cloud|Moon|Stone|Leaf|Rain) \d{2}$/.test(bWelcome.self.name));

    A.send({t:'act',a:'greet'}); const greet=await B.wait(m=>m.t==='act'&&m.a==='greet');
    report('greet action relays to the same instance',greet.id===aWelcome.self.id);
    A.send({t:'act',a:'release',text:'private text must not leave the browser'}); const release=await B.wait(m=>m.t==='act'&&m.a==='release');
    report('release relays without any text field',release.id===aWelcome.self.id&&!Object.prototype.hasOwnProperty.call(release,'text'));
    A.send({t:'act',a:'incense',to:bWelcome.self.id,extra:'ignored'}); const incense=await B.wait(m=>m.t==='act'&&m.a==='incense');
    await B.wait(m=>m.t==='merit'&&m.merit===1);
    report('incense keeps a valid same-instance target and increments merit',incense.to===bWelcome.self.id);
    A.send({t:'act',a:'shout'}); A.send({t:'chat',text:'not allowed'});
    const unsupportedGone=await B.noMessage(m=>m.t==='act'&&m.a==='shout'||m.t==='chat');
    report('unsupported shout and chat frames are not relayed',unsupportedGone);
    A.send({t:'move',x:5,y:.6}); const moved=await B.wait(m=>m.t==='move'&&m.id===aWelcome.self.id);
    report('move coordinates are clamped to the 0–1 range',moved.x===1&&moved.y===.6);
    A.send({t:'bead'}); const beadMerit=await B.wait(m=>m.t==='merit'&&m.merit===2);
    report('merit is 1 after incense and advances to 2 after a bead',beadMerit.merit===2);

    const C=await connect(primary.port,'?room=lotus&inst=0');clients.push(C);const cWelcome=await C.wait(m=>m.t==='welcome');
    const D=await connect(primary.port,'?room=lotus&inst=1');clients.push(D);const dWelcome=await D.wait(m=>m.t==='welcome');
    const E=await connect(primary.port,'?room=lotus&inst=1');clients.push(E);const eWelcome=await E.wait(m=>m.t==='welcome');
    report('C joins instance 0; D starts empty instance 1; E joins D',cWelcome.inst===0&&dWelcome.inst===1&&dWelcome.peers.length===0&&eWelcome.inst===1&&eWelcome.peers.some(v=>v.id===dWelcome.self.id));
    const rooms=await get(primary.port,'/api/rooms');const lotus=JSON.parse(rooms.body).find(r=>r.id==='lotus');
    report('/api/rooms lists instance counts in sorted order and has cache header',rooms.status===200&&rooms.headers['cache-control']==='public, max-age=5'&&lotus.instances.length===2&&lotus.instances[0].inst===0&&lotus.instances[0].count===3&&lotus.instances[1].inst===1&&lotus.instances[1].count===2);

    B.close();await A.wait(m=>m.t==='leave'&&m.id===bWelcome.self.id);
    C.close();await new Promise(r=>setTimeout(r,80));
    const F=await connect(primary.port,'?room=lotus');clients.push(F);const fWelcome=await F.wait(m=>m.t==='welcome');
    report('after B and C leave, F chooses the fullest open instance 1',fWelcome.inst===1);
    D.close();E.close();await new Promise(r=>setTimeout(r,100));
    const G=await connect(primary.port,'?room=lotus');clients.push(G);const gWelcome=await G.wait(m=>m.t==='welcome');
    report('when instances tie at one visitor each, G chooses lower index 0',gWelcome.inst===0);
    const defaultRoom=await connect(primary.port);clients.push(defaultRoom);const defaultWelcome=await defaultRoom.wait(m=>m.t==='welcome');
    report('missing room parameter defaults to lotus',defaultWelcome.room==='lotus');
    const unknown=await connect(primary.port,'?room=nope');clients.push(unknown);const unknownClose=await unknown.waitClose();
    report('unknown room closes with 4004',unknownClose.code===4004);

    const root=await get(primary.port,'/');const og=await get(primary.port,'/og.png');const missing=await get(primary.port,'/missing');
    report('GET / returns the static HTML page',root.status===200&&/text\/html/.test(root.headers['content-type']));
    report('GET /og.png returns a PNG with the PNG signature',og.status===200&&og.headers['content-type']==='image/png'&&og.body.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])));
    report('unknown HTTP paths return 404',missing.status===404);

    secondary=await startServer({cap:1,maxi:1});
    const fullA=await connect(secondary.port,'?room=lotus&inst=0');clients.push(fullA);await fullA.wait(m=>m.t==='welcome');
    const fullB=await connect(secondary.port,'?room=lotus&inst=0');clients.push(fullB);const fullMsg=await fullB.wait(m=>m.t==='full');const fullClose=await fullB.waitClose();
    report('a one-slot server sends full and closes the next client with 4009',fullMsg.t==='full'&&fullClose.code===4009);
  }catch(error){
    console.error('FAIL test run error:',error.stack||error);
    checks.push({name:'test run completed',ok:false});
  }finally{
    for(const c of clients)c.close();
    if(primary)await stopServer(primary.child);if(secondary)await stopServer(secondary.child);
  }
  const failed=checks.filter(c=>!c.ok);console.log(`\n${checks.length-failed.length}/${checks.length} checks passed`);
  if(failed.length)process.exitCode=1;else console.log('ALL TESTS PASS');
}
main();
