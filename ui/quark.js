const quarkCapacities = new Map();
let quarkTargetKeys = '', quarkBusy = false;
function paintQuarkStatus(s){
  const q=s.quark||{},running=!!q.running;
  $('quark-login-status').textContent=(q.stage||'尚未开始登录')+(q.error?'\n'+q.error:'');
  $('quark-login-badge').textContent=running?'等待授权':(s.accounts||[]).some(a=>a.provider==='quark')?'已有账号':'未登录';
  $('quark-qr-image').classList.toggle('hidden',!q.image);$('quark-qr-placeholder').classList.toggle('hidden',!!q.image);
  if(q.image&&$('quark-qr-image').src!==q.image)$('quark-qr-image').src=q.image;
  $('quark-qr-expiry').textContent=running&&q.expires?'剩余约 '+Math.max(0,q.expires-Math.floor(Date.now()/1000))+' 秒':'';
  const link=$('quark-authorize-link');link.classList.toggle('hidden',!q.authorize_url);if(q.authorize_url)link.href=q.authorize_url;else link.removeAttribute('href');
  const accounts=(s.accounts||[]).filter(a=>a.provider==='quark'),keys=accounts.map(a=>a.key).join('|');
  if(keys!==quarkTargetKeys){quarkTargetKeys=keys;const select=$('quark-login-target'),old=select.value;select.replaceChildren(new Option('自动识别同一账号 / 添加新账号',''));accounts.forEach(a=>select.add(new Option(a.name,a.key)));if(accounts.some(a=>a.key===old))select.value=old;}
  quarkControls();
}
function quarkControls(){
  const running=!!(state.webdav&&state.webdav.running),login=!!(state.quark&&state.quark.running),locked=running||quarkBusy||busy;
  for(const id of ['quark-login-start','quark-login-method','quark-login-target','quark-cookie-import','quark-cookie'])$(id).disabled=locked||login;
  $('quark-login-cancel').disabled=!login||!!(state.quark&&state.quark.completed);
  const phone=$('quark-login-method').value==='phone';
  $('quark-login-start').textContent=phone?'打开手机号登录窗口':'生成登录二维码';
  $('quark-qr-placeholder').textContent=phone?'官网手机号登录\n手机号、短信验证码和官网验证在新窗口中完成。':'选择登录方式后生成二维码。请在对应手机 App 中扫码确认。';
  $('quark-service-hint').textContent=running?'WebDAV 正在运行，停止服务后可添加或更新夸克授权。':'夸克账号可独立使用。授权后在 WebDAV 服务页勾选挂载，再启动服务。';
}
async function quarkAction(action){
  quarkBusy=true;quarkControls();try{
    const body={method:$('quark-login-method').value,account:$('quark-login-target').value};
    if(action==='import')body.cookie=$('quark-cookie').value;
    const data=await api('quark/'+action,body);if(action==='import')$('quark-cookie').value='';paintStatus(data);
    notice(action==='start'?(body.method==='phone'?'正在打开夸克官网手机号登录窗口。':'夸克登录已开始，请使用所选 App 扫码并确认。'):action==='cancel'?(data.quark&&data.quark.completed?'登录已完成，专用窗口已关闭。':'已取消夸克登录。'):'夸克授权已验证保存。','success');
  }catch(e){notice(e.message)}finally{quarkBusy=false;quarkControls()}
}
function quarkBytes(value){if(value===0)return '0 B';const units=['B','KiB','MiB','GiB','TiB','PiB'];let n=Number(value),i=0;while(n>=1024&&i<units.length-1){n/=1024;i++}return n.toFixed(i?2:0)+' '+units[i]}
function decorateQuarkAccount(host,account){
  const tools=document.createElement('div'),quota=document.createElement('button'),remove=document.createElement('button'),capacity=document.createElement('div');
  tools.className='quark-account-actions';quota.className='text-button small';quota.textContent='查询容量';remove.className='text-button small danger';remove.textContent='移除本机账号';remove.disabled=busy||!!(state.webdav&&state.webdav.running);
  capacity.className='quark-capacity';const show=()=>{capacity.textContent=quarkCapacities.get(account.key)||''};show();
  quota.onclick=async()=>{quota.disabled=true;try{const q=await api('quark/quota',{account:account.key});quarkCapacities.set(account.key,'总空间 '+quarkBytes(q.total)+' · 已用 '+quarkBytes(q.used)+' · 剩余 '+quarkBytes(q.available));show()}catch(e){quarkCapacities.set(account.key,'容量未知：'+e.message);show()}finally{quota.disabled=false}};
  remove.onclick=async()=>{try{const data=await api('quark/remove',{account:account.key});quarkCapacities.delete(account.key);paintStatus(data);notice('已移除本机保存的夸克授权，云端文件未操作。','success')}catch(e){notice(e.message)}};
  tools.append(quota,remove);host.append(tools,capacity);
}
window.addEventListener('DOMContentLoaded',()=>{
  $('quark-login-method').onchange=quarkControls;
  $('quark-login-start').onclick=()=>quarkAction('start');$('quark-login-cancel').onclick=()=>quarkAction('cancel');$('quark-cookie-import').onclick=()=>quarkAction('import');
});
