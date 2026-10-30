const $=id=>document.getElementById(id);const storageKey='cloudprobe-ui-'+location.port;let nonce=document.querySelector('meta[name="cloudprobe-ui-session"]').content||location.hash.slice(1)||sessionStorage.getItem(storageKey)||'';if(nonce)sessionStorage.setItem(storageKey,nonce);if(location.hash)history.replaceState(null,'',location.pathname);
let state={},qrLoaded=false,qrTried=false,qrLoading=false,polling=false,busy=false,stopped=false,lastPoll=0,report=null,selected=null,view='all',historyDirs=[],currentDir='',currentAccount='';
const titles = {
  login:['账号与登录','集中管理独立夸克账号、爆米花登录与备用授权。','连接与授权'],
  webdav:['WebDAV 服务','连接第三方客户端，统一管理云盘挂载和访问权限。','服务与挂载'],
  transfers:['传输设置','按用途配置调度、云端连接、缓存与总体低速恢复。','调度与资源'],
  files:['文件浏览与验证','读取真实目录，查看文件条目并验证指定文件内容。','目录与内容'],
  diagnostics:['日志中心','查看本次运行的接口来源、请求过程、重试与回退。','运行与诊断']
};
function tab(name) {
  if (!titles[name]) return;
  window.scrollTo(0,0);
  document.querySelectorAll('[data-tab]').forEach(button => {
    const active = button.dataset.tab === name;
    button.classList.toggle('active',active);
    if (active) button.setAttribute('aria-current','page'); else button.removeAttribute('aria-current');
  });
  Object.keys(titles).forEach(key => $('tab-' + key).classList.toggle('hidden',key !== name));
  $('page-title').textContent = titles[name][0]; $('breadcrumb-page').textContent = titles[name][0];
  $('page-subtitle').textContent = titles[name][1]; $('page-eyebrow').textContent = titles[name][2];
  try { sessionStorage.setItem('cloudprobe-page-' + location.port,name); } catch { /* Navigation still works without browser storage. */ }
}
function logTab(name) {
  if (!['run','dav','events'].includes(name)) return;
  for (const key of ['run','dav','events']) {
    const active = key === name, button = $('log-tab-' + key);
    button.classList.toggle('active',active); button.setAttribute('aria-selected',String(active));
    button.tabIndex = active ? 0 : -1; $('log-panel-' + key).classList.toggle('hidden',!active);
  }
}
document.querySelectorAll('[data-tab]').forEach(button => button.onclick = () => tab(button.dataset.tab));
document.querySelectorAll('[data-go], [data-log]:not([data-log-tab])').forEach(button => button.onclick = event => {
  event.preventDefault(); if (button.dataset.go) tab(button.dataset.go);
  if (button.dataset.log) logTab(button.dataset.log);
  if (button.dataset.logFilter) { $('run-result').value = button.dataset.logFilter; renderRunLog(); }
});
document.querySelectorAll('[data-log-tab]').forEach(button => {
  button.onclick = () => logTab(button.dataset.logTab);
  button.onkeydown = event => {
    const keys = ['run','dav','events'], index = keys.indexOf(button.dataset.logTab);
    let target;
    if (event.key === 'ArrowRight') target = keys[(index + 1) % keys.length];
    if (event.key === 'ArrowLeft') target = keys[(index + keys.length - 1) % keys.length];
    if (event.key === 'Home') target = keys[0]; if (event.key === 'End') target = keys[keys.length - 1];
    if (target) { event.preventDefault(); logTab(target); $('log-tab-' + target).focus(); }
  };
});
$('go-files').onclick = () => tab('files');
try { tab(sessionStorage.getItem('cloudprobe-page-' + location.port) || 'login'); } catch { tab('login'); }
function paintConsoleStatus(s) {
	const pending = Number(s.credential_save_pending || 0), warning = $('credential-save-warning');
	warning.classList.toggle('hidden',pending === 0);
	warning.textContent = pending ? '有 ' + pending + ' 个账号的新授权尚未保存。' + (s.credential_save_error || '正在重试保存。') : '';
  const d = s.webdav || {}, running = !!d.running;
  $('account-dot').classList.toggle('ready',!!(s.has_accounts || s.logged_in));
  $('global-login').textContent = s.has_accounts || s.logged_in ? '云盘账号可用' : '账号未登录';
  $('dav-dot').classList.toggle('ready',running);
  $('global-dav').textContent = running ? 'WebDAV 运行中' : 'WebDAV 未启动';
  const host = $('account-summary'); host.replaceChildren();
  for (const account of s.accounts || []) {
    const row = document.createElement('div'), icon = document.createElement('span'), info = document.createElement('div');
    const name = document.createElement('strong'), provider = document.createElement('p'), badge = document.createElement('span');
    row.className = 'account-item'; icon.className = 'account-avatar'; icon.textContent = account.provider === 'quark' ? '夸' : account.provider === 'wopan' ? '联' : '移';
    info.className = 'account-info'; name.textContent = account.name;
    provider.textContent = account.provider === 'quark' ? '夸克网盘 · 独立账号' : account.provider === 'wopan' ? '联通云盘 · 网易绑定' : '移动云盘 · 网易绑定';
    badge.className = 'tag' + (account.official_web_available ? ' yes' : '');
    badge.textContent = account.provider === 'quark' ? (account.channels || []).join(' · ') : account.provider === 'wopan' ? account.official_web_available ? '官网备用授权已保存' : '可添加官网备用授权' : '已读取绑定凭据';
    info.append(name,provider,badge); if(account.provider === 'quark') decorateQuarkAccount(info,account); row.append(icon,info); host.append(row);
  }
  if (!(s.accounts || []).length) {
    const empty = document.createElement('div'); empty.className = 'empty';
    empty.textContent = s.logged_in ? '尚未发现已绑定的云盘，可刷新账号列表。' : '扫码登录后，已绑定的移动、联通云盘会显示在这里。'; host.append(empty);
  }
  $('login-badge').classList.toggle('yes',!!s.logged_in);
}
function notice(text,kind='error'){const n=$('notice');n.textContent=text;n.className='banner '+kind;n.classList.toggle('hidden',!text)}
let engineFailureSignature = '', engineDiagnosticText = '';
function paintEngineFailure(s) {
 const diagnostic = s.engine_diagnostic || (s.engine_error ? {title:'网易引擎不可用',reason:s.engine_error,steps:['关闭程序后重新启动；如果仍失败，复制诊断信息以进一步定位。']} : null);
 const host = $('engine-failure');
 host.classList.toggle('hidden',!diagnostic);
 if (!diagnostic) { engineFailureSignature='';engineDiagnosticText='';return; }
 $('engine-label').textContent='引擎不可用';
 $('footer-state').textContent=diagnostic.reason;
 $('login-state').textContent=diagnostic.title || '网易引擎不可用';
 $('login-qr').classList.remove('hidden');
 $('login-done').classList.add('hidden');
 $('login-badge').textContent='引擎不可用';
 $('qr-image').classList.add('hidden');
 $('qr-placeholder').classList.remove('hidden');
 $('qr-placeholder').textContent=diagnostic.reason;
 $('qr-expiry').textContent='请先按页面顶部的处理办法排除故障，再重新启动程序。';
 const signature=JSON.stringify(diagnostic);
 if(signature===engineFailureSignature)return;
 engineFailureSignature=signature;
 $('engine-failure-title').textContent=diagnostic.title || '网易引擎不可用';
 $('engine-failure-reason').textContent=diagnostic.reason || s.engine_error;
 const steps=$('engine-failure-steps');steps.replaceChildren();
 for(const step of diagnostic.steps || []) { const item=document.createElement('li');item.textContent=step;steps.append(item); }
 const paths=diagnostic.paths || {}, lines=[];
 if(diagnostic.detail)lines.push(diagnostic.detail);
 for(const [key,label] of [['executable','当前程序'],['sdk','SDK 目录'],['bridge','桥接组件'],['data','资料目录']])if(paths[key])lines.push(label+'：'+paths[key]);
 $('engine-failure-detail').textContent=lines.join('\n\n') || '底层没有提供更多错误信息，原因尚未确认。';
 $('engine-copy-status').textContent='';
 engineDiagnosticText=['CloudProbe '+(s.version || ''),diagnostic.title,diagnostic.reason,'错误类型：'+(diagnostic.code || 'unknown'),'处理办法：',...(diagnostic.steps || []),...lines].filter(Boolean).join('\n');
}
$('engine-copy-diagnostic').onclick=async()=>{
 try {await navigator.clipboard.writeText(engineDiagnosticText);$('engine-copy-status').textContent='诊断信息已复制';}
 catch { $('engine-failure-details').open=true;$('engine-copy-status').textContent='复制失败，请在展开的详情中手动选择复制。'; }
};
async function api(path,data,method='POST'){const r=await fetch('/api/'+path,{method,headers:{'X-CloudProbe-UI':nonce,...(method==='POST'?{'Content-Type':'application/json'}:{})},...(method==='POST'?{body:JSON.stringify(data||{})}:{})});let d;try{d=await r.json()}catch{throw new Error(await r.text().catch(()=> '本地服务连接异常'))}if(!r.ok){const e=new Error(d.error||'请求失败');e.data=d;throw e}return d}
function selectedAccountReady(){const a=(state.accounts||[]).find(a=>a.key===$('account').value);return !!a&&(a.provider==='quark'||state.engine_ready&&state.logged_in)}
function controls(){const ready=selectedAccountReady();$('query-files').disabled=!ready||busy;$('refresh-accounts').disabled=(!state.logged_in&&!state.has_accounts)||busy;$('account').disabled=!(state.accounts||[]).length||busy;$('logout').disabled=!state.logged_in||!state.engine_ready||busy;$('refresh-qr').disabled=!state.engine_ready||state.logged_in||qrLoading;$('poll-login').disabled=!qrLoaded||state.logged_in||polling;$('fetch-file').disabled=!ready||busy||!selected||selected.directory;$('export-results').disabled=!state.has_result||busy;$('back-dir').disabled=!historyDirs.length||busy;davControls();quarkControls()}
function paintStatus(s){state=s;$('engine-dot').classList.toggle('ready',!!s.engine_ready);$('engine-label').textContent=s.engine_ready?'原版引擎就绪':'引擎启动中';$('footer-state').textContent=s.stage||'';$('version-label').textContent=s.version||'—';$('login-state').textContent=s.stage||'';$('login-badge').textContent=s.logged_in?'已登录':'未登录';$('login-qr').classList.toggle('hidden',!!s.logged_in);$('login-done').classList.toggle('hidden',!s.logged_in);$('logged-user').textContent=s.user||'';$('event-log').textContent=(s.events||[]).join('\n')||'等待原版引擎初始化…';paintDAV(s.webdav||{});paintWebLogin();paintTransfers(s.webdav||{});paintConsoleStatus(s);paintQuarkStatus(s);pollRunLog(s.run_log||{});
 const a=$('account'),old=a.value,next=s.accounts||[];if(a.options.length!==next.length||Array.from(a.options).some((o,i)=>!next[i]||next[i].key!==o.value)){a.replaceChildren();if(!next.length){a.add(new Option(s.logged_in?'未发现已绑定的移动/联通云盘':'登录后选择',''))}else{next.forEach(x=>a.add(new Option(x.name,x.key)));if(next.some(x=>x.key===old))a.value=old;accountChanged()}}
 paintEngineFailure(s);
 controls();if(s.engine_ready&&!s.logged_in&&!qrTried&&!qrLoading)refreshQR();
}
async function status(){if(stopped)return;try{paintStatus(await api('status',null,'GET'))}catch(e){$('footer-state').textContent='本地服务已断开；重新启动工具后刷新页面。'}}
async function refreshQR(){qrTried=true;qrLoading=true;notice('');$('qr-placeholder').classList.remove('hidden');$('qr-placeholder').textContent='正在获取真实登录二维码…';$('qr-image').classList.add('hidden');controls();try{const d=await api('qr');$('qr-image').src=d.image;$('qr-image').classList.remove('hidden');$('qr-placeholder').classList.add('hidden');qrLoaded=true;await status()}catch(e){notice(e.message);$('qr-placeholder').textContent='二维码获取失败，可点击刷新重试。'}finally{qrLoading=false;controls()}}
$('refresh-qr').onclick=refreshQR;
async function poll(force=false){if(stopped||!state.engine_ready||polling||busy||!qrLoaded||state.logged_in)return;if(!force&&Date.now()-lastPoll<(state.poll_after_seconds||2)*1000)return;lastPoll=Date.now();polling=true;try{paintStatus(await api('poll'))}catch(e){notice(e.message)}finally{polling=false;controls()}}
$('poll-login').onclick=()=>poll(true);
$('logout').onclick=async()=>{try{paintStatus(await api('logout'));qrLoaded=false;qrTried=false;report=null;selected=null;renderRows();notice('已退出本次登录。','success');await status()}catch(e){notice(e.message)}};
$('refresh-accounts').onclick=async()=>{busy=true;controls();try{paintStatus(await api('accounts'));notice('绑定列表已刷新。','success')}catch(e){notice(e.message)}finally{busy=false;controls()}};
function accountChanged(){currentAccount=$('account').value;const a=(state.accounts||[]).find(x=>x.key===currentAccount);$('directory').value=a?a.root:'root';historyDirs=[];currentDir='';selected=null;report=null;renderRows();$('query-status').textContent=a&&a.provider==='wopan'?'联通根目录 ID 为 0；列表将保留压缩包、视频等所有返回条目。':'请选择目录后读取；列目录不会下载文件正文。';controls()}
$('account').onchange=accountChanged;
async function loadFiles(fromBack=false){const dir=$('directory').value.trim();if(!dir){notice('请输入目录 ID。');return}busy=true;selected=null;controls();notice('');$('query-status').textContent='正在通过所选云盘驱动分页读取目录，请稍候…';$('query-status').classList.add('busy');try{const d=await api('list',{account:$('account').value,directory:dir,expected:$('expected').value.trim()});report=d.report;if(currentDir&&currentDir!==dir&&!fromBack)historyDirs.push(currentDir);currentDir=dir;showReport();await status()}catch(e){if(e.data&&e.data.report){report=e.data.report;showReport();await status()}else{$('query-status').textContent='目录请求失败，不能将其解释为空目录。'}notice(e.message)}finally{busy=false;$('query-status').classList.remove('busy');controls()}}
$('query-files').onclick=()=>loadFiles();$('directory').onkeydown=e=>{if(e.key==='Enter')loadFiles()};$('back-dir').onclick=()=>{if(historyDirs.length){$('directory').value=historyDirs.pop();loadFiles(true)}};
function bytes(n){if(!n)return '—';if(n<1024)return n+' B';if(n<1048576)return(n/1024).toFixed(1)+' KiB';return(n/1048576).toFixed(2)+' MiB'}
function showReport(){if(report.directory){$('directory').value=report.directory;currentDir=report.directory}renderRows();let text=report.items.length?(report.query_completed?'目录已按分页规则读取完成。':'查询未完成。'):'本次目录请求未返回文件条目。';text+=' '+report.stop_reason+'；读取 '+report.pages+' 页。';for(const e of report.expected_files||[])text+='\n'+e.name+'：'+e.conclusion;$('query-status').textContent=text}
document.querySelectorAll('[data-view]').forEach(b=>b.onclick=()=>{view=b.dataset.view;document.querySelectorAll('[data-view]').forEach(x=>x.classList.toggle('on',x===b));renderRows()});
function renderRows(){const body=$('file-rows');body.replaceChildren();const all=report?report.items:[];const items=all.filter(x=>view==='all'||view==='subtitle'&&x.baomihua_subtitle_candidate||view==='ignored'&&!x.directory&&!x.baomihua_subtitle_candidate);$('file-count').textContent=report?'返回 '+all.length+' 条 · 显示 '+items.length+' 条':'尚未查询';
 if(!items.length){const row=body.insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='empty';cell.textContent=!report?'还没有查询目录。真实返回将显示在这里。':all.length?'当前筛选视图没有条目。':'本次请求未返回文件条目，请核对目录 ID；不代表整个网盘为空。';return}
 items.forEach(x=>{const row=body.insertRow();row.className='file-row'+(selected===x?' selected':'');row.insertCell().textContent=x.directory?'▸':'○';const name=row.insertCell(),n=document.createElement('div'),id=document.createElement('div');n.className='file-name';n.textContent=x.name;id.className='file-id';id.textContent=x.id+(x.fid?' / '+x.fid:'');name.append(n,id);row.insertCell().textContent=x.directory?'文件夹':x.kind||x.suffix||'无扩展名';row.insertCell().textContent=x.directory?'—':bytes(x.size);const tag=document.createElement('span');tag.className='tag'+(x.baomihua_subtitle_candidate?' yes':'');tag.textContent=x.directory?'—':x.baomihua_subtitle_candidate?'是':'否';row.insertCell().append(tag);row.insertCell().textContent=x.filter_reason;row.onclick=()=>{selected=x;$('save-name').value=x.directory?'':x.name;renderRows();controls()};row.ondblclick=()=>{if(x.directory){$('directory').value=x.id;loadFiles()}}})}
async function getDownload(path,name){const r=await fetch('/api/'+path,{headers:{'X-CloudProbe-UI':nonce}});if(!r.ok){let d=await r.json().catch(()=>({error:'下载响应失败'}));throw new Error(d.error)}const blob=await r.blob(),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),30000)}
$('fetch-file').onclick=async()=>{if(!selected)return;busy=true;controls();notice('');$('download-status').textContent='正在下载指定文件…';try{const d=await api('fetch',{account:$('account').value,id:selected.id,fid:selected.fid||'',name:$('save-name').value||selected.name});await getDownload('download/'+d.download_key,d.receipt.file);$('download-status').textContent='完成 · '+d.receipt.bytes+' 字节';notice('已取得指定文件内容。SHA-256：'+d.receipt.sha256,'success');await status()}catch(e){$('download-status').textContent='下载失败';notice(e.message)}finally{busy=false;controls()}};
$('export-results').onclick=()=>getDownload('export','cloudprobe-diagnostics.zip').catch(e=>notice(e.message));
$('quit').onclick=async()=>{try{await api('quit');stopped=true;qrLoaded=false;notice('测试工具已关闭。可以关闭本页。','success');state.engine_ready=false;state.logged_in=false;$('engine-label').textContent='已关闭';$('engine-dot').classList.remove('ready');$('footer-state').textContent='后台已停止';controls()}catch(e){notice(e.message)}};
let davFormReady=false,davAccountKeys='',davRows=[];
let davWriteWarningSignature='';
function paintDAVWriteWarnings(d) {
 const warnings=d.running?(d.mounts||[]).filter(m=>m.write_warning):[];
 $('dav-write-warning').classList.toggle('hidden',!warnings.length);
 $('dav-write-warning-title').textContent=warnings.some(m=>m.write_warning.kind==='denied')?'WebDAV 已启动，部分写入被拒绝':'WebDAV 已启动，写入能力尚未确认';
 const signature=JSON.stringify(warnings.map(m=>[m.alias,m.write_warning]));
 if(signature===davWriteWarningSignature)return;
 davWriteWarningSignature=signature;
 const host=$('dav-write-warning-items');host.replaceChildren();
 for(const m of warnings){const w=m.write_warning,row=document.createElement('div'),title=document.createElement('strong');title.textContent=m.alias+' · '+w.path;row.append(title);for(const text of [w.message,'原因：'+w.reason,'处理办法：'+w.advice]){const p=document.createElement('p');p.textContent=text;row.append(p)}host.append(row)}
}
function paintDAV(d){state.webdav=d;paintDAVWriteWarnings(d);if(!davFormReady){$('dav-listen').value=d.listen||'127.0.0.1';$('dav-port').value=d.port||50146;$('dav-user').value=d.username||'cloudprobe';$('dav-max').value=d.max_upload_mib||10240;$('dav-concurrency').value=d.upload_concurrency||4;$('dav-mode').value=d.upload_mode||'auto';davFormReady=true}
 $('dav-state').textContent=d.running?'运行中':'未启动';$('dav-state').style.color=d.running?'#21865a':'';$('dav-active').textContent=d.active||0;$('dav-count').textContent=(d.mounts||[]).length;
 const accounts=state.accounts||[],keys=accounts.map(a=>a.key).join('|');if(keys!==davAccountKeys){davAccountKeys=keys;davRows=[];const body=$('dav-mounts');body.replaceChildren();const counts={};accounts.forEach(a=>{const saved=(d.saved_mounts||[]).find(m=>m.account===a.key);counts[a.provider]=(counts[a.provider]||0)+1;const row=body.insertRow(),enabled=document.createElement('input'),alias=document.createElement('input'),root=document.createElement('input'),permission=document.createElement('select'),storage=document.createElement('select');enabled.type='checkbox';enabled.checked=!(d.saved_mounts||[]).length||!!saved;enabled.setAttribute('aria-label','启用 '+a.name);alias.value=saved?saved.alias:(a.provider==='quark'?'夸克网盘-':a.provider==='wopan'?'联通云盘-':'移动云盘-')+counts[a.provider];alias.setAttribute('aria-label','挂载文件夹名');root.value=saved?saved.root:a.root;root.setAttribute('aria-label','挂载根目录 ID');permission.add(new Option('读写','write'));permission.add(new Option('只读','read'));if(saved)permission.value=saved.writable?'write':'read';permission.setAttribute('aria-label','挂载权限');storage.add(new Option('跟随全局',''));storage.add(new Option('正文纯内存 · 保存续传状态','memory'));storage.add(new Option('内存优先','auto'));storage.add(new Option('磁盘兼容','disk'));storage.value=saved?(saved.upload_storage_mode||''):(a.provider==='quark'?'auto':'');storage.setAttribute('aria-label','挂载上传缓存 · '+a.name);row.insertCell().append(enabled);row.insertCell().textContent=a.name;row.insertCell().append(alias);row.insertCell().append(root);row.insertCell().append(permission);row.insertCell().append(storage);davRows.push({account:a.key,enabled,alias,root,permission,storage})});if(!accounts.length){const cell=body.insertRow().insertCell();cell.colSpan=6;cell.className='empty';cell.textContent='登录后显示可用的云盘账号。'}}
 const addresses=$('dav-addresses');addresses.replaceChildren();if(d.running){(d.urls||[]).forEach(u=>{const line=document.createElement('div'),text=document.createElement('span'),copy=document.createElement('button');text.textContent=u+' ';copy.textContent='复制地址';copy.className='small';copy.onclick=()=>copyDAV(u);line.append(text,copy);addresses.append(line)});const p=document.createElement('div');p.textContent='用户名：'+d.username+' · 密码在基础设置中管理。客户端填写 /dav/ 可看到挂载文件夹。';addresses.append(p)}else{addresses.textContent='服务已停止。填写设置并启动后，将显示连接地址。'}
 $('dav-log').textContent=(d.activity||[]).slice(-60).reverse().map(x=>x.time+'  '+x.method+'  '+x.status+'  '+x.path+'  '+x.duration_ms+' ms  #'+(x.request_id||'')+(x.method==='PUT'?'\n  客户端接收 '+logBytes(x.received_bytes)+' · 云端发送 '+logBytes(x.sent_bytes)+' · 已确认 '+logBytes(x.confirmed_bytes):'')+(x.error?'\n  '+x.error:'')).join('\n')||'尚无 WebDAV 请求。';if(d.mounts&&d.mounts.length)$('dav-capabilities').textContent=d.mounts.map(m=>m.alias+'：'+m.write_status).join('；')+'。跨挂载复制/移动暂不支持。';davControls()}
function davControls(){const running=!!(state.webdav&&state.webdav.running);$('dav-start').disabled=busy||running||!!(state.wopan_web_login&&state.wopan_web_login.running)||!!(state.quark&&state.quark.running)||!state.has_accounts;$('dav-gen-pass').disabled=busy||running;$('dav-stop').disabled=busy||!running;for(const id of ['dav-listen','dav-port','dav-user','dav-pass','dav-max','dav-concurrency','dav-mode'])$(id).disabled=busy||running;for(const row of davRows)for(const key of ['enabled','alias','root','permission','storage'])row[key].disabled=busy||running;
 const reason=running?'服务运行中，停止后可修改设置。':busy?'正在处理操作，请稍候。':state.wopan_web_login&&state.wopan_web_login.running?'正在进行官网备用登录，完成或取消后可启动服务。':state.quark&&state.quark.running?'请先完成或取消夸克登录。':!state.has_accounts?'请先在“账号与登录”添加可用云盘。':'设置就绪后启动服务，账号与传输配置会一并保存。';
 $('dav-control-reason').textContent=reason;$('dav-start').title=$('dav-start').disabled?reason:'';
 $('dav-settings-note').textContent=running?'服务运行中，基础设置与挂载只读；停止后可修改。':'设置随下次启动服务保存并生效。';
 $('dav-readonly-tag').classList.toggle('hidden',!running);transferControls()}
function paintWebLogin(){const select=$('wopan-web-account'),accounts=(state.accounts||[]).filter(a=>a.provider==='wopan'),old=select.value;const keys=accounts.map(a=>a.key).join('|');if(select.dataset.keys!==keys){select.dataset.keys=keys;select.replaceChildren();accounts.forEach(a=>select.add(new Option(a.name,a.key)));if(accounts.some(a=>a.key===old))select.value=old;if(!accounts.length)select.add(new Option('尚无联通绑定账号',''))}const a=accounts.find(a=>a.key===select.value),s=state.wopan_web_login||{},running=!!(state.webdav&&state.webdav.running);$('wopan-web-badge').textContent=a&&a.official_web_available?'已保存官网备用授权':'尚未添加';$('wopan-web-badge').classList.toggle('yes',!!(a&&a.official_web_available));$('web-login-service-link').classList.toggle('hidden',!running);$('wopan-web-status').textContent=running?'WebDAV 正在运行。请前往“WebDAV 服务”停止服务，再更新官网备用授权。':s.running?s.stage+(s.error?'\n'+s.error:''):a&&a.official_web_available?'备用登录已保存在本机。启动 WebDAV 后，联通直连接口会使用这份授权。':s.error||s.stage||'点击后会打开本工具专用的联通官网窗口，完成登录后自动验证同一账号。';$('wopan-web-open').disabled=!a||busy||running||s.running;$('wopan-web-stop').disabled=!s.running;$('wopan-web-clear').disabled=!a||!a.official_web_available||busy||running;$('wopan-web-import').disabled=!a||busy||running;$('wopan-web-account').disabled=running||s.running}
$('wopan-web-account').onchange=paintWebLogin;
async function webLoginAction(action){try{const data={account:$('wopan-web-account').value};if(action==='import')data.cookie=$('wopan-web-cookie').value;await api('webdav/wopan-login/'+action,data);if(action==='import')$('wopan-web-cookie').value='';await status();notice(action==='start'?'请在打开的官网窗口完成登录。':action==='clear'?'已移除官网备用登录。':action==='import'?'官网备用登录已验证保存。':'已取消登录。','success')}catch(e){notice(e.message)}}
$('wopan-web-open').onclick=()=>webLoginAction('start');$('wopan-web-stop').onclick=()=>webLoginAction('stop');$('wopan-web-clear').onclick=()=>webLoginAction('clear');$('wopan-web-import').onclick=()=>webLoginAction('import');
async function copyDAV(text){try{await navigator.clipboard.writeText(text);notice('已复制。','success')}catch{notice('复制失败，请手动选择并复制。')}}
$('dav-gen-pass').onclick=()=>{const values=crypto.getRandomValues(new Uint8Array(16));$('dav-pass').value=Array.from(values,b=>b.toString(16).padStart(2,'0')).join('');$('dav-pass-hint').textContent='已生成新密码，点击“启动 WebDAV”后生效；请复制到第三方软件。'};
$('dav-show').onclick=()=>{const input=$('dav-pass');input.type=input.type==='password'?'text':'password';$('dav-show').textContent=input.type==='password'?'显示密码':'隐藏密码'};
$('dav-copy-pass').onclick=()=>{$('dav-pass').value?copyDAV($('dav-pass').value):notice('当前密码未在页面保留。如已设置，可停止服务后填写新密码。')};
$('dav-start').onclick=async()=>{busy=true;controls();notice('正在验证挂载和写接口，首次启动可能需要数秒。','');try{const d=await api('webdav/start',{listen:$('dav-listen').value,port:Number($('dav-port').value),username:$('dav-user').value,password:$('dav-pass').value,max_upload_mib:Number($('dav-max').value),upload_concurrency:Number($('dav-concurrency').value),upload_mode:$('dav-mode').value,...transferPayload(),mounts:davRows.filter(r=>r.enabled.checked).map(r=>({account:r.account,alias:r.alias.value.trim(),root:r.root.value.trim(),writable:r.permission.value==='write',upload_storage_mode:r.storage.value,openlist:openListPayload(r.account)}))});if(d.generated_password){$('dav-pass').value=d.generated_password;$('dav-pass-hint').textContent='已自动生成密码，请复制到第三方软件。此密码只在本页保留，配置中保存校验值。'}paintDAV(d);notice('WebDAV 已启动，可以使用连接地址访问。','success');await status()}catch(e){notice(e.message)}finally{busy=false;controls()}};
$('dav-stop').onclick=async()=>{busy=true;controls();try{paintDAV(await api('webdav/stop'));notice('WebDAV 已停止。','success');await status()}catch(e){notice(e.message)}finally{busy=false;controls()}};
setInterval(status,4000);setInterval(poll,2200);setInterval(()=>{if(qrLoaded&&!state.logged_in&&!state.engine_error){const remain=Math.max(0,(state.qr_expires||0)-Math.floor(Date.now()/1000));$('qr-expiry').textContent=remain?'二维码剩余约 '+remain+' 秒；请在手机上确认登录。':'二维码可能已到期，请刷新。'}},1000);status();
