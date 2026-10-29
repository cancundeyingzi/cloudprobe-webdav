let transfersReady = false, backupKeys = null, backupRows = new Map();
// Each field belongs to one task-focused section; IDs and payload names remain
// stable so an edited form survives polling and navigation without duplication.
const transferFields = [
  ['concurrent_uploads','同时上传文件上限',3,1,5,1,'upload-advanced','server'],
  ['concurrent_downloads','同时下载文件上限',4,1,5,1,'download-fields','server'],
  ['download_concurrency','云端单文件下载分片并发',8,1,16,1,'download-fields','server'],
  ['upload_part_mib','云端上传分片大小（MiB）',5,5,16,1,'upload-advanced'],
  ['download_part_mib','云端下载分片大小（MiB）',5,4,32,1,'download-fields','server'],
  ['download_memory_mib','下载预取正文内存上限（MiB）',128,32,1024,1,'download-fields','server'],
  ['total_connections','云端正文请求总并发预算',64,1,160,1,'upload-advanced'],
  ['upload_low_speed_mb','总体低速阈值 X（MB/s）',1,0.01,10000,0.01,'recovery-fields','recovery'],
  ['upload_low_seconds','持续低速检测窗口（秒）',5,1,120,1,'recovery-fields','recovery'],
  ['upload_recovery_tries','连续无效次数 Y',3,1,100,1,'recovery-fields','recovery'],
  ['upload_memory_mib','单文件正文缓存上限（MiB）',32,5,1024,1,'memory-fields'],
  ['total_upload_memory_mib','全部上传正文内存上限（MiB）',128,5,4096,1,'memory-fields'],
  ['upload_prefetch_mib','提前接收目标（MiB）',10,1,1024,1,'memory-fields']
];
const transferModes = [
  ['upload_storage_mode','上传缓存方式','memory',[['memory','正文纯内存 · 保存续传状态（联通 / 夸克）'],['auto','内存优先 · 准备阶段不兼容时使用磁盘'],['disk','磁盘兼容 · 支持云端重启续传']],'memory-fields'],
  ['upload_recovery_mode','总体低速恢复','auto',[['auto','启用 · 所有上传统一暂停'],['off','关闭 · 保留故障重试']],'recovery-fields']
];
function paintTransfers(d) {
  if (!transfersReady) {
    const settings = d.transfer_settings || {};
    $('transfer-scheduling').value = settings.transfer_scheduling || 'server';
    for (const [key,label,value,choices,group] of transferModes) {
      const box = document.createElement('div'), title = document.createElement('label'), select = document.createElement('select');
      box.className = 'field'; title.htmlFor = 'transfer-' + key; title.textContent = label; select.id = title.htmlFor;
      for (const [id,text] of choices) select.add(new Option(text,id));
      select.value = settings[key] || value; box.append(title,select); $('transfer-' + group).append(box);
    }
    for (const [key,label,value,min,max,step,group,policy] of transferFields) {
      const box = document.createElement('div'), title = document.createElement('label'), input = document.createElement('input');
      box.className = 'field' + (policy === 'server' ? ' server-only' : '');
      title.htmlFor = 'transfer-' + key; title.textContent = label; input.id = title.htmlFor;
      input.type = 'number'; input.min = min; input.max = max; input.step = step; input.value = settings[key] ?? value;
      box.append(title,input); $('transfer-' + group).append(box);
    }
    $('transfer-download-mode').value = settings.download_part_mode || 'auto';
    $('transfer-scheduling').onchange = transferControls;
    $('transfer-upload_recovery_mode').onchange = transferControls;
    transfersReady = true;
  }
  renderTransferQueues(d);
  const accounts = state.accounts || [], keys = accounts.map(a => a.key).join('|');
  if (backupKeys !== keys) {
    backupKeys = keys; backupRows = new Map(); const host = $('openlist-backups'); host.replaceChildren();
    for (const a of accounts) {
      const saved = (d.saved_mounts || []).find(m => m.account === a.key) || {};
      const box = document.createElement('details'), title = document.createElement('summary'), fields = document.createElement('div');
      title.textContent = a.name + ' · OpenList 备用' + (saved.openlist_enabled ? '（已保存）' : ''); fields.className = 'backup-fields';
      box.append(title,fields); const row = {};
      for (const [key,label,type,value] of [
        ['enabled','启用此账号的 OpenList 备用','checkbox',saved.openlist_enabled || false],
        ['address','OpenList 服务地址（HTTPS 或本机 HTTP）','url',saved.openlist_address || ''],
        ['root','对应的 OpenList 挂载目录','text',saved.openlist_root || '/'],
        ['token','OpenList API Token（留空保留原值）','password',''],
        ['meta_password','挂载目录访问密码（若有）','password',''],
        ['verify_write','首次核验：允许创建并清理专用验证文件','checkbox',false]
      ]) {
        const labelNode = document.createElement('label'), input = document.createElement('input');
        input.type = type; input.autocomplete = 'off'; input.setAttribute('aria-label',label + ' · ' + a.name);
        if (type === 'checkbox') input.checked = value; else input.value = value;
        labelNode.append(document.createTextNode(label),input); fields.append(labelNode); row[key] = input;
      }
      backupRows.set(a.key,row); host.append(box);
    }
    if (!accounts.length) { const empty = document.createElement('div'); empty.className = 'empty'; empty.textContent = '登录后可为每个云盘配置已有 OpenList 服务。'; host.append(empty); }
  }
  transferControls();
}
function transferControls() {
  const running = !!(state.webdav && state.webdav.running), disabled = busy || running;
  const client = $('transfer-scheduling').value === 'client';
  $('transfer-scheduling').disabled = disabled;
  for (const [key,,,,,,,policy] of transferFields) {
    const input = $('transfer-' + key);
    if (input) input.disabled = disabled || (policy === 'server' && client) || (policy === 'recovery' && $('transfer-upload_recovery_mode').value === 'off');
  }
  for (const [key] of transferModes) { const input = $('transfer-' + key); if (input) input.disabled = disabled; }
  $('transfer-download-mode').disabled = disabled || client;
  document.querySelectorAll('.server-only').forEach(node => node.classList.toggle('hidden',client));
  document.querySelectorAll('.client-only').forEach(node => node.classList.toggle('hidden',!client));
  $('dispatch-explanation').textContent = client ? '文件数量由客户端决定；GET / Range 按实际请求读取。云端上传仍按网盘协议分片，内存与请求预算继续生效。' : '使用已设文件队列和云端下载分片加速。客户端 Range 始终按实际范围读取，不再叠加下载分片。';
  $('download-policy-note').textContent = client ? '跟随客户端的 GET / Range 请求，不额外拆分单个下载。' : '普通 GET 可由服务端并行加速；不支持 Range 或无法核对文件版本时自动单路读取。';
  $('transfer-settings-note').textContent = running ? '服务运行中，设置只读。停止 WebDAV 后修改，再启动生效。' : '所有设置随下次启动 WebDAV 保存并生效。';
  $('transfer-readonly-tag').classList.toggle('hidden',!running);
  for (const row of backupRows.values()) for (const input of Object.values(row)) input.disabled = disabled;
}
function renderTransferQueues(d) {
  const queues = d.running ? d.transfer_queues || {} : {}, host = $('transfer-queue'); host.replaceChildren();
  const add = (label,value,detail) => {
    const box = document.createElement('div'), title = document.createElement('span'), number = document.createElement('strong'), note = document.createElement('small');
    box.className = 'transfer-stat'; title.textContent = label; number.textContent = value; note.textContent = detail;
    box.append(title,number,note); host.append(box);
  };
  for (const [key,label] of [['uploads','上传文件'],['downloads','下载文件'],['connections','云端正文请求']]) {
    const q = queues[key];
    add(label,q ? q.active + (q.limit ? ' / ' + q.limit : ' 个') : '未启动',q ? (q.limit ? '排队 ' + q.queued : '客户端调度 · 不设文件数量上限') : '启动 WebDAV 后显示');
  }
  const recovery = queues.upload_recovery || {}, memory = queues.upload_memory || {};
  const downloadMemory = queues.download_memory || {};
  add('下载预取缓存',d.running ? Math.round((downloadMemory.used_bytes || 0) / 1048576) + ' / ' + Math.round((downloadMemory.limit_bytes || 0) / 1048576) + ' MiB' : '未启动',d.running ? '等待内存 ' + (downloadMemory.queued || 0) + ' · 窗口消费或丢弃后释放' : '启动 WebDAV 后显示');
  const recoveryState = recovery.bandwidth_limited ? '按带宽受限处理，停止主动暂停' : recovery.paused ? '全部上传暂停中，1 秒后继续' : recovery.enabled ? '总体速度监测中' : '主动低速恢复未启用';
  add('总体上传速度',recovery.scope ? ((recovery.bytes_per_second || 0) / 1e6).toFixed(2) + ' MB/s' : '—','所有文件与线程的正文发送速度合计');
  add('上传正文缓存',memory.limit_bytes ? ((memory.used_bytes || 0) / 1048576).toFixed(1) + ' / ' + (memory.limit_bytes / 1048576).toFixed(0) + ' MiB' : '—','峰值 ' + ((memory.peak_bytes || 0) / 1048576).toFixed(1) + ' MiB · 缓冲池 ' + ((memory.allocated_bytes || 0) / 1048576).toFixed(1) + ' MiB');
  add('总体低速恢复',!d.running ? '未启动' : recovery.attempts ? '已暂停 ' + recovery.attempts + ' 次' : '尚未暂停',!d.running ? '启动 WebDAV 后监测' : recoveryState + ' · 连续无效 ' + (recovery.consecutive_failures || 0) + ' 次');
}
function transferPayload() {
  const out = {download_part_mode:$('transfer-download-mode').value,transfer_scheduling:$('transfer-scheduling').value};
  for (const [key,label,fallback,min,max,step,,policy] of transferFields) {
    const value = Number($('transfer-' + key).value);
    if (!Number.isFinite(value) || (step === 1 && !Number.isInteger(value)) || value < min || value > max) {
      if (policy === 'server' && out.transfer_scheduling === 'client') { out[key] = fallback; continue; }
      tab('transfers');
      const input = $('transfer-' + key), details = input.closest('details');
      if (details) details.open = true;
      input.focus();
      throw new Error(label + '需为 ' + min + '—' + max);
    }
    out[key] = value;
  }
  for (const [key] of transferModes) out[key] = $('transfer-' + key).value;
  return out;
}
function openListPayload(account) {
  const r = backupRows.get(account); if (!r) return null;
  return {enabled:r.enabled.checked,address:r.address.value.trim(),root:r.root.value.trim(),token:r.token.value,meta_password:r.meta_password.value,verify_write:r.verify_write.checked};
}

async function refreshUploadResume() {
  const host = $('resume-tasks');
  try {
    const data = await api('upload-resume', null, 'GET');
    host.replaceChildren();
    for (const task of data.tasks || []) {
      const row = document.createElement('div'), text = document.createElement('p');
      text.textContent = (task.target || task.id) + ' · ' + (task.mode === 'memory' ? '正文纯内存' : '磁盘兼容') + ' · ' + task.state + ' · 云端已确认 ' + bytes(task.confirmed || 0) + ' / ' + bytes(task.size || 0);
      row.append(text);
      for (const [action,label] of [[task.paused ? 'resume' : 'pause',task.paused ? '继续' : '暂停'],['reset','放弃续传并重传']]) {
        const button = document.createElement('button'); button.className = 'small'; button.textContent = label;
        button.onclick = async () => {
          if (action === 'reset' && !confirm('删除该任务的续传记录？下次将重新上传公网正文，已有云端临时文件不会自动删除。')) return;
          button.disabled = true;
          try { await api('upload-resume',{id:task.id,action}); await refreshUploadResume(); }
          catch (e) { notice(e.message); button.disabled = false; }
        };
        row.append(button);
      }
      host.append(row);
    }
    if (!host.childNodes.length) host.textContent = '没有待恢复的云端上传任务。';
  } catch (e) { host.textContent = '读取续传任务失败：' + e.message; }
}
$('resume-refresh').onclick = refreshUploadResume;
