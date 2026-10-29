let runRows = [], runAfter = 0, runID = '', runLoading = false, runGap = false;
function logBytes(n) { return n ? bytes(n) : '0 B'; }

async function pollRunLog(meta) {
  const diskState = meta.error ? '；写入失败：' + meta.error : meta.disk_paused ? '；磁盘记录已暂停，正在重试日志清理。' : '；页面保留最近 800 条，导出包含本次运行仍保留的日志。';
  const retentionState = meta.retention_error ? '；清理提示：' + meta.retention_error : '';
  const droppedState = meta.dropped_entries ? '；本次有 ' + meta.dropped_entries + ' 条记录未写入磁盘。' : '';
  const exportState = meta.export_error ? '；导出提示：' + meta.export_error : '';
  $('run-file').textContent = meta.file ? '本机日志：' + meta.file + diskState + retentionState + droppedState + exportState : '';
  if (!meta.run_id || runLoading) return;
  if (runID !== meta.run_id) { runID = meta.run_id; runAfter = 0; runRows = []; runGap = false; }
  if (runAfter >= meta.last_id) return;
  runLoading = true;
  try {
    const d = await api('run-log?after=' + runAfter, null, 'GET');
    if (d.run_id !== runID) return;
    runGap = runGap || d.gap;
    runRows.push(...d.entries);
    runRows = runRows.slice(-800);
    if (d.entries.length) runAfter = d.entries[d.entries.length - 1].id;
    renderRunLog();
  } catch (e) { $('run-count').textContent = '读取日志失败：' + e.message; }
  finally { runLoading = false; }
}

function renderRunLog() {
  const host = $('run-log');
  const opened = new Set(Array.from(host.querySelectorAll('details[open]'), n => n.dataset.id));
  const search = $('run-search').value.trim().toLowerCase();
  const source = $('run-source').value, filter = $('run-result').value;
  const rows = runRows.filter(e =>
    (!search || JSON.stringify(e).toLowerCase().includes(search)) &&
    (!source || e.source.includes(source)) &&
    (!filter || filter === 'error' && (e.error || e.result === '回退' || Number(e.result) >= 400) ||
      filter === 'upload' && /上传|分片|暂存|接收|提交|等待写入锁/.test(e.operation) ||
      filter === 'download' && /下载|读取正文|读取分段|分段读取/.test(e.operation)));
  host.replaceChildren();
  $('run-count').textContent = '显示 ' + rows.length + ' / ' + runRows.length + ' 条 · 最新在前' + (runGap ? ' · 较早记录请导出查看' : '');
  if (!rows.length) {
    const p = document.createElement('div'); p.className = 'empty'; p.textContent = '暂无符合条件的操作记录。'; host.append(p); return;
  }
  for (const e of rows.slice().reverse()) {
    const row = document.createElement('details'), summary = document.createElement('summary'), detail = document.createElement('div');
    row.dataset.id = String(e.id); row.open = opened.has(String(e.id));
    row.className = 'run-row' + (e.error || Number(e.result) >= 400 ? ' bad' : '');
    const timing = e.duration_ms === undefined ? '' : ' · ' + e.duration_ms + ' ms';
    summary.textContent = new Date(e.time).toLocaleTimeString('zh-CN', { hour12: false }) +
      ' [' + e.operation + '][' + e.source + '] ' + (e.path || '') + ' → ' + e.result +
      (e.operation === '读取目录' && e.result === '成功' ? ' · 返回 ' + (e.count || 0) + ' 条' : '') +
      (e.part ? ' · 分片 ' + e.part + '/' + e.parts : '') +
      (e.bytes_per_second ? ' · ' + logBytes(e.bytes_per_second) + '/s' : '') + timing;
    detail.className = 'run-detail';
    const lines = [];
    if (e.request_id) lines.push('请求 #' + e.request_id + ' · ' + (e.method || '') + ' · ' + (e.mount || ''));
    if (e.endpoint) lines.push('接口 / 节点：' + e.endpoint);
    if (e.protocol) lines.push('实际协议：' + e.protocol);
    if (e.worker) lines.push('分片工作任务：' + e.worker);
    if (e.connection_id) lines.push('连接编号：' + e.connection_id + (e.connection_reused ? '（复用）' : ''));
    if (e.local_address) lines.push('本机端点：' + e.local_address);
    if (e.remote_address) lines.push('远端端点：' + e.remote_address);
    if (e.http_status) lines.push('上游 HTTP 状态：' + e.http_status);
    if (e.detail) lines.push(e.detail);
    for (const [key, label] of [['bytes', '本阶段正文'], ['received_bytes', '客户端接收'], ['sent_bytes', '交给 HTTP 传输的正文'], ['confirmed_bytes', '云端已确认正文'], ['response_bytes', '发送给客户端的响应体']]) {
      if (e[key] !== undefined) lines.push(label + '：' + logBytes(e[key]));
    }
    for (const [key, label] of [['send_ms', '连接及正文发送耗时'], ['disk_write_ms', '本地磁盘 Write 累计耗时'], ['confirm_wait_ms', '等待上游响应'], ['queue_ms', '分片总并发排队']]) {
      if (e[key] !== undefined) lines.push(label + '：' + e[key] + ' ms');
    }
    if (e.attempt) lines.push('本分片第 ' + e.attempt + ' 次请求');
    if (e.error) lines.push('错误：' + e.error);
    detail.textContent = lines.join('\n'); row.append(summary, detail);
    if (e.items && e.items.length) {
      const list = document.createElement('pre');
      list.textContent = e.items.map(it => (it.directory ? '目录' : '文件') + '  ' + it.name + '  ' + (it.directory ? '' : logBytes(it.size)) + (it.kind ? '  ' + it.kind : '')).join('\n');
      row.append(list);
    }
    host.append(row);
  }
}
window.addEventListener('DOMContentLoaded', () => {
  $('run-search').oninput = renderRunLog;
  $('run-source').onchange = renderRunLog;
  $('run-result').onchange = renderRunLog;
  $('run-clear-filters').onclick = () => { $('run-search').value = ''; $('run-source').value = ''; $('run-result').value = ''; renderRunLog(); };
  $('export-run-log').onclick = () => getDownload('run-log/export', 'cloudprobe-run-log.jsonl').catch(e => notice(e.message));
});
