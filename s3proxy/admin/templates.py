"""HTML template for admin dashboard."""

DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>S3Proxy Admin</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f1117;color:#f0f6fc;font-family:'SF Mono',Menlo,Consolas,monospace;font-size:13px;padding:20px;max-width:960px;margin:0 auto}

.header{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:16px}
.header-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.pod-name{font-size:16px;color:#f0f6fc;font-weight:700}
.header-meta{color:#8b949e;font-size:12px;margin-top:2px}
.header-meta span{margin-right:16px}
.kek{color:#7ee787}
.pods-bar{display:none;flex-wrap:wrap;gap:6px;margin-top:8px;padding-top:8px;border-top:1px solid #21262d}
.pod-badge{display:inline-flex;align-items:center;gap:6px;padding:3px 8px;background:#0f1117;border:1px solid #30363d;border-radius:4px;font-size:11px;color:#8b949e}
.pod-badge.current{border-color:#58a6ff;color:#f0f6fc}
.pod-badge .pb-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}

.section{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:16px}
.section-title{font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;font-weight:600;display:flex;justify-content:space-between;align-items:center}
.section-tag{font-size:10px;color:#484f58;text-transform:none;letter-spacing:0;font-weight:400}
.section.warning{border-left:3px solid #f85149}
.section.caution{border-left:3px solid #d29922}

.row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #21262d}
.row:last-child{border-bottom:none}
.label{color:#8b949e}
.value{color:#f0f6fc;font-weight:500}

.bar-container{width:100px;height:6px;background:#21262d;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:8px}
.bar-fill{height:100%;border-radius:3px;transition:width 0.5s ease}
.bar-green{background:#3fb950}
.bar-yellow{background:#d29922}
.bar-red{background:#f85149}

.errors-row{display:flex;gap:20px;padding:5px 0}
.errors-row .err-item{color:#8b949e}
.errors-row .err-val{font-weight:500;color:#f0f6fc}
.err-val.hot{color:#f85149}

.throughput-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.tp-item{text-align:center;padding:8px;background:#0f1117;border-radius:6px}
.tp-num{font-size:18px;color:#f0f6fc;font-weight:700;font-variant-numeric:tabular-nums}
.tp-unit{font-size:10px;color:#8b949e;margin-top:2px}
.tp-label{font-size:11px;color:#8b949e;margin-top:4px}
canvas.sparkline{display:block;width:100%;height:24px;margin-top:6px}

.feed-wrap{max-height:400px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#30363d #0f1117}
.feed-wrap::-webkit-scrollbar{width:6px}
.feed-wrap::-webkit-scrollbar-track{background:#0f1117}
.feed-wrap::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
.feed-table{width:100%;border-collapse:collapse}
.feed-table th{position:sticky;top:0;background:#161b22;z-index:1;text-align:left;color:#8b949e;font-weight:500;padding:4px 6px;border-bottom:1px solid #30363d;font-size:11px}
.feed-table td{padding:3px 6px;border-bottom:1px solid #1c2128;font-size:12px;white-space:nowrap;font-variant-numeric:tabular-nums}
.feed-table tr.new-row{animation:rowIn 0.5s ease}
@keyframes rowIn{from{background:rgba(63,185,80,0.1)}to{background:transparent}}
.method-get{color:#58a6ff}
.method-put{color:#3fb950}
.method-del{color:#f85149}
.method-other{color:#8b949e}
.status-2xx{color:#3fb950}
.status-4xx{color:#d29922}
.status-5xx{color:#f85149}
.latency-warn{color:#d29922}
.latency-bad{color:#f85149}
.path-cell{max-width:220px;overflow:hidden;text-overflow:ellipsis}
.crypto-badge{font-size:10px;padding:1px 4px;border-radius:3px;font-weight:600;letter-spacing:0.5px}
.crypto-enc{background:#1c3a2a;color:#3fb950}
.crypto-dec{background:#172030;color:#58a6ff}
.empty{color:#484f58;font-style:italic;padding:12px 0}
td.r{text-align:right}

#status-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#3fb950;margin-right:4px;transition:background 0.3s}
.refresh-badge{font-size:10px;color:#484f58;display:flex;align-items:center;gap:4px}
.spinner{width:8px;height:8px;border:1.5px solid #30363d;border-top-color:#58a6ff;border-radius:50%;display:inline-block}
.spinner.active{animation:spin 0.6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>

<div class="header">
<div class="header-top">
<div><span id="status-dot"></span><span class="pod-name" id="pod-name">loading...</span></div>
<div class="refresh-badge"><span class="spinner" id="spinner"></span> <span id="refresh-label">3s</span></div>
</div>
<div class="header-meta">
<span>uptime <strong id="uptime">-</strong></span>
<span>KEK <strong class="kek" id="kek-fp">-</strong></span>
<span id="storage-label">-</span>
</div>
<div class="pods-bar" id="pods-bar"></div>
</div>

<div class="section" id="health-section">
<div class="section-title">Health <span class="section-tag">this pod</span></div>
<div class="row"><span class="label">Memory</span><span class="value" id="memory">-</span></div>
<div class="row"><span class="label">In-Flight</span><span class="value" id="in-flight">-</span></div>
<div class="section-title" style="margin-top:10px;margin-bottom:6px">Errors <span class="section-tag">10 min rate</span></div>
<div class="errors-row">
<span class="err-item">4xx <span class="err-val" id="err-4xx">0</span>/min</span>
<span class="err-item">5xx <span class="err-val" id="err-5xx">0</span>/min</span>
<span class="err-item">503 <span class="err-val" id="err-503">0</span>/min</span>
</div>
</div>

<div class="section">
<div class="section-title">Throughput <span class="section-tag">this pod &middot; 10 min</span></div>
<div class="throughput-grid">
<div class="tp-item"><div class="tp-num" id="tp-req">0</div><div class="tp-unit">/min</div><div class="tp-label">requests</div><canvas class="sparkline" id="spark-req" height="24"></canvas></div>
<div class="tp-item"><div class="tp-num" id="tp-enc">0</div><div class="tp-unit">/min &middot; <span id="tp-enc-bytes">0 B</span>/min</div><div class="tp-label">encrypt</div><canvas class="sparkline" id="spark-enc" height="24"></canvas></div>
<div class="tp-item"><div class="tp-num" id="tp-dec">0</div><div class="tp-unit">/min &middot; <span id="tp-dec-bytes">0 B</span>/min</div><div class="tp-label">decrypt</div><canvas class="sparkline" id="spark-dec" height="24"></canvas></div>
</div>
</div>

<div class="section" id="feed-section">
<div class="section-title">Live Feed <span class="section-tag">last 50 &middot; this pod</span></div>
<div class="feed-wrap" id="feed-wrap">
<table class="feed-table">
<thead><tr><th>Time</th><th>Method</th><th>Path</th><th>Op</th><th style="text-align:right">Status</th><th style="text-align:right">Latency</th><th style="text-align:right">Size</th><th></th></tr></thead>
<tbody id="feed-body"></tbody>
</table>
<div class="empty" id="feed-empty">Waiting for requests...</div>
</div>
</div>

<script>
function barClass(p){return p>80?'bar-red':p>50?'bar-yellow':'bar-green'}
function bar(p){return '<div class="bar-container"><div class="bar-fill '+barClass(p)+'" style="width:'+Math.min(p,100)+'%"></div></div>'}

function drawSpark(id,data,color){
  var c=document.getElementById(id);
  if(!c)return;
  var dpr=window.devicePixelRatio||1;
  var w=c.offsetWidth,h=parseInt(c.getAttribute('height'))||24;
  c.width=w*dpr;c.height=h*dpr;
  var ctx=c.getContext('2d');
  ctx.scale(dpr,dpr);
  if(!data||data.length<2)return;
  var mx=0;
  for(var i=0;i<data.length;i++)if(data[i]>mx)mx=data[i];
  if(!mx)return;
  var bw=Math.max(1,Math.floor(w/data.length)-1);
  ctx.fillStyle=color||'#3fb950';
  for(var i=0;i<data.length;i++){
    var bh=Math.round((data[i]/mx)*(h-2));
    if(data[i]>0&&bh<1)bh=1;
    ctx.fillRect(i*(bw+1),h-bh,bw,bh);
  }
}

function updatePods(pods,currentPod){
  var bar=document.getElementById('pods-bar');
  if(!bar)return;
  if(!pods||pods.length<=1){bar.style.display='none';return;}
  bar.style.display='flex';
  var html='';
  pods.forEach(function(p){
    var pod=p.pod||{},h=p.health||{},t=(p.throughput||{}).rates||{},f=p.formatted||{};
    var isCurrent=pod.pod_name===currentPod;
    var memPct=h.memory_usage_pct||0;
    var dotColor=memPct>80?'#f85149':memPct>50?'#d29922':'#3fb950';
    if((t.errors_5xx_per_min||0)>0)dotColor='#f85149';
    html+='<div class="pod-badge'+(isCurrent?' current':'')+'">';
    html+='<span class="pb-dot" style="background:'+dotColor+'"></span>';
    html+=pod.pod_name+' '+(f.uptime||'-')+' '+memPct+'% '+(t.requests_per_min||0)+'/m';
    html+='</div>';
  });
  bar.innerHTML=html;
}

function formatTime(ts){
  var d=new Date(ts*1000);
  return d.toLocaleTimeString('en-GB',{hour12:false});
}
function formatSize(bytes){
  if(!bytes||bytes<=0)return'-';
  if(bytes<1024)return bytes+'B';
  if(bytes<1048576)return(bytes/1024).toFixed(1)+'KB';
  if(bytes<1073741824)return(bytes/1048576).toFixed(1)+'MB';
  return(bytes/1073741824).toFixed(1)+'GB';
}
function methodClass(m){
  if(m==='GET')return'method-get';
  if(m==='PUT')return'method-put';
  if(m==='DELETE')return'method-del';
  return'method-other';
}
function statusClass(s){
  if(s>=500)return'status-5xx';
  if(s>=400)return'status-4xx';
  return'status-2xx';
}
function latencyFmt(ms){
  if(ms<1)return'<1ms';
  if(ms<1000)return Math.round(ms)+'ms';
  return(ms/1000).toFixed(1)+'s';
}
function latencyClass(ms){
  if(ms>5000)return'latency-bad';
  if(ms>1000)return'latency-warn';
  return'';
}
function cryptoBadge(c){
  if(c==='encrypt')return'<span class="crypto-badge crypto-enc">ENC</span>';
  if(c==='decrypt')return'<span class="crypto-badge crypto-dec">DEC</span>';
  return'';
}

var _prevFirst=0;
function updateFeed(log){
  var body=document.getElementById('feed-body');
  var empty=document.getElementById('feed-empty');
  if(!log||log.length===0){body.innerHTML='';empty.style.display='block';return;}
  empty.style.display='none';
  var isNew=log[0].timestamp!==_prevFirst;
  _prevFirst=log[0].timestamp;
  var html='';
  for(var i=0;i<log.length;i++){
    var r=log[i];
    var cls=i===0&&isNew?' class="new-row"':'';
    html+='<tr'+cls+'>';
    html+='<td>'+formatTime(r.timestamp)+'</td>';
    html+='<td class="'+methodClass(r.method)+'">'+r.method+'</td>';
    html+='<td class="path-cell" title="'+r.path+'">'+r.path+'</td>';
    html+='<td style="color:#8b949e">'+r.operation+'</td>';
    html+='<td class="r '+statusClass(r.status)+'">'+r.status+'</td>';
    html+='<td class="r '+latencyClass(r.duration_ms)+'">'+latencyFmt(r.duration_ms)+'</td>';
    html+='<td class="r">'+formatSize(r.size)+'</td>';
    html+='<td>'+cryptoBadge(r.crypto)+'</td>';
    html+='</tr>';
  }
  body.innerHTML=html;
}

function update(d){
  var pod=d.pod||{},h=d.health||{},t=(d.throughput||{}).rates||{},f=d.formatted||{};
  document.getElementById('pod-name').textContent=pod.pod_name||'unknown';
  document.getElementById('uptime').textContent=f.uptime||'-';
  document.getElementById('kek-fp').textContent=pod.kek_fingerprint||'-';
  document.getElementById('storage-label').textContent=pod.storage_backend||'-';
  document.getElementById('memory').innerHTML=(f.memory_reserved||'0 B')+' / '+(f.memory_limit||'0 B')+' ('+(h.memory_usage_pct||0)+'%)'+bar(h.memory_usage_pct||0);
  document.getElementById('in-flight').textContent=h.requests_in_flight||0;

  var e4=document.getElementById('err-4xx'),e5=document.getElementById('err-5xx'),e503=document.getElementById('err-503');
  e4.textContent=t.errors_4xx_per_min||0;
  e5.textContent=t.errors_5xx_per_min||0;
  e503.textContent=t.errors_503_per_min||0;
  e5.className='err-val'+((t.errors_5xx_per_min||0)>0?' hot':'');
  e503.className='err-val'+((t.errors_503_per_min||0)>0?' hot':'');

  var hs=document.getElementById('health-section');
  hs.className='section'+((t.errors_5xx_per_min||0)>0?' warning':((t.errors_4xx_per_min||0)>0?' caution':''));

  document.getElementById('tp-req').textContent=t.requests_per_min||0;
  document.getElementById('tp-enc').textContent=t.encrypt_per_min||0;
  document.getElementById('tp-dec').textContent=t.decrypt_per_min||0;
  document.getElementById('tp-enc-bytes').textContent=f.bytes_encrypted_per_min||'0 B';
  document.getElementById('tp-dec-bytes').textContent=f.bytes_decrypted_per_min||'0 B';

  var hist=(d.throughput||{}).history||{};
  drawSpark('spark-req',hist.requests_per_min,'#58a6ff');
  drawSpark('spark-enc',hist.encrypt_per_min,'#3fb950');
  drawSpark('spark-dec',hist.decrypt_per_min,'#58a6ff');

  updateFeed(d.request_log||[]);
  updatePods(d.all_pods||[],pod.pod_name);

  var dot=document.getElementById('status-dot');
  dot.style.background=(t.errors_5xx_per_min||0)>0?'#f85149':((t.errors_4xx_per_min||0)>0?'#d29922':'#3fb950');
}

function refresh(){
  var sp=document.getElementById('spinner');
  sp.className='spinner active';
  fetch('api/status',{credentials:'same-origin'})
    .then(function(r){return r.json()})
    .then(function(d){update(d);sp.className='spinner';})
    .catch(function(){sp.className='spinner';});
}
refresh();
setInterval(refresh,3000);
</script>
</body>
</html>
"""
