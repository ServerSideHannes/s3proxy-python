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
body{background:#0f1117;color:#c9d1d9;font-family:'SF Mono',Menlo,Consolas,monospace;font-size:13px;padding:20px;max-width:960px;margin:0 auto}

.header{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:16px}
.header-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.pod-name{font-size:16px;color:#f0f6fc;font-weight:700}
.header-meta{color:#8b949e;font-size:12px;margin-top:2px}
.header-meta span{margin-right:16px}
.kek{color:#7ee787}
.tip{color:#484f58;font-size:11px;margin-top:8px;border-top:1px solid #21262d;padding-top:8px}

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
.errors-row .err-val{font-weight:500}
.err-val.hot{color:#f85149}

.throughput-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.tp-item{text-align:center;padding:8px;background:#0f1117;border-radius:6px}
.tp-num{font-size:18px;color:#f0f6fc;font-weight:700;font-variant-numeric:tabular-nums}
.tp-unit{font-size:10px;color:#8b949e;margin-top:2px}
.tp-label{font-size:11px;color:#8b949e;margin-top:4px}

table{width:100%;border-collapse:collapse;margin-top:8px}
th{text-align:left;color:#8b949e;font-weight:500;padding:5px 8px;border-bottom:1px solid #30363d;font-size:11px}
td{padding:5px 8px;border-bottom:1px solid #21262d;color:#c9d1d9;font-size:12px}
td.num{font-variant-numeric:tabular-nums;text-align:right}
.stale{color:#d29922}
.empty{color:#484f58;font-style:italic;padding:12px 0}

.pods-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;margin-bottom:16px}
.pod-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}
.pod-card.current{border-color:#58a6ff}
.pod-card .pc-name{font-size:12px;color:#f0f6fc;font-weight:600;margin-bottom:6px;display:flex;justify-content:space-between}
.pod-card .pc-uptime{color:#8b949e;font-weight:400;font-size:11px}
.pod-card .pc-row{display:flex;justify-content:space-between;font-size:11px;padding:2px 0;color:#8b949e}
.pod-card .pc-val{color:#c9d1d9}
.pod-card .pc-bar{width:100%;height:4px;background:#21262d;border-radius:2px;margin:4px 0}
.pod-card .pc-bar-fill{height:100%;border-radius:2px;transition:width 0.5s ease}
.pod-card.warning{border-left:3px solid #f85149}

#status-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#3fb950;margin-right:4px;transition:background 0.3s}
.refresh-badge{font-size:10px;color:#484f58;display:flex;align-items:center;gap:4px}
.spinner{width:8px;height:8px;border:1.5px solid #30363d;border-top-color:#58a6ff;border-radius:50%;display:inline-block}
.spinner.active{animation:spin 0.6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

canvas.sparkline{display:block;width:100%;height:24px;margin-top:6px}
.bw-row{display:flex;align-items:center;gap:12px;padding:5px 0;border-bottom:1px solid #21262d}
.bw-row:last-child{border-bottom:none}
.bw-label{color:#8b949e;font-size:11px;width:56px;flex-shrink:0}
canvas.bw-spark{display:block;flex:1;height:32px}
.bw-val{color:#f0f6fc;font-size:12px;font-weight:500;width:100px;text-align:right;flex-shrink:0;font-variant-numeric:tabular-nums}
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
<div class="tip" id="tip-line">Served by this pod. Other pods publish metrics via Redis.</div>
</div>

<div class="pods-grid" id="pods-grid"></div>

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

<div class="section">
<div class="section-title">Bandwidth <span class="section-tag">this pod &middot; 10 min</span></div>
<div class="bw-row"><span class="bw-label">encrypt</span><canvas class="bw-spark" id="spark-bw-enc" height="32"></canvas><span class="bw-val" id="bw-enc-val">0 B/min</span></div>
<div class="bw-row"><span class="bw-label">decrypt</span><canvas class="bw-spark" id="spark-bw-dec" height="32"></canvas><span class="bw-val" id="bw-dec-val">0 B/min</span></div>
</div>

<div class="section">
<div class="section-title">Active Uploads <span class="section-tag" id="uploads-source">cluster &middot; Redis</span></div>
<div id="upload-count" style="margin-bottom:6px;color:#8b949e"><span class="value" id="upload-num">0</span> active</div>
<div id="uploads-table"></div>
</div>

<script>
function barClass(p){return p>80?'bar-red':p>50?'bar-yellow':'bar-green'}
function bar(p){return '<div class="bar-container"><div class="bar-fill '+barClass(p)+'" style="width:'+Math.min(p,100)+'%"></div></div>'}
function miniBar(p){return '<div class="pc-bar"><div class="pc-bar-fill '+barClass(p)+'" style="width:'+Math.min(p,100)+'%"></div></div>'}
function age(iso){
  if(!iso)return'-';
  var s=Math.floor((Date.now()-new Date(iso+'Z'))/1000);
  if(s<0)s=0;
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m';
  if(s<86400)return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m';
  return Math.floor(s/86400)+'d '+Math.floor((s%86400)/3600)+'h';
}

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

function updatePods(pods, currentPod){
  var grid=document.getElementById('pods-grid');
  if(!pods||pods.length<=1){grid.innerHTML='';return;}
  var html='';
  pods.forEach(function(p){
    var pod=p.pod||{},h=p.health||{},t=(p.throughput||{}).rates||{},f=p.formatted||{};
    var isCurrent=pod.pod_name===currentPod;
    var hasErrors=(t.errors_5xx_per_min||0)>0||(t.errors_503_per_min||0)>0;
    var cls='pod-card'+(isCurrent?' current':'')+(hasErrors?' warning':'');
    html+='<div class="'+cls+'">';
    html+='<div class="pc-name"><span>'+pod.pod_name+'</span><span class="pc-uptime">'+(f.uptime||'-')+'</span></div>';
    html+=miniBar(h.memory_usage_pct||0);
    html+='<div class="pc-row"><span>Memory</span><span class="pc-val">'+(f.memory_reserved||'0 B')+' / '+(f.memory_limit||'0 B')+' ('+(h.memory_usage_pct||0)+'%)</span></div>';
    html+='<div class="pc-row"><span>In-Flight</span><span class="pc-val">'+(h.requests_in_flight||0)+'</span></div>';
    html+='<div class="pc-row"><span>Throughput</span><span class="pc-val">'+(t.requests_per_min||0)+'/min</span></div>';
    if(hasErrors)html+='<div class="pc-row"><span>Errors</span><span class="pc-val hot">5xx:'+(t.errors_5xx_per_min||0)+' 503:'+(t.errors_503_per_min||0)+'</span></div>';
    html+='</div>';
  });
  grid.innerHTML=html;
}

function update(d){
  var pod=d.pod||{},h=d.health||{},t=(d.throughput||{}).rates||{},f=d.formatted||{},u=d.uploads||{};
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
  drawSpark('spark-req',hist.requests_per_min,'#3fb950');
  drawSpark('spark-enc',hist.encrypt_per_min,'#3fb950');
  drawSpark('spark-dec',hist.decrypt_per_min,'#58a6ff');
  drawSpark('spark-bw-enc',hist.bytes_encrypted_per_min,'#3fb950');
  drawSpark('spark-bw-dec',hist.bytes_decrypted_per_min,'#58a6ff');
  document.getElementById('bw-enc-val').textContent=(f.bytes_encrypted_per_min||'0 B')+'/min';
  document.getElementById('bw-dec-val').textContent=(f.bytes_decrypted_per_min||'0 B')+'/min';

  document.getElementById('upload-num').textContent=u.active_count||0;
  document.getElementById('uploads-source').textContent=pod.storage_backend==='In-memory'?'this pod':'cluster \\u00b7 Redis';
  var ut=document.getElementById('uploads-table');
  if(!u.uploads||u.uploads.length===0){ut.innerHTML='<div class="empty">No active uploads</div>';}
  else{var h2='<table><tr><th>Bucket</th><th>Key</th><th style="text-align:right">Parts</th><th style="text-align:right">Size</th><th style="text-align:right">Age</th></tr>';
    u.uploads.forEach(function(up){
      var stale=up.is_stale?' class="stale"':'';
      h2+='<tr><td>'+up.bucket+'</td><td>'+up.key+'</td><td class="num">'+up.parts_count+'</td><td class="num">'+(up.total_plaintext_size_formatted||up.total_plaintext_size)+'</td><td class="num"'+stale+'>'+age(up.created_at)+(up.is_stale?' \\u26a0':'')+'</td></tr>';
    });
    h2+='</table>';ut.innerHTML=h2;}

  updatePods(d.all_pods||[], pod.pod_name);

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
