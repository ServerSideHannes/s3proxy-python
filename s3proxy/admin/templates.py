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
body{background:#0f1117;color:#f0f6fc;font-family:'SF Mono',Menlo,Consolas,monospace;font-size:13px;padding:20px;max-width:1000px;margin:0 auto}

.section{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:12px}
.section-title{font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;font-weight:600;display:flex;justify-content:space-between;align-items:center}
.section-tag{font-size:10px;color:#484f58;text-transform:none;letter-spacing:0;font-weight:400}

/* Fleet Status Bar */
.fleet-bar{display:flex;justify-content:space-between;align-items:center}
.fleet-status{display:flex;align-items:center;gap:10px}
.fleet-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.fleet-label{font-size:18px;font-weight:700;letter-spacing:1px}
.fleet-meta{color:#8b949e;font-size:12px;margin-top:6px}
.fleet-meta span{margin-right:14px}
.kek-ok{color:#3fb950}
.kek-bad{color:#f85149;font-weight:700}
.refresh-badge{font-size:10px;color:#484f58;display:flex;align-items:center;gap:4px}
.spinner{width:8px;height:8px;border:1.5px solid #30363d;border-top-color:#58a6ff;border-radius:50%;display:inline-block}
.spinner.active{animation:spin 0.6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* Alerts */
.alerts{margin-top:10px}
.alert{display:flex;align-items:center;gap:8px;padding:5px 0;font-size:12px;border-top:1px solid #21262d}
.alert:first-child{border-top:none}
.alert-warn{color:#d29922}
.alert-crit{color:#f85149}
.alert-sev{font-weight:700;font-size:10px;width:60px;flex-shrink:0}

/* Pod Grid */
.pod-table{width:100%;border-collapse:collapse}
.pod-table th{text-align:left;color:#8b949e;font-weight:500;padding:4px 8px;border-bottom:1px solid #30363d;font-size:11px}
.pod-table th.r,.pod-table td.r{text-align:right}
.pod-table td{padding:5px 8px;border-bottom:1px solid #1c2128;font-size:12px;font-variant-numeric:tabular-nums}
.pod-table tr.pod-row{cursor:pointer;transition:background 0.15s}
.pod-table tr.pod-row:hover{background:#1c2128}
.pod-table tr.pod-row.current{background:#161b22;border-left:2px solid #58a6ff}
.pod-dot{width:6px;height:6px;border-radius:50%;display:inline-block}
.bar-container{width:60px;height:5px;background:#21262d;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:6px}
.bar-fill{height:100%;border-radius:3px}
.bar-green{background:#3fb950}
.bar-yellow{background:#d29922}
.bar-red{background:#f85149}
.pod-expand{display:none;background:#0f1117;border-bottom:1px solid #21262d}
.pod-expand td{padding:12px 8px}
.pod-expand.open{display:table-row}
canvas.sparkline{display:block;width:100%;height:24px;margin-top:4px}
.spark-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
.spark-item{text-align:center}
.spark-label{font-size:10px;color:#8b949e}

/* Throughput */
.tp-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.tp-item{text-align:center;padding:8px;background:#0f1117;border-radius:6px}
.tp-num{font-size:20px;color:#f0f6fc;font-weight:700;font-variant-numeric:tabular-nums}
.tp-unit{font-size:10px;color:#8b949e;margin-top:2px}
.tp-label{font-size:11px;color:#8b949e;margin-top:4px}
canvas.tp-spark{display:block;width:100%;height:24px;margin-top:6px}

/* Errors + Latency */
.el-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.err-row{display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid #21262d}
.err-row:last-child{border-bottom:none}
.err-label{color:#8b949e;font-size:12px}
.err-val{font-weight:600;font-size:13px;font-variant-numeric:tabular-nums}
.err-val.hot{color:#f85149}
canvas.err-spark{width:80px;height:16px;display:inline-block;vertical-align:middle;margin-left:8px}
.lat-row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #21262d}
.lat-row:last-child{border-bottom:none}
.lat-label{color:#8b949e;font-size:12px}
.lat-val{font-weight:600;font-size:13px;font-variant-numeric:tabular-nums}
.lat-warn{color:#d29922}
.lat-bad{color:#f85149}

/* Recent Requests */
.feed-table{width:100%;border-collapse:collapse}
.feed-table th{position:sticky;top:0;background:#161b22;z-index:1;text-align:left;color:#8b949e;font-weight:500;padding:4px 6px;border-bottom:1px solid #30363d;font-size:11px}
.feed-table td{padding:3px 6px;border-bottom:1px solid #1c2128;font-size:12px;white-space:nowrap;font-variant-numeric:tabular-nums}
.feed-table tr.new-row{animation:rowIn 0.5s ease}
@keyframes rowIn{from{background:rgba(63,185,80,0.1)}to{background:transparent}}
.method-get{color:#58a6ff}.method-put{color:#3fb950}.method-del{color:#f85149}.method-other{color:#8b949e}
.status-2xx{color:#3fb950}.status-4xx{color:#d29922}.status-5xx{color:#f85149}
.latency-warn{color:#d29922}.latency-bad{color:#f85149}
.path-cell{max-width:180px;overflow:hidden;text-overflow:ellipsis}
.crypto-badge{font-size:10px;padding:1px 4px;border-radius:3px;font-weight:600;letter-spacing:0.5px}
.crypto-enc{background:#1c3a2a;color:#3fb950}.crypto-dec{background:#172030;color:#58a6ff}
.empty{color:#484f58;font-style:italic;padding:8px 0}
td.r{text-align:right}
</style>
</head>
<body>

<!-- Fleet Status Bar -->
<div class="section" id="fleet-section">
<div class="fleet-bar">
<div>
<div class="fleet-status">
<span class="fleet-dot" id="fleet-dot" style="background:#3fb950"></span>
<span class="fleet-label" id="fleet-label">HEALTHY</span>
</div>
<div class="fleet-meta">
<span id="fleet-pods">1 pod</span>
<span id="fleet-storage">-</span>
<span>KEK <strong id="fleet-kek" class="kek-ok">-</strong></span>
</div>
</div>
<div class="refresh-badge"><span class="spinner" id="spinner"></span> <span>3s</span></div>
</div>
<div class="alerts" id="alerts"></div>
</div>

<!-- Pod Grid -->
<div class="section">
<div class="section-title">Pods <span class="section-tag" id="pod-count-tag">1 pod</span></div>
<table class="pod-table" id="pod-table">
<thead><tr>
<th>Pod</th><th>Uptime</th><th>Memory</th><th class="r">In-Fl</th>
<th class="r">Req/m</th><th class="r">Enc/m</th><th class="r">Dec/m</th><th class="r">5xx/m</th><th></th>
</tr></thead>
<tbody id="pod-body"></tbody>
</table>
</div>

<!-- Fleet Throughput -->
<div class="section">
<div class="section-title">Throughput <span class="section-tag" id="tp-tag">all pods &middot; 10 min</span></div>
<div class="tp-grid">
<div class="tp-item"><div class="tp-num" id="tp-req">0</div><div class="tp-unit">/min</div><div class="tp-label">requests</div><canvas class="tp-spark" id="spark-req" height="24"></canvas></div>
<div class="tp-item"><div class="tp-num" id="tp-enc">0</div><div class="tp-unit">/min &middot; <span id="tp-enc-bytes">0 B</span>/min</div><div class="tp-label">encrypt</div><canvas class="tp-spark" id="spark-enc" height="24"></canvas></div>
<div class="tp-item"><div class="tp-num" id="tp-dec">0</div><div class="tp-unit">/min &middot; <span id="tp-dec-bytes">0 B</span>/min</div><div class="tp-label">decrypt</div><canvas class="tp-spark" id="spark-dec" height="24"></canvas></div>
</div>
</div>

<!-- Errors + Latency -->
<div class="section">
<div class="el-grid">
<div>
<div class="section-title">Errors <span class="section-tag">fleet &middot; 10 min rate</span></div>
<div class="err-row"><span class="err-label">4xx</span><span><span class="err-val" id="err-4xx">0</span>/min<canvas class="err-spark" id="spark-4xx" height="16"></canvas></span></div>
<div class="err-row"><span class="err-label">5xx</span><span><span class="err-val" id="err-5xx">0</span>/min<canvas class="err-spark" id="spark-5xx" height="16"></canvas></span></div>
<div class="err-row"><span class="err-label">503</span><span><span class="err-val" id="err-503">0</span>/min<canvas class="err-spark" id="spark-503" height="16"></canvas></span></div>
</div>
<div>
<div class="section-title">Latency <span class="section-tag">this pod</span></div>
<div class="lat-row"><span class="lat-label">p50</span><span class="lat-val" id="lat-p50">-</span></div>
<div class="lat-row"><span class="lat-label">p95</span><span class="lat-val" id="lat-p95">-</span></div>
<div class="lat-row"><span class="lat-label">p99</span><span class="lat-val" id="lat-p99">-</span></div>
<div style="margin-top:6px;font-size:11px;color:#484f58"><span id="lat-count">0</span> total requests</div>
</div>
</div>
</div>

<!-- Recent Requests -->
<div class="section">
<div class="section-title">Recent Requests <span class="section-tag">last 10 &middot; this pod</span></div>
<table class="feed-table">
<thead><tr><th>Time</th><th>Method</th><th>Path</th><th>Op</th><th style="text-align:right">Status</th><th style="text-align:right">Latency</th><th style="text-align:right">Size</th><th></th></tr></thead>
<tbody id="feed-body"></tbody>
</table>
<div class="empty" id="feed-empty">Waiting for requests...</div>
</div>

<script>
/* Helpers */
function barClass(p){return p>80?'bar-red':p>50?'bar-yellow':'bar-green'}
function barHtml(p){return '<div class="bar-container"><div class="bar-fill '+barClass(p)+'" style="width:'+Math.min(p,100)+'%"></div></div>'}
function fmtUptime(s){var d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);var p=[];if(d)p.push(d+'d');if(h)p.push(h+'h');p.push(m+'m');return p.join(' ')}
function fmtBytes(n){if(!n||n<=0)return'-';if(n<1024)return n+'B';if(n<1048576)return(n/1024).toFixed(1)+'KB';if(n<1073741824)return(n/1048576).toFixed(1)+'MB';return(n/1073741824).toFixed(1)+'GB'}
function fmtLatency(ms){if(!ms||ms<=0)return'-';if(ms<1)return'<1ms';if(ms<1000)return Math.round(ms)+'ms';return(ms/1000).toFixed(1)+'s'}
function latClass(ms){if(ms>5000)return' lat-bad';if(ms>1000)return' lat-warn';return''}
function methodClass(m){return m==='GET'?'method-get':m==='PUT'?'method-put':m==='DELETE'?'method-del':'method-other'}
function statusClass(s){return s>=500?'status-5xx':s>=400?'status-4xx':'status-2xx'}
function cryptoBadge(c){if(c==='encrypt')return'<span class="crypto-badge crypto-enc">ENC</span>';if(c==='decrypt')return'<span class="crypto-badge crypto-dec">DEC</span>';return''}
function fmtTime(ts){return new Date(ts*1000).toLocaleTimeString('en-GB',{hour12:false})}

function drawSpark(id,data,color){
  var c=document.getElementById(id);if(!c)return;
  var dpr=window.devicePixelRatio||1,w=c.offsetWidth,h=parseInt(c.getAttribute('height'))||24;
  c.width=w*dpr;c.height=h*dpr;
  var ctx=c.getContext('2d');ctx.scale(dpr,dpr);
  if(!data||data.length<2)return;
  var mx=0;for(var i=0;i<data.length;i++)if(data[i]>mx)mx=data[i];
  if(!mx)return;
  var bw=Math.max(1,Math.floor(w/data.length)-1);
  ctx.fillStyle=color||'#3fb950';
  for(var i=0;i<data.length;i++){var bh=Math.round((data[i]/mx)*(h-2));if(data[i]>0&&bh<1)bh=1;ctx.fillRect(i*(bw+1),h-bh,bw,bh)}
}

/* Fleet Status + Alerts */
var _knownPods={};
function computeAlerts(d){
  var alerts=[],pods=d.all_pods||[];
  var localPod=d.pod||{},localHealth=d.health||{},localRates=(d.throughput||{}).rates||{};

  // Build pod list (use all_pods if available, else just local)
  var podList=pods.length>0?pods:[{pod:localPod,health:localHealth,throughput:d.throughput||{}}];

  // KEK consistency
  var keks={};
  podList.forEach(function(p){var k=(p.pod||{}).kek_fingerprint;if(k)keks[k]=(keks[k]||0)+1});
  if(Object.keys(keks).length>1)alerts.push({sev:'warn',msg:'KEK fingerprint mismatch across pods'});

  podList.forEach(function(p){
    var h=p.health||{},r=((p.throughput||{}).rates)||{},name=(p.pod||{}).pod_name||'?';
    if(h.memory_usage_pct>95)alerts.push({sev:'crit',msg:name+' memory at '+h.memory_usage_pct+'%'});
    else if(h.memory_usage_pct>85)alerts.push({sev:'warn',msg:name+' memory at '+h.memory_usage_pct+'%'});
    if((r.errors_5xx_per_min||0)>0)alerts.push({sev:'crit',msg:name+' has 5xx errors: '+r.errors_5xx_per_min+'/min'});
    if((r.errors_503_per_min||0)>0)alerts.push({sev:'warn',msg:name+' backpressure active: '+r.errors_503_per_min+' 503/min'});
  });

  // Latency
  var lat=d.latency||{};
  if((lat.p99_ms||0)>5000)alerts.push({sev:'warn',msg:'p99 latency '+fmtLatency(lat.p99_ms)});

  // Missing pods
  var currentNames={};
  podList.forEach(function(p){var n=(p.pod||{}).pod_name;if(n){currentNames[n]=true;_knownPods[n]=true}});
  Object.keys(_knownPods).forEach(function(n){if(!currentNames[n])alerts.push({sev:'crit',msg:n+' not reporting'})});

  return alerts;
}

function updateFleet(d){
  var alerts=computeAlerts(d);
  var pods=d.all_pods||[];
  var podList=pods.length>0?pods:[{pod:d.pod,health:d.health,throughput:d.throughput}];

  // Fleet status
  var hasCrit=alerts.some(function(a){return a.sev==='crit'});
  var hasWarn=alerts.some(function(a){return a.sev==='warn'});
  var dot=document.getElementById('fleet-dot'),lbl=document.getElementById('fleet-label');
  if(hasCrit){dot.style.background='#f85149';lbl.textContent='CRITICAL';lbl.style.color='#f85149'}
  else if(hasWarn){dot.style.background='#d29922';lbl.textContent='DEGRADED';lbl.style.color='#d29922'}
  else{dot.style.background='#3fb950';lbl.textContent='HEALTHY';lbl.style.color='#3fb950'}

  // Fleet meta
  document.getElementById('fleet-pods').textContent=podList.length+' pod'+(podList.length!==1?'s':'');
  document.getElementById('fleet-storage').textContent=(d.pod||{}).storage_backend||'-';
  document.getElementById('pod-count-tag').textContent=podList.length+' pod'+(podList.length!==1?'s':'');

  // KEK
  var keks={};podList.forEach(function(p){var k=(p.pod||{}).kek_fingerprint;if(k)keks[k]=true});
  var kekEl=document.getElementById('fleet-kek');
  var kekKeys=Object.keys(keks);
  if(kekKeys.length===1){kekEl.textContent=kekKeys[0]+' (all match)';kekEl.className='kek-ok'}
  else if(kekKeys.length>1){kekEl.textContent='MISMATCH';kekEl.className='kek-bad'}
  else{kekEl.textContent=(d.pod||{}).kek_fingerprint||'-';kekEl.className='kek-ok'}

  // Alerts
  var alertEl=document.getElementById('alerts');
  if(alerts.length===0){alertEl.innerHTML='';return}
  var h='';alerts.forEach(function(a){
    var cls=a.sev==='crit'?'alert-crit':'alert-warn';
    h+='<div class="alert '+cls+'"><span class="alert-sev">'+a.sev.toUpperCase()+'</span>'+a.msg+'</div>';
  });
  alertEl.innerHTML=h;
}

/* Pod Grid */
var _expandedPod=null;
function updatePods(d){
  var pods=d.all_pods||[];
  var localPod=d.pod||{};
  var podList=pods.length>0?pods:[{pod:localPod,health:d.health,throughput:d.throughput,formatted:d.formatted}];
  var body=document.getElementById('pod-body');
  var html='';

  podList.forEach(function(p,idx){
    var pod=p.pod||{},h=p.health||{},r=((p.throughput||{}).rates)||{},f=p.formatted||{};
    var isCurrent=pod.pod_name===localPod.pod_name;
    var memPct=h.memory_usage_pct||0;
    var dotColor=(r.errors_5xx_per_min||0)>0?'#f85149':memPct>80?'#f85149':memPct>50?'#d29922':'#3fb950';
    var cls='pod-row'+(isCurrent?' current':'');
    var expanded=_expandedPod===pod.pod_name;

    html+='<tr class="'+cls+'" data-pod="'+pod.pod_name+'">';
    html+='<td>'+(expanded?'&#9660; ':'&#9654; ')+pod.pod_name+'</td>';
    html+='<td>'+(f.uptime||fmtUptime(pod.uptime_seconds||0))+'</td>';
    html+='<td>'+Math.round(memPct)+'%'+barHtml(memPct)+'</td>';
    html+='<td class="r">'+(h.requests_in_flight||0)+'</td>';
    html+='<td class="r">'+(r.requests_per_min||0)+'</td>';
    html+='<td class="r">'+(r.encrypt_per_min||0)+'</td>';
    html+='<td class="r">'+(r.decrypt_per_min||0)+'</td>';
    html+='<td class="r" style="color:'+((r.errors_5xx_per_min||0)>0?'#f85149':'#8b949e')+'">'+(r.errors_5xx_per_min||0)+'</td>';
    html+='<td><span class="pod-dot" style="background:'+dotColor+'"></span></td>';
    html+='</tr>';

    // Expandable detail row
    html+='<tr class="pod-expand'+(expanded?' open':'')+'" id="expand-'+idx+'">';
    html+='<td colspan="9"><div class="spark-grid">';
    html+='<div class="spark-item"><div class="spark-label">requests/min</div><canvas class="sparkline" id="pod-spark-req-'+idx+'" height="24"></canvas></div>';
    html+='<div class="spark-item"><div class="spark-label">encrypt/min</div><canvas class="sparkline" id="pod-spark-enc-'+idx+'" height="24"></canvas></div>';
    html+='<div class="spark-item"><div class="spark-label">decrypt/min</div><canvas class="sparkline" id="pod-spark-dec-'+idx+'" height="24"></canvas></div>';
    html+='</div></td></tr>';
  });

  body.innerHTML=html;

  // Draw sparklines for expanded pods
  podList.forEach(function(p,idx){
    if(_expandedPod!==(p.pod||{}).pod_name)return;
    var hist=((p.throughput||{}).history)||{};
    setTimeout(function(){
      drawSpark('pod-spark-req-'+idx,hist.requests_per_min,'#58a6ff');
      drawSpark('pod-spark-enc-'+idx,hist.encrypt_per_min,'#3fb950');
      drawSpark('pod-spark-dec-'+idx,hist.decrypt_per_min,'#58a6ff');
    },10);
  });

  // Click handlers
  body.querySelectorAll('.pod-row').forEach(function(row){
    row.addEventListener('click',function(){
      var name=row.getAttribute('data-pod');
      _expandedPod=_expandedPod===name?null:name;
      updatePods(window._lastData||d);
    });
  });
}

/* Fleet Throughput */
function updateThroughput(d){
  var pods=d.all_pods||[];
  var podList=pods.length>0?pods:[{throughput:d.throughput,formatted:d.formatted}];

  // Sum rates from all pods
  var req=0,enc=0,dec=0,bytesEnc=0,bytesDec=0;
  podList.forEach(function(p){
    var r=((p.throughput||{}).rates)||{};
    req+=(r.requests_per_min||0);enc+=(r.encrypt_per_min||0);dec+=(r.decrypt_per_min||0);
    bytesEnc+=(r.bytes_encrypted_per_min||0);bytesDec+=(r.bytes_decrypted_per_min||0);
  });

  document.getElementById('tp-req').textContent=Math.round(req*10)/10;
  document.getElementById('tp-enc').textContent=Math.round(enc*10)/10;
  document.getElementById('tp-dec').textContent=Math.round(dec*10)/10;
  document.getElementById('tp-enc-bytes').textContent=fmtBytes(bytesEnc);
  document.getElementById('tp-dec-bytes').textContent=fmtBytes(bytesDec);
  document.getElementById('tp-tag').textContent=(podList.length>1?'all pods':'this pod')+' \\u00b7 10 min';

  // Sparklines from serving pod
  var hist=(d.throughput||{}).history||{};
  drawSpark('spark-req',hist.requests_per_min,'#58a6ff');
  drawSpark('spark-enc',hist.encrypt_per_min,'#3fb950');
  drawSpark('spark-dec',hist.decrypt_per_min,'#58a6ff');
}

/* Errors */
function updateErrors(d){
  var pods=d.all_pods||[];
  var podList=pods.length>0?pods:[{throughput:d.throughput}];
  var e4=0,e5=0,e503=0;
  podList.forEach(function(p){
    var r=((p.throughput||{}).rates)||{};
    e4+=(r.errors_4xx_per_min||0);e5+=(r.errors_5xx_per_min||0);e503+=(r.errors_503_per_min||0);
  });
  var el4=document.getElementById('err-4xx'),el5=document.getElementById('err-5xx'),el503=document.getElementById('err-503');
  el4.textContent=Math.round(e4*10)/10;
  el5.textContent=Math.round(e5*10)/10;el5.className='err-val'+(e5>0?' hot':'');
  el503.textContent=Math.round(e503*10)/10;el503.className='err-val'+(e503>0?' hot':'');

  // Error sparklines from serving pod history (not available yet, skip)
}

/* Latency */
function updateLatency(d){
  var lat=d.latency||{};
  var p50El=document.getElementById('lat-p50'),p95El=document.getElementById('lat-p95'),p99El=document.getElementById('lat-p99');
  p50El.textContent=fmtLatency(lat.p50_ms);p50El.className='lat-val'+latClass(lat.p50_ms||0);
  p95El.textContent=fmtLatency(lat.p95_ms);p95El.className='lat-val'+latClass(lat.p95_ms||0);
  p99El.textContent=fmtLatency(lat.p99_ms);p99El.className='lat-val'+latClass(lat.p99_ms||0);
  document.getElementById('lat-count').textContent=lat.count||0;
}

/* Recent Requests */
var _prevFirst=0;
function updateFeed(log){
  var body=document.getElementById('feed-body'),empty=document.getElementById('feed-empty');
  if(!log||log.length===0){body.innerHTML='';empty.style.display='block';return}
  empty.style.display='none';
  var isNew=log[0].timestamp!==_prevFirst;_prevFirst=log[0].timestamp;
  var html='';
  for(var i=0;i<log.length;i++){
    var r=log[i],cls=i===0&&isNew?' class="new-row"':'';
    html+='<tr'+cls+'>';
    html+='<td>'+fmtTime(r.timestamp)+'</td>';
    html+='<td class="'+methodClass(r.method)+'">'+r.method+'</td>';
    html+='<td class="path-cell" title="'+r.path+'">'+r.path+'</td>';
    html+='<td style="color:#8b949e">'+r.operation+'</td>';
    html+='<td class="r '+statusClass(r.status)+'">'+r.status+'</td>';
    html+='<td class="r'+(r.duration_ms>5000?' latency-bad':r.duration_ms>1000?' latency-warn':'')+'">'+fmtLatency(r.duration_ms)+'</td>';
    html+='<td class="r">'+fmtBytes(r.size)+'</td>';
    html+='<td>'+cryptoBadge(r.crypto)+'</td></tr>';
  }
  body.innerHTML=html;
}

/* Main update */
function update(d){
  window._lastData=d;
  updateFleet(d);
  updatePods(d);
  updateThroughput(d);
  updateErrors(d);
  updateLatency(d);
  updateFeed(d.request_log||[]);
}

function refresh(){
  var sp=document.getElementById('spinner');sp.className='spinner active';
  fetch('api/status',{credentials:'same-origin'})
    .then(function(r){return r.json()})
    .then(function(d){update(d);sp.className='spinner'})
    .catch(function(){sp.className='spinner'});
}
refresh();setInterval(refresh,3000);
</script>
</body>
</html>
"""
