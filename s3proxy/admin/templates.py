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
body{background:#0f1117;color:#c9d1d9;font-family:'SF Mono',Menlo,Consolas,monospace;font-size:14px;padding:24px;max-width:900px;margin:0 auto}
h1{font-size:18px;color:#f0f6fc;margin-bottom:4px}
.subtitle{color:#8b949e;font-size:12px;margin-bottom:24px}
.refresh-badge{display:inline-block;background:#1c2128;border:1px solid #30363d;border-radius:4px;padding:2px 8px;font-size:11px;color:#8b949e}
.section{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:16px}
.section-title{font-size:13px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;font-weight:600}
.row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #21262d}
.row:last-child{border-bottom:none}
.label{color:#8b949e}
.value{color:#f0f6fc;font-weight:500}
.bar-container{width:120px;height:8px;background:#21262d;border-radius:4px;display:inline-block;vertical-align:middle;margin-left:8px}
.bar-fill{height:100%;border-radius:4px;transition:width 0.3s}
.bar-green{background:#3fb950}
.bar-yellow{background:#d29922}
.bar-red{background:#f85149}
table{width:100%;border-collapse:collapse;margin-top:8px}
th{text-align:left;color:#8b949e;font-weight:500;padding:6px 8px;border-bottom:1px solid #30363d;font-size:12px}
td{padding:6px 8px;border-bottom:1px solid #21262d;color:#c9d1d9}
.empty{color:#484f58;font-style:italic;padding:12px 0}
.num{font-variant-numeric:tabular-nums}
#status-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#3fb950;margin-right:6px}
</style>
</head>
<body>
<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px">
<div><h1><span id="status-dot"></span>S3Proxy Admin</h1><div class="subtitle">Encryption proxy dashboard</div></div>
<div class="refresh-badge">auto-refresh: 10s</div>
</div>

<div class="section">
<div class="section-title">Key Status</div>
<div class="row"><span class="label">KEK Fingerprint</span><span class="value" id="kek-fp"></span></div>
<div class="row"><span class="label">Algorithm</span><span class="value" id="algo"></span></div>
<div class="row"><span class="label">DEK Tag Name</span><span class="value" id="dek-tag"></span></div>
</div>

<div class="section">
<div class="section-title">Active Uploads</div>
<div class="row"><span class="label">Count</span><span class="value num" id="upload-count"></span></div>
<div id="uploads-table"></div>
</div>

<div class="section">
<div class="section-title">System Health</div>
<div class="row"><span class="label">Memory</span><span class="value" id="memory"></span></div>
<div class="row"><span class="label">In-Flight Requests</span><span class="value num" id="in-flight"></span></div>
<div class="row"><span class="label">503 Rejections</span><span class="value num" id="rejections"></span></div>
<div class="row"><span class="label">Uptime</span><span class="value" id="uptime"></span></div>
<div class="row"><span class="label">Storage Backend</span><span class="value" id="storage"></span></div>
</div>

<div class="section">
<div class="section-title">Request Stats</div>
<div class="row"><span class="label">Total Requests</span><span class="value num" id="total-req"></span></div>
<div class="row"><span class="label">Encrypt Ops</span><span class="value num" id="encrypt-ops"></span></div>
<div class="row"><span class="label">Decrypt Ops</span><span class="value num" id="decrypt-ops"></span></div>
<div class="row"><span class="label">Bytes Encrypted</span><span class="value" id="bytes-enc"></span></div>
<div class="row"><span class="label">Bytes Decrypted</span><span class="value" id="bytes-dec"></span></div>
</div>

<script>
function fmt(n){return n.toLocaleString()}
function barClass(pct){return pct>80?'bar-red':pct>50?'bar-yellow':'bar-green'}
function bar(pct){return '<div class="bar-container"><div class="bar-fill '+barClass(pct)+'" style="width:'+Math.min(pct,100)+'%"></div></div>'}
function age(iso){
  var s=Math.floor((Date.now()-new Date(iso+'Z'))/1000);
  if(s<60)return s+'s';
  if(s<3600)return Math.floor(s/60)+'m';
  if(s<86400)return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m';
  return Math.floor(s/86400)+'d '+Math.floor((s%86400)/3600)+'h';
}
function update(d){
  var k=d.key_status,u=d.upload_status,h=d.system_health,r=d.request_stats,f=d.formatted;
  document.getElementById('kek-fp').textContent=k.kek_fingerprint;
  document.getElementById('algo').textContent=k.algorithm;
  document.getElementById('dek-tag').textContent=k.dek_tag_name;
  document.getElementById('upload-count').textContent=u.active_count;
  var ut=document.getElementById('uploads-table');
  if(u.uploads.length===0){ut.innerHTML='<div class="empty">No active uploads</div>';}
  else{var h2='<table><tr><th>Bucket</th><th>Key</th><th>Parts</th><th>Size</th><th>Age</th></tr>';
    u.uploads.forEach(function(up){h2+='<tr><td>'+up.bucket+'</td><td>'+up.key+'</td><td class="num">'+up.parts_count+'</td><td class="num">'+up.total_plaintext_size+'</td><td>'+age(up.created_at)+'</td></tr>';});
    h2+='</table>';ut.innerHTML=h2;}
  document.getElementById('memory').innerHTML=f.memory_reserved+' / '+f.memory_limit+' ('+h.memory_usage_pct+'%)'+bar(h.memory_usage_pct);
  document.getElementById('in-flight').textContent=fmt(h.requests_in_flight);
  document.getElementById('rejections').textContent=fmt(h.memory_rejections);
  document.getElementById('uptime').textContent=f.uptime;
  document.getElementById('storage').textContent=h.storage_backend;
  document.getElementById('total-req').textContent=fmt(r.total_requests);
  document.getElementById('encrypt-ops').textContent=fmt(r.encrypt_ops);
  document.getElementById('decrypt-ops').textContent=fmt(r.decrypt_ops);
  document.getElementById('bytes-enc').textContent=f.bytes_encrypted;
  document.getElementById('bytes-dec').textContent=f.bytes_decrypted;
}
function refresh(){
  fetch('api/status',{credentials:'same-origin'})
    .then(function(r){return r.json()})
    .then(update)
    .catch(function(){});
}
refresh();
setInterval(refresh,10000);
</script>
</body>
</html>
"""
