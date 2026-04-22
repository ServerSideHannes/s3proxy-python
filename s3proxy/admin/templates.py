"""HTML template for the admin dashboard."""

from __future__ import annotations

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>S3 Encryption Proxy</title>
<style>
  :root {
    --bg: #fafafa;
    --surface: #ffffff;
    --border: #e5e7eb;
    --border-strong: #d1d5db;
    --text: #111827;
    --text-muted: #6b7280;
    --text-subtle: #9ca3af;
    --ok: #10b981;
    --ok-bg: #ecfdf5;
    --err: #ef4444;
    --err-bg: #fef2f2;
    --dark: #111827;
    --icon-bg: #f3f4f6;
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI",
                 Helvetica, Arial, sans-serif;
    font-size: 14px;
    line-height: 1.4;
    -webkit-font-smoothing: antialiased;
  }
  .page {
    max-width: 1080px;
    margin: 0 auto;
    padding: 32px 24px 96px 24px;
  }
  /* ---- Header ---- */
  .app-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 24px;
  }
  .brand { display: flex; align-items: center; gap: 12px; }
  .brand-mark {
    width: 40px; height: 40px;
    background: var(--dark);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
  }
  .brand-mark svg { color: #fff; }
  .brand-name { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; }
  .head-right { display: flex; align-items: center; gap: 16px; }
  .status-pill {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px;
    border: 1px solid var(--ok);
    border-radius: 999px;
    color: var(--ok);
    font-size: 13px;
    background: transparent;
  }
  .status-pill .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--ok);
  }
  .uptime { color: var(--text-muted); font-size: 13px; }
  /* ---- Grid of summary cards ---- */
  .cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 20px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px;
  }
  .card-head {
    display: flex;
    align-items: center;
    gap: 10px;
    color: var(--text-muted);
    font-size: 13px;
    margin-bottom: 10px;
  }
  .card-icon {
    width: 30px; height: 30px;
    background: var(--icon-bg);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text);
  }
  .card-value {
    font-size: 34px;
    font-weight: 600;
    letter-spacing: -0.02em;
    line-height: 1.1;
  }
  .card-unit {
    font-size: 16px;
    font-weight: 500;
    color: var(--text-muted);
    margin-left: 4px;
  }
  .card-delta { margin-top: 6px; font-size: 12px; color: var(--text-muted); }
  .card-delta .up   { color: var(--ok); }
  .card-delta .down { color: var(--ok); }  /* down is good for errors */
  .spark {
    margin-top: 12px;
    width: 100%;
    height: 28px;
    display: block;
    color: var(--text-subtle);
  }
  /* ---- Section card ---- */
  .section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 20px;
  }
  .section-head {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 14px;
  }
  .section-title { font-size: 16px; font-weight: 600; }
  .section-actions { display: flex; align-items: center; gap: 18px; }
  .live {
    display: inline-flex; align-items: center; gap: 6px;
    color: var(--text-muted); font-size: 13px;
  }
  .live .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--ok); box-shadow: 0 0 0 3px rgba(16,185,129,0.15);
  }
  .link-action {
    color: var(--text); font-size: 13px; text-decoration: none;
  }
  .link-action:hover { text-decoration: underline; }
  /* ---- Tables ---- */
  table { width: 100%; border-collapse: collapse; }
  th, td {
    text-align: left;
    padding: 10px 8px;
    font-size: 13px;
    white-space: nowrap;
  }
  th {
    color: var(--text-muted);
    font-weight: 500;
    border-bottom: 1px solid var(--border);
  }
  td {
    border-bottom: 1px solid var(--border);
    color: var(--text);
  }
  tr:last-child td { border-bottom: none; }
  td.truncate {
    max-width: 200px;
    overflow: hidden; text-overflow: ellipsis;
  }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 500;
  }
  .pill.ok  { background: var(--ok-bg);  color: var(--ok); }
  .pill.err { background: var(--err-bg); color: var(--err); }
  .enc-cell { display: inline-flex; align-items: center; gap: 6px; }
  .enc-cell.on  { color: var(--ok); }
  .enc-cell.off { color: var(--text-muted); }
  /* ---- Two-column bottom row ---- */
  .split {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }
  .btn-dark {
    background: #111827;
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 6px 12px;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    display: inline-flex; align-items: center; gap: 4px;
  }
  .btn-dark:hover { background: #1f2937; }
  .btn-ghost {
    background: transparent;
    color: var(--text);
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    padding: 5px 10px;
    font-size: 12px;
    cursor: pointer;
    display: inline-flex; align-items: center; gap: 4px;
  }
  .btn-ghost:hover { background: var(--icon-bg); }
  .view-more {
    display: inline-block;
    color: var(--text);
    font-size: 13px;
    margin-top: 10px;
    text-decoration: none;
  }
  .view-more:hover { text-decoration: underline; }
  /* ---- Footer bar ---- */
  .footer {
    position: fixed; left: 0; right: 0; bottom: 0;
    border-top: 1px solid var(--border);
    background: var(--surface);
    padding: 10px 24px;
    font-size: 12px;
    color: var(--text-muted);
    display: flex; align-items: center; gap: 28px;
  }
  .footer .brand-mini { display: inline-flex; align-items: center; gap: 6px; }
  .footer .spacer { flex: 1; }
  /* ---- Responsive ---- */
  @media (max-width: 880px) {
    .cards { grid-template-columns: repeat(2, 1fr); }
    .split { grid-template-columns: 1fr; }
  }
  @media (max-width: 520px) {
    .cards { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<div class="page">

  <header class="app-head">
    <div class="brand">
      <span class="brand-mark" aria-hidden="true">
        <!-- lock icon -->
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
             stroke="currentColor" stroke-width="2"
             stroke-linecap="round" stroke-linejoin="round">
          <rect x="4" y="11" width="16" height="10" rx="2"></rect>
          <path d="M8 11V7a4 4 0 0 1 8 0v4"></path>
        </svg>
      </span>
      <span class="brand-name" id="h-title">S3 Encryption Proxy</span>
    </div>
    <div class="head-right">
      <span class="status-pill"><span class="dot"></span><span id="h-status">Running</span></span>
      <span class="uptime">Uptime: <span id="h-uptime">—</span></span>
    </div>
  </header>

  <section class="cards">
    <div class="card">
      <div class="card-head">
        <span class="card-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round">
            <path d="M7 7h11l-3-3"></path><path d="M17 17H6l3 3"></path>
          </svg>
        </span>
        <span id="c1-label">Requests</span>
      </div>
      <div><span class="card-value" id="c1-value">—</span><span class="card-unit" id="c1-unit"></span></div>
      <div class="card-delta" id="c1-delta">&nbsp;</div>
      <svg class="spark" id="c1-spark" viewBox="0 0 100 28" preserveAspectRatio="none"></svg>
    </div>

    <div class="card">
      <div class="card-head">
        <span class="card-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round">
            <rect x="4" y="11" width="16" height="10" rx="2"></rect>
            <path d="M8 11V7a4 4 0 0 1 8 0v4"></path>
          </svg>
        </span>
        <span id="c2-label">Data Encrypted</span>
      </div>
      <div><span class="card-value" id="c2-value">—</span><span class="card-unit" id="c2-unit"></span></div>
      <div class="card-delta" id="c2-delta">&nbsp;</div>
      <svg class="spark" id="c2-spark" viewBox="0 0 100 28" preserveAspectRatio="none"></svg>
    </div>

    <div class="card">
      <div class="card-head">
        <span class="card-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round">
            <path d="M10.3 3.9 2 18a2 2 0 0 0 1.7 3h16.6A2 2 0 0 0 22 18L13.7 3.9a2 2 0 0 0-3.4 0z"></path>
            <line x1="12" y1="9" x2="12" y2="13"></line>
            <line x1="12" y1="17" x2="12.01" y2="17"></line>
          </svg>
        </span>
        <span id="c3-label">Errors</span>
      </div>
      <div><span class="card-value" id="c3-value">—</span><span class="card-unit" id="c3-unit"></span></div>
      <div class="card-delta" id="c3-delta">&nbsp;</div>
      <svg class="spark" id="c3-spark" viewBox="0 0 100 28" preserveAspectRatio="none"></svg>
    </div>

    <div class="card">
      <div class="card-head">
        <span class="card-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round">
            <path d="M4 8c0-2.2 3.6-4 8-4s8 1.8 8 4-3.6 4-8 4-8-1.8-8-4z"></path>
            <path d="M4 8v8c0 2.2 3.6 4 8 4s8-1.8 8-4V8"></path>
          </svg>
        </span>
        <span id="c4-label">Active Buckets</span>
      </div>
      <div><span class="card-value" id="c4-value">—</span></div>
      <div class="card-delta" id="c4-delta">&nbsp;</div>
    </div>
  </section>

  <section class="section">
    <div class="section-head">
      <div class="section-title">Recent Activity</div>
      <div class="section-actions">
        <span class="live"><span class="dot"></span>Live</span>
        <a href="#" class="link-action">View all logs →</a>
      </div>
    </div>
    <table>
      <thead>
        <tr>
          <th>Time</th><th>Operation</th><th>Bucket</th><th>Object</th>
          <th>Status</th><th>Size</th><th>Client IP</th><th>Latency</th>
        </tr>
      </thead>
      <tbody id="activity-body">
        <tr><td colspan="8" style="color:var(--text-muted);padding:18px 8px">
          No requests yet — traffic will appear here.
        </td></tr>
      </tbody>
    </table>
  </section>

  <section class="split">
    <div class="section" style="margin-bottom:0">
      <div class="section-head">
        <div class="section-title">Buckets</div>
        <button class="btn-dark" type="button">+ Add Bucket</button>
      </div>
      <table>
        <thead>
          <tr><th>Name</th><th>Encryption</th><th>Objects</th><th>Size</th></tr>
        </thead>
        <tbody id="buckets-body">
          <tr><td colspan="4" style="color:var(--text-muted);padding:14px 8px">
            No buckets observed yet.
          </td></tr>
        </tbody>
      </table>
      <a href="#" class="view-more">View all buckets →</a>
    </div>

    <div class="section" style="margin-bottom:0">
      <div class="section-head">
        <div class="section-title">Keys</div>
        <button class="btn-dark" type="button">+ Create Key</button>
      </div>
      <table>
        <thead>
          <tr><th>Key ID</th><th>Type</th><th>Status</th><th>Created</th></tr>
        </thead>
        <tbody id="keys-body"></tbody>
      </table>
      <a href="#" class="view-more">View all keys →</a>
    </div>
  </section>

</div>

<footer class="footer">
  <span class="brand-mini">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
         stroke="currentColor" stroke-width="2"
         stroke-linecap="round" stroke-linejoin="round">
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path>
    </svg>
    Proxy Version: <span id="f-version">—</span>
  </span>
  <span>Requests: <span id="f-rps">0</span> req/s</span>
  <span>Throughput: <span id="f-throughput">0 B/s</span></span>
  <span>Last error: <span id="f-lasterr">never</span></span>
  <span class="spacer"></span>
  <button class="btn-ghost" type="button" id="refresh-btn">
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none"
         stroke="currentColor" stroke-width="2"
         stroke-linecap="round" stroke-linejoin="round">
      <path d="M21 12a9 9 0 1 1-3-6.7"></path>
      <polyline points="21 3 21 9 15 9"></polyline>
    </svg>
    Refresh
  </button>
</footer>

<script>
  const API = "__API_URL__";
  const $ = (id) => document.getElementById(id);

  function setText(id, v) { const el = $(id); if (el) el.textContent = v; }

  function drawSpark(id, values) {
    const svg = $(id);
    if (!svg) return;
    svg.innerHTML = "";
    if (!values || values.length < 2) return;
    const w = 100, h = 28;
    const max = Math.max(...values, 1);
    const step = w / (values.length - 1);
    const pts = values.map((v, i) => {
      const y = h - (v / max) * (h - 4) - 2;
      return i * step + "," + y;
    }).join(" ");
    const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", "currentColor");
    line.setAttribute("stroke-width", "1.2");
    line.setAttribute("points", pts);
    svg.appendChild(line);
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function renderActivity(rows) {
    const tbody = $("activity-body");
    if (!tbody) return;
    if (!rows || rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" style="color:var(--text-muted);padding:18px 8px">No requests yet — traffic will appear here.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td style="color:var(--text-muted)">${escapeHtml(r.time)}</td>
        <td class="mono">${escapeHtml(r.operation)}</td>
        <td>${escapeHtml(r.bucket)}</td>
        <td class="truncate mono" title="${escapeHtml(r.object)}">${escapeHtml(r.object)}</td>
        <td><span class="pill ${r.status === "Success" ? "ok" : "err"}">${escapeHtml(r.status)}</span></td>
        <td>${escapeHtml(r.size)}</td>
        <td class="mono">${escapeHtml(r.client_ip)}</td>
        <td>${escapeHtml(r.latency)}</td>
      </tr>
    `).join("");
  }

  function renderBuckets(rows) {
    const tbody = $("buckets-body");
    if (!tbody) return;
    if (!rows || rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" style="color:var(--text-muted);padding:14px 8px">No buckets observed yet.</td></tr>';
      return;
    }
    const lock = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="11" width="16" height="10" rx="2"></rect><path d="M8 11V7a4 4 0 0 1 8 0v4"></path></svg>';
    tbody.innerHTML = rows.map(b => `
      <tr>
        <td>${escapeHtml(b.name)}</td>
        <td><span class="enc-cell ${b.encrypted ? "on" : "off"}">${lock}${b.encrypted ? "Encrypted" : "Not Encrypted"}</span></td>
        <td>${escapeHtml(b.objects)}</td>
        <td>${escapeHtml(b.size)}</td>
      </tr>
    `).join("");
  }

  function renderKeys(rows) {
    const tbody = $("keys-body");
    if (!tbody) return;
    tbody.innerHTML = (rows || []).map(k => `
      <tr>
        <td class="mono">${escapeHtml(k.id)}</td>
        <td>${escapeHtml(k.type)}</td>
        <td><span class="pill ok">${escapeHtml(k.status)}</span></td>
        <td>${escapeHtml(k.created)}</td>
      </tr>
    `).join("");
  }

  async function refresh() {
    try {
      const r = await fetch(API, {credentials: "same-origin"});
      if (!r.ok) return;
      const d = await r.json();

      setText("h-title", d.header.title);
      setText("h-status", d.header.status);
      setText("h-uptime", d.header.uptime);

      const c = d.cards;
      setText("c1-label", c.requests.label + " (24h)");
      setText("c1-value", c.requests.value);
      setText("c1-unit", c.requests.unit);
      drawSpark("c1-spark", c.requests.spark);

      setText("c2-label", c.data_encrypted.label + " (24h)");
      setText("c2-value", c.data_encrypted.value);
      setText("c2-unit", c.data_encrypted.unit);
      drawSpark("c2-spark", c.data_encrypted.spark);

      setText("c3-label", c.errors.label + " (24h)");
      setText("c3-value", c.errors.value);
      setText("c3-unit", c.errors.unit);
      drawSpark("c3-spark", c.errors.spark);

      setText("c4-label", c.active_buckets.label);
      setText("c4-value", c.active_buckets.value);
      setText("c4-delta", c.active_buckets.detail || "");

      renderActivity(d.activity);
      renderBuckets(d.buckets);
      renderKeys(d.keys);

      setText("f-version", "v" + d.footer.version);
      setText("f-rps", d.footer.req_per_s);
      setText("f-throughput", d.footer.throughput);
      setText("f-lasterr", d.footer.last_error);
    } catch (e) {
      // swallow; next tick will retry
    }
  }

  document.getElementById("refresh-btn").addEventListener("click", refresh);
  refresh();
  setInterval(refresh, 5000);
</script>
</body>
</html>
"""


def render_dashboard(admin_path: str = "/admin") -> str:
    """Return the dashboard HTML with the API URL substituted."""
    api_url = admin_path.rstrip("/") + "/api/status"
    return _DASHBOARD_HTML.replace("__API_URL__", api_url)
