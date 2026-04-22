"""HTML templates for the admin dashboard — login + dashboard."""

from __future__ import annotations

from html import escape as _esc

_SHARED_CSS = """
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
  a { color: inherit; }
  .brand-mark {
    width: 40px; height: 40px;
    background: var(--dark);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }
  .brand-mark svg { color: #fff; }
  .brand-name { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; }
  .btn-dark {
    background: #111827; color: #fff;
    border: none; border-radius: 8px;
    padding: 8px 14px; font-size: 13px; font-weight: 500;
    cursor: pointer;
  }
  .btn-dark:hover { background: #1f2937; }
  .btn-dark:disabled { opacity: .6; cursor: not-allowed; }
  .btn-ghost {
    background: transparent; color: var(--text);
    border: 1px solid var(--border-strong);
    border-radius: 8px; padding: 5px 10px;
    font-size: 12px; cursor: pointer;
    display: inline-flex; align-items: center; gap: 4px;
  }
  .btn-ghost:hover { background: var(--icon-bg); }
"""


_LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sign in · S3 Encryption Proxy</title>
<style>
  __SHARED_CSS__
  .login-page {
    min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
    padding: 24px;
  }
  .login-card {
    width: 100%;
    max-width: 380px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 28px;
  }
  .login-head {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 20px;
  }
  .login-title { font-size: 18px; font-weight: 600; }
  .login-subtitle { color: var(--text-muted); font-size: 13px; margin-top: 2px; }
  .field { margin-bottom: 14px; }
  .field label {
    display: block; margin-bottom: 6px;
    font-size: 12px; font-weight: 500;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .field input {
    width: 100%;
    padding: 10px 12px;
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    font-size: 14px;
    background: #fff;
    font-family: inherit;
    color: var(--text);
    transition: border-color .15s, box-shadow .15s;
  }
  .field input:focus {
    outline: none;
    border-color: var(--dark);
    box-shadow: 0 0 0 3px rgba(17,24,39,0.08);
  }
  .login-error {
    background: var(--err-bg);
    color: var(--err);
    border: 1px solid rgba(239,68,68,0.2);
    border-radius: 8px;
    padding: 8px 12px;
    font-size: 13px;
    margin-bottom: 14px;
  }
  .login-submit { width: 100%; margin-top: 6px; }
  .login-hint {
    margin-top: 18px;
    font-size: 12px;
    color: var(--text-subtle);
    text-align: center;
  }
</style>
</head>
<body>
<main class="login-page">
  <form class="login-card" method="post" action="__LOGIN_ACTION__" autocomplete="on">
    <div class="login-head">
      <span class="brand-mark" aria-hidden="true">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
             stroke="currentColor" stroke-width="2"
             stroke-linecap="round" stroke-linejoin="round">
          <rect x="4" y="11" width="16" height="10" rx="2"></rect>
          <path d="M8 11V7a4 4 0 0 1 8 0v4"></path>
        </svg>
      </span>
      <div>
        <div class="login-title">S3 Encryption Proxy</div>
        <div class="login-subtitle">Sign in to the admin dashboard</div>
      </div>
    </div>

    __ERROR_BLOCK__

    <div class="field">
      <label for="u">Username</label>
      <input id="u" name="username" type="text" autocomplete="username" autofocus required>
    </div>
    <div class="field">
      <label for="p">Password</label>
      <input id="p" name="password" type="password" autocomplete="current-password" required>
    </div>

    <button type="submit" class="btn-dark login-submit">Sign in</button>
    <div class="login-hint">Credentials default to your AWS access key / secret.</div>
  </form>
</main>
</body>
</html>
"""


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>S3 Encryption Proxy</title>
<style>
  __SHARED_CSS__
  .page {
    max-width: 1080px;
    margin: 0 auto;
    padding: 32px 24px 96px 24px;
  }
  .app-head {
    display: flex; align-items: center; justify-content: space-between;
    gap: 16px; padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 24px;
  }
  .brand { display: flex; align-items: center; gap: 12px; }
  .head-right { display: flex; align-items: center; gap: 14px; }
  .status-pill {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px;
    border: 1px solid var(--ok);
    border-radius: 999px;
    color: var(--ok);
    font-size: 13px; background: transparent;
  }
  .status-pill .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--ok);
  }
  .uptime { color: var(--text-muted); font-size: 13px; }
  .head-user {
    color: var(--text-muted); font-size: 12px;
    border-left: 1px solid var(--border);
    padding-left: 14px;
  }
  .head-user a {
    color: var(--text); text-decoration: none; margin-left: 6px;
  }
  .head-user a:hover { text-decoration: underline; }

  /* ---- Cards ---- */
  .cards {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 16px; margin-bottom: 20px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px;
    cursor: pointer;
    transition: border-color .15s, box-shadow .15s;
    position: relative;
    user-select: none;
  }
  .card:hover { border-color: var(--border-strong); }
  .card.open { border-color: var(--dark); box-shadow: 0 0 0 3px rgba(17,24,39,0.04); }
  .card-head {
    display: flex; align-items: center; justify-content: space-between;
    color: var(--text-muted); font-size: 13px;
    margin-bottom: 10px;
  }
  .card-head-left { display: flex; align-items: center; gap: 10px; }
  .card-icon {
    width: 30px; height: 30px;
    background: var(--icon-bg);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text);
  }
  .card-chevron {
    color: var(--text-subtle);
    transition: transform .15s;
  }
  .card.open .card-chevron { transform: rotate(180deg); color: var(--text); }
  .card-value {
    font-size: 34px; font-weight: 600;
    letter-spacing: -0.02em; line-height: 1.1;
  }
  .card-unit {
    font-size: 16px; font-weight: 500;
    color: var(--text-muted); margin-left: 4px;
  }
  .card-delta { margin-top: 6px; font-size: 12px; color: var(--text-muted); }
  .spark {
    margin-top: 12px; width: 100%; height: 28px;
    display: block; color: var(--text-subtle);
  }
  .card-expand {
    display: none;
    margin-top: 14px;
    padding-top: 14px;
    border-top: 1px dashed var(--border);
  }
  .card.open .card-expand { display: block; }
  .kv {
    display: flex; justify-content: space-between;
    padding: 6px 0;
    font-size: 13px;
    border-bottom: 1px solid var(--border);
  }
  .kv:last-child { border-bottom: none; }
  .kv .k { color: var(--text-muted); }
  .kv .v { font-variant-numeric: tabular-nums; }

  /* ---- Sections ---- */
  .section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px; margin-bottom: 20px;
  }
  .section-head {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 14px; gap: 12px;
  }
  .section-title { font-size: 16px; font-weight: 600; }
  .section-actions { display: flex; align-items: center; gap: 14px; }
  .live {
    display: inline-flex; align-items: center; gap: 6px;
    color: var(--text-muted); font-size: 13px;
  }
  .live .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--ok); box-shadow: 0 0 0 3px rgba(16,185,129,0.15);
  }
  .back-link {
    display: inline-flex; align-items: center; gap: 6px;
    color: var(--text); font-size: 13px;
    text-decoration: none; cursor: pointer;
  }
  .back-link:hover { text-decoration: underline; }

  /* ---- Tables ---- */
  table { width: 100%; border-collapse: collapse; }
  th, td {
    text-align: left; padding: 10px 8px;
    font-size: 13px; white-space: nowrap;
  }
  th {
    color: var(--text-muted); font-weight: 500;
    border-bottom: 1px solid var(--border);
  }
  td {
    border-bottom: 1px solid var(--border);
    color: var(--text);
  }
  tr:last-child td { border-bottom: none; }
  td.truncate {
    max-width: 220px;
    overflow: hidden; text-overflow: ellipsis;
  }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .pill {
    display: inline-block;
    padding: 2px 10px; border-radius: 999px;
    font-size: 12px; font-weight: 500;
  }
  .pill.ok  { background: var(--ok-bg);  color: var(--ok); }
  .pill.err { background: var(--err-bg); color: var(--err); }
  .enc-cell { display: inline-flex; align-items: center; gap: 6px; }
  .enc-cell.on  { color: var(--ok); }
  .enc-cell.off { color: var(--text-muted); }
  .linkish {
    color: var(--text); text-decoration: none;
    border-bottom: 1px dashed var(--text-subtle);
    cursor: pointer;
  }
  .linkish:hover {
    border-bottom-color: var(--text);
    color: #000;
  }

  .split {
    display: grid; grid-template-columns: 1fr 1fr; gap: 20px;
  }

  /* ---- Detail views ---- */
  .detail-sub { color: var(--text-muted); font-size: 13px; margin-top: 2px; }
  .empty-state {
    color: var(--text-muted); font-size: 13px;
    padding: 24px; text-align: center;
  }

  /* ---- Footer ---- */
  .footer {
    position: fixed; left: 0; right: 0; bottom: 0;
    border-top: 1px solid var(--border);
    background: var(--surface);
    padding: 10px 24px;
    font-size: 12px; color: var(--text-muted);
    display: flex; align-items: center; gap: 28px;
  }
  .footer .brand-mini { display: inline-flex; align-items: center; gap: 6px; }
  .footer .spacer { flex: 1; }

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
      <span class="head-user">Signed in · <a href="__LOGOUT_URL__">Logout</a></span>
    </div>
  </header>

  <!-- ================== DASHBOARD VIEW ================== -->
  <div id="view-dashboard">
    <section class="cards">
      __CARD_REQUESTS__
      __CARD_DATA__
      __CARD_ERRORS__
      __CARD_BUCKETS__
    </section>

    <section class="section">
      <div class="section-head">
        <div class="section-title">Recent Activity</div>
        <div class="section-actions">
          <span class="live"><span class="dot"></span>Live</span>
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
          <tr><td colspan="8" class="empty-state">No requests yet — traffic will appear here.</td></tr>
        </tbody>
      </table>
    </section>

    <section class="split">
      <div class="section" style="margin-bottom:0">
        <div class="section-head">
          <div class="section-title">Buckets</div>
        </div>
        <table>
          <thead>
            <tr><th>Name</th><th>Encryption</th><th>Objects</th><th>Size</th></tr>
          </thead>
          <tbody id="buckets-body">
            <tr><td colspan="4" class="empty-state">No buckets observed yet.</td></tr>
          </tbody>
        </table>
      </div>

      <div class="section" style="margin-bottom:0">
        <div class="section-head">
          <div class="section-title">Keys</div>
        </div>
        <table>
          <thead>
            <tr><th>Key ID</th><th>Type</th><th>Status</th><th>Created</th></tr>
          </thead>
          <tbody id="keys-body"></tbody>
        </table>
      </div>
    </section>
  </div>

  <!-- ================== BUCKET DETAIL VIEW ================== -->
  <div id="view-bucket" style="display:none">
    <section class="section">
      <div class="section-head">
        <div>
          <a class="back-link" data-goto="">← Back to dashboard</a>
          <div class="section-title" style="margin-top:6px" id="bv-title">Bucket</div>
          <div class="detail-sub" id="bv-sub">—</div>
        </div>
      </div>
      <table>
        <thead>
          <tr><th>Key</th><th>Size</th><th>Last Modified</th></tr>
        </thead>
        <tbody id="bv-body">
          <tr><td colspan="3" class="empty-state">Loading…</td></tr>
        </tbody>
      </table>
    </section>
  </div>

  <!-- ================== OBJECT DETAIL VIEW ================== -->
  <div id="view-object" style="display:none">
    <section class="section">
      <div class="section-head">
        <div>
          <a class="back-link" id="ov-back" data-goto="">← Back</a>
          <div class="section-title" style="margin-top:6px" id="ov-title">Object</div>
          <div class="detail-sub mono" id="ov-sub">—</div>
        </div>
      </div>
      <table>
        <tbody id="ov-body">
          <tr><td colspan="2" class="empty-state">Loading…</td></tr>
        </tbody>
      </table>
    </section>
  </div>

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
  const API_STATUS = "__STATUS_URL__";
  const API_BUCKET = "__BUCKET_URL__";      // expects /bucket appended
  const API_OBJECT = "__OBJECT_URL__";      // expects /bucket/key appended
  const $ = (id) => document.getElementById(id);

  function setText(id, v) { const el = $(id); if (el) el.textContent = v; }
  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

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

  // ------------------- Hash routing: #, #bucket=X, #bucket=X&object=Y -------------------
  function parseHash() {
    const h = (location.hash || "").replace(/^#/, "");
    if (!h) return {view: "dashboard"};
    const params = new URLSearchParams(h);
    const bucket = params.get("bucket");
    const object = params.get("object");
    if (bucket && object) return {view: "object", bucket, object};
    if (bucket) return {view: "bucket", bucket};
    return {view: "dashboard"};
  }

  function showView(name) {
    for (const v of ["dashboard", "bucket", "object"]) {
      const el = $("view-" + v);
      if (el) el.style.display = (v === name) ? "" : "none";
    }
  }

  async function navigateFromHash() {
    const route = parseHash();
    showView(route.view);
    if (route.view === "bucket")  await loadBucket(route.bucket);
    if (route.view === "object")  await loadObject(route.bucket, route.object);
  }

  function gotoDashboard() { location.hash = ""; }
  function gotoBucket(bucket)         { location.hash = "bucket=" + encodeURIComponent(bucket); }
  function gotoObject(bucket, object) {
    location.hash = "bucket=" + encodeURIComponent(bucket) + "&object=" + encodeURIComponent(object);
  }

  // ------------------- Dashboard rendering -------------------
  function renderCard(prefix, card) {
    setText(prefix + "-label", card.label + (prefix === "c4" ? "" : " (24h)"));
    setText(prefix + "-value", card.value);
    setText(prefix + "-unit", card.unit || "");
    if (card.spark !== undefined) drawSpark(prefix + "-spark", card.spark);
    setText(prefix + "-delta", card.detail || "");

    const expand = $(prefix + "-expand");
    if (expand && card.breakdown) {
      expand.innerHTML = card.breakdown.length === 0
        ? '<div class="empty-state" style="padding:8px 0">Nothing to show.</div>'
        : card.breakdown.map(b =>
            `<div class="kv"><span class="k">${escapeHtml(b.label)}</span><span class="v">${escapeHtml(b.value)}</span></div>`
          ).join("");
    }
  }

  function renderActivity(rows) {
    const tbody = $("activity-body");
    if (!tbody) return;
    if (!rows || rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="empty-state">No requests yet — traffic will appear here.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => {
      const hasBucket = r.bucket && r.bucket !== "—";
      const hasObject = hasBucket && r.object && r.object !== "—";
      const bucketCell = hasBucket
        ? `<a class="linkish" href="#bucket=${encodeURIComponent(r.bucket)}">${escapeHtml(r.bucket)}</a>`
        : escapeHtml(r.bucket);
      const objectCell = hasObject
        ? `<a class="linkish mono" href="#bucket=${encodeURIComponent(r.bucket)}&object=${encodeURIComponent(r.object)}" title="${escapeHtml(r.object)}">${escapeHtml(r.object)}</a>`
        : `<span class="mono">${escapeHtml(r.object)}</span>`;
      return `
        <tr>
          <td style="color:var(--text-muted)">${escapeHtml(r.time)}</td>
          <td class="mono">${escapeHtml(r.operation)}</td>
          <td>${bucketCell}</td>
          <td class="truncate">${objectCell}</td>
          <td><span class="pill ${r.status === "Success" ? "ok" : "err"}">${escapeHtml(r.status)}</span></td>
          <td>${escapeHtml(r.size)}</td>
          <td class="mono">${escapeHtml(r.client_ip)}</td>
          <td>${escapeHtml(r.latency)}</td>
        </tr>`;
    }).join("");
  }

  function renderBuckets(rows) {
    const tbody = $("buckets-body");
    if (!tbody) return;
    if (!rows || rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No buckets observed yet.</td></tr>';
      return;
    }
    const lock = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="11" width="16" height="10" rx="2"></rect><path d="M8 11V7a4 4 0 0 1 8 0v4"></path></svg>';
    tbody.innerHTML = rows.map(b => `
      <tr>
        <td><a class="linkish" href="#bucket=${encodeURIComponent(b.name)}">${escapeHtml(b.name)}</a></td>
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
      const r = await fetch(API_STATUS, {credentials: "same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) return;
      const d = await r.json();
      setText("h-title", d.header.title);
      setText("h-status", d.header.status);
      setText("h-uptime", d.header.uptime);
      renderCard("c1", d.cards.requests);
      renderCard("c2", d.cards.data_encrypted);
      renderCard("c3", d.cards.errors);
      renderCard("c4", d.cards.active_buckets);
      renderActivity(d.activity);
      renderBuckets(d.buckets);
      renderKeys(d.keys);
      setText("f-version", "v" + d.footer.version);
      setText("f-rps", d.footer.req_per_s);
      setText("f-throughput", d.footer.throughput);
      setText("f-lasterr", d.footer.last_error);
    } catch (e) { /* retry next tick */ }
  }

  // ------------------- Bucket detail -------------------
  async function loadBucket(bucket) {
    setText("bv-title", bucket);
    setText("bv-sub", "Listing objects…");
    const tbody = $("bv-body");
    tbody.innerHTML = '<tr><td colspan="3" class="empty-state">Loading…</td></tr>';
    try {
      const r = await fetch(API_BUCKET + "/" + encodeURIComponent(bucket), {credentials:"same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) {
        tbody.innerHTML = `<tr><td colspan="3" class="empty-state">Failed to load: ${r.status}</td></tr>`;
        return;
      }
      const d = await r.json();
      setText("bv-sub", `${d.count} object${d.count === 1 ? "" : "s"}${d.is_truncated ? " (truncated — showing first 500)" : ""}`);
      if (!d.objects.length) {
        tbody.innerHTML = '<tr><td colspan="3" class="empty-state">No objects.</td></tr>';
        return;
      }
      tbody.innerHTML = d.objects.map(o => `
        <tr>
          <td><a class="linkish mono" href="#bucket=${encodeURIComponent(bucket)}&object=${encodeURIComponent(o.key)}">${escapeHtml(o.key)}</a></td>
          <td>${escapeHtml(o.size_h)}</td>
          <td class="mono" style="color:var(--text-muted)">${escapeHtml(o.last_modified)}</td>
        </tr>
      `).join("");
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="3" class="empty-state">Network error.</td></tr>';
    }
  }

  // ------------------- Object detail -------------------
  async function loadObject(bucket, object) {
    setText("ov-title", object.split("/").pop() || object);
    setText("ov-sub", bucket + " / " + object);
    $("ov-back").setAttribute("href", "#bucket=" + encodeURIComponent(bucket));
    const tbody = $("ov-body");
    tbody.innerHTML = '<tr><td colspan="2" class="empty-state">Loading…</td></tr>';
    try {
      const url = API_OBJECT + "/" + encodeURIComponent(bucket) + "/" + encodeURI(object);
      const r = await fetch(url, {credentials: "same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) {
        tbody.innerHTML = `<tr><td colspan="2" class="empty-state">Failed to load: ${r.status}</td></tr>`;
        return;
      }
      const d = await r.json();
      const rows = [
        ["Bucket", d.bucket],
        ["Key", d.key],
        ["Size (stored)", d.size_h],
        ["Content-Type", d.content_type || "—"],
        ["ETag", d.etag || "—"],
        ["Last Modified", d.last_modified || "—"],
        ["Encrypted", d.encrypted ? "Yes (AES-256-GCM)" : "No"],
      ];
      for (const [k, v] of Object.entries(d.metadata || {})) {
        rows.push(["x-amz-meta-" + k, v]);
      }
      tbody.innerHTML = rows.map(([k, v]) =>
        `<tr><td style="color:var(--text-muted);width:200px">${escapeHtml(k)}</td><td class="mono">${escapeHtml(v)}</td></tr>`
      ).join("");
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="2" class="empty-state">Network error.</td></tr>';
    }
  }

  // ------------------- Wire up ------------------
  document.querySelectorAll(".card").forEach(card => {
    card.addEventListener("click", () => card.classList.toggle("open"));
  });
  document.querySelectorAll("[data-goto]").forEach(el => {
    el.addEventListener("click", (e) => { e.preventDefault(); gotoDashboard(); });
  });
  $("refresh-btn").addEventListener("click", refresh);
  window.addEventListener("hashchange", navigateFromHash);

  refresh();
  navigateFromHash();
  setInterval(() => {
    // Only refresh dashboard while we're actually on it
    if (parseHash().view === "dashboard") refresh();
  }, 5000);
</script>
</body>
</html>
"""


def _card_html(num: str, label: str, icon_svg: str) -> str:
    return f"""
      <div class="card" data-card="{num}">
        <div class="card-head">
          <div class="card-head-left">
            <span class="card-icon">{icon_svg}</span>
            <span id="c{num}-label">{label}</span>
          </div>
          <svg class="card-chevron" width="14" height="14" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="6 9 12 15 18 9"></polyline>
          </svg>
        </div>
        <div><span class="card-value" id="c{num}-value">—</span><span class="card-unit" id="c{num}-unit"></span></div>
        <div class="card-delta" id="c{num}-delta">&nbsp;</div>
        <svg class="spark" id="c{num}-spark" viewBox="0 0 100 28" preserveAspectRatio="none"></svg>
        <div class="card-expand" id="c{num}-expand"></div>
      </div>
    """


_ICON_REQUESTS = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
    ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M7 7h11l-3-3"/><path d="M17 17H6l3 3"/></svg>'
)
_ICON_LOCK = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
    ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg>'
)
_ICON_ERR = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
    ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M10.3 3.9 2 18a2 2 0 0 0 1.7 3h16.6A2 2 0 0 0 22 18L13.7 3.9a2 2 0 0 0-3.4 0z"/>'
    '<line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>'
)
_ICON_BUCKET = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
    ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M4 8c0-2.2 3.6-4 8-4s8 1.8 8 4-3.6 4-8 4-8-1.8-8-4z"/>'
    '<path d="M4 8v8c0 2.2 3.6 4 8 4s8-1.8 8-4V8"/></svg>'
)


def render_dashboard(admin_path: str = "/admin") -> str:
    """Render the dashboard HTML with API URLs + logout link substituted."""
    prefix = admin_path.rstrip("/")
    html = _DASHBOARD_HTML
    html = html.replace("__SHARED_CSS__", _SHARED_CSS)
    html = html.replace("__STATUS_URL__", f"{prefix}/api/status")
    html = html.replace("__BUCKET_URL__", f"{prefix}/api/buckets")
    html = html.replace("__OBJECT_URL__", f"{prefix}/api/objects")
    html = html.replace("__LOGIN_URL__", f"{prefix}/login")
    html = html.replace("__LOGOUT_URL__", f"{prefix}/logout")
    html = html.replace("__CARD_REQUESTS__", _card_html("1", "Requests", _ICON_REQUESTS))
    html = html.replace("__CARD_DATA__", _card_html("2", "Data Encrypted", _ICON_LOCK))
    html = html.replace("__CARD_ERRORS__", _card_html("3", "Errors", _ICON_ERR))
    html = html.replace("__CARD_BUCKETS__", _card_html("4", "Active Buckets", _ICON_BUCKET))
    return html


def render_login(admin_path: str = "/admin", error: str | None = None) -> str:
    """Render the sign-in page."""
    prefix = admin_path.rstrip("/")
    error_block = (
        f'<div class="login-error">{_esc(_login_error_text(error))}</div>' if error else ""
    )
    html = _LOGIN_HTML
    html = html.replace("__SHARED_CSS__", _SHARED_CSS)
    html = html.replace("__LOGIN_ACTION__", f"{prefix}/login")
    html = html.replace("__ERROR_BLOCK__", error_block)
    return html


def _login_error_text(err: str) -> str:
    mapping = {"1": "Invalid username or password."}
    return mapping.get(err, "Sign in failed. Please try again.")
