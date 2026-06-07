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
    --accent: #2563eb;
    --accent-soft: rgba(37,99,235,0.12);
    --accent-softer: rgba(37,99,235,0.04);
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
    display: block;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px;
    cursor: pointer;
    transition: border-color .15s, box-shadow .15s, transform .15s;
    position: relative;
    user-select: none;
    text-decoration: none;
    color: inherit;
  }
  .card:hover {
    border-color: var(--border-strong);
    transform: translateY(-1px);
    box-shadow: 0 2px 8px rgba(17,24,39,0.04);
  }
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
  .card-chevron { color: var(--text-subtle); }
  .card:hover .card-chevron { color: var(--text); }
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
  .chart-block {
    display: flex; flex-direction: column;
    gap: 10px;
  }
  .chart-header {
    display: flex; align-items: baseline; justify-content: space-between;
    gap: 12px;
  }
  .chart-title {
    font-size: 13px; font-weight: 500;
    color: var(--text);
  }
  .chart-subtle {
    font-size: 12px; color: var(--text-muted);
    font-variant-numeric: tabular-nums;
  }
  .chart-wrap {
    position: relative;
    width: 100%;
    height: 280px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 14px 10px 14px;
  }
  .chart-wrap--big {
    height: 420px;
    padding: 18px 18px 14px 18px;
  }
  .chart-wrap svg {
    width: 100%; height: 100%;
    display: block;
    font-family: inherit;
  }
  .chart-wrap .axis line,
  .chart-wrap .axis path { stroke: var(--border-strong); fill: none; }
  .chart-wrap .axis text {
    fill: var(--text-muted);
    font-size: 10px;
    font-variant-numeric: tabular-nums;
    font-family: inherit;
  }
  .chart-wrap .axis-label {
    fill: var(--text-subtle);
    font-size: 10px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .chart-wrap .grid line {
    stroke: #eef0f3;
    stroke-width: 1;
  }
  .chart-wrap .series {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-linejoin: round;
    stroke-linecap: round;
  }
  .chart-wrap .series-dot {
    fill: var(--accent);
    opacity: 0;
  }
  .chart-wrap:hover .series-dot { opacity: 1; }
  .chart-wrap .series-fill { fill: url(#chartGradient); stroke: none; }
  .chart-wrap .cursor-line {
    stroke: var(--text-muted);
    stroke-width: 1;
    stroke-dasharray: 3 3;
    opacity: 0;
  }
  .chart-wrap .cursor-dot {
    fill: #fff;
    stroke: var(--accent);
    stroke-width: 2;
    opacity: 0;
  }
  .chart-tooltip {
    position: absolute;
    background: var(--dark);
    color: #fff;
    font-size: 12px;
    padding: 7px 10px;
    border-radius: 8px;
    pointer-events: none;
    transform: translate(-50%, calc(-100% - 10px));
    white-space: nowrap;
    display: none;
    font-variant-numeric: tabular-nums;
    box-shadow: 0 4px 14px rgba(17,24,39,0.15);
  }
  .chart-tooltip::after {
    content: "";
    position: absolute;
    bottom: -4px; left: 50%;
    transform: translateX(-50%) rotate(45deg);
    width: 8px; height: 8px;
    background: var(--dark);
  }
  .chart-tooltip .tip-v {
    font-size: 14px; font-weight: 600;
    display: block;
  }
  .chart-tooltip .tip-t {
    font-size: 11px; opacity: 0.7;
    display: block; margin-top: 2px;
  }
  .kv {
    display: flex; justify-content: space-between;
    padding: 9px 0;
    font-size: 13px;
    border-bottom: 1px solid var(--border);
    gap: 10px; min-width: 0;
  }
  .kv:last-child { border-bottom: none; }
  .kv .k { color: var(--text-muted); overflow: hidden; text-overflow: ellipsis; }
  .kv .v { font-variant-numeric: tabular-nums; flex-shrink: 0; }

  /* ---- Metric detail page ---- */
  .metric-head {
    margin-bottom: 20px;
  }
  .metric-head-text { min-width: 0; }
  .metric-hero-label {
    color: var(--text); font-size: 15px; font-weight: 600;
    margin-bottom: 4px;
  }
  .metric-hero-value {
    font-size: 40px; font-weight: 600;
    letter-spacing: -0.02em; line-height: 1.05;
  }
  .metric-hero-value .card-unit { font-size: 18px; margin-left: 6px; }
  .metric-hero-delta {
    color: var(--text-muted); font-size: 12px; margin-top: 6px;
  }
  .metric-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.9fr) minmax(0, 1fr);
    gap: 28px;
    align-items: start;
  }
  .metric-chart-col { min-width: 0; }
  .metric-breakdown-col { min-width: 0; padding-top: 4px; }
  .breakdown-heading {
    margin: 0 0 12px 0;
    font-size: 13px; font-weight: 600;
    color: var(--text);
  }
  .breakdown-bars {
    display: flex; flex-direction: column;
    gap: 10px;
  }
  .bb-row { display: flex; flex-direction: column; gap: 5px; }
  .bb-head {
    display: flex; justify-content: space-between;
    align-items: baseline; gap: 10px;
    font-size: 13px;
  }
  .bb-label { color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .bb-value {
    color: var(--text); font-variant-numeric: tabular-nums;
    flex-shrink: 0;
  }
  .bb-percent {
    color: var(--text-muted); font-size: 12px;
    font-variant-numeric: tabular-nums;
    margin-left: 6px;
  }
  .bb-track {
    position: relative;
    height: 6px;
    background: var(--icon-bg);
    border-radius: 3px;
    overflow: hidden;
  }
  .bb-fill {
    position: absolute; left: 0; top: 0; bottom: 0;
    background: var(--dark);
    border-radius: 3px;
    transition: width .25s ease-out;
  }
  @media (max-width: 880px) {
    .metric-grid { grid-template-columns: 1fr; gap: 20px; }
  }

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

  /* ---- Explorer (list-style) ---- */
  .crumbs {
    display: flex; flex-wrap: wrap;
    align-items: center; gap: 4px;
    font-size: 13px;
    margin: 4px 0 16px 0;
    color: var(--text-muted);
  }
  .crumbs a {
    color: var(--text);
    text-decoration: none;
    padding: 3px 8px;
    border-radius: 6px;
    cursor: pointer;
  }
  .crumbs a:hover { background: var(--icon-bg); }
  .crumbs .sep { color: var(--text-subtle); padding: 0 2px; }
  .crumbs .curr {
    font-weight: 500; color: var(--text);
    padding: 3px 8px;
  }

  .explorer { width: 100%; border-collapse: collapse; }
  .explorer th, .explorer td {
    text-align: left; padding: 9px 10px;
    font-size: 13px;
    border-bottom: 1px solid var(--border);
  }
  .explorer th {
    color: var(--text-muted); font-weight: 500;
    background: #fafbfc; position: sticky; top: 0;
  }
  .explorer tr:last-child td { border-bottom: none; }
  .explorer tr.row { cursor: pointer; }
  .explorer tr.row:hover td { background: var(--icon-bg); }
  .explorer td.col-name {
    display: flex; align-items: center; gap: 10px;
    min-width: 0;
  }
  .explorer td.col-name a {
    color: var(--text);
    text-decoration: none;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    flex: 1; min-width: 0;
  }
  .explorer td.col-name a:hover { text-decoration: underline; }
  .explorer .row-icon {
    width: 18px; height: 18px;
    display: inline-flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }
  .explorer .row-icon.folder { color: #eab308; }
  .explorer .row-icon.file { color: var(--text-subtle); }
  .explorer td.col-size,
  .explorer td.col-modified {
    color: var(--text-muted);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }
  .explorer td.col-size { text-align: right; }

  /* ---- Logs view ---- */
  .logs-toolbar {
    display: flex; flex-wrap: wrap;
    gap: 8px; margin-bottom: 14px;
    align-items: center;
  }
  .logs-toolbar input[type="search"],
  .logs-toolbar select {
    padding: 7px 10px;
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    font-size: 13px;
    background: #fff;
    color: var(--text);
    font-family: inherit;
  }
  .logs-toolbar input[type="search"] {
    flex: 1 1 260px; min-width: 160px;
  }
  .logs-toolbar input[type="search"]:focus,
  .logs-toolbar select:focus {
    outline: none;
    border-color: var(--dark);
    box-shadow: 0 0 0 3px rgba(17,24,39,0.06);
  }
  .logs-count { color: var(--text-muted); font-size: 12px; margin-left: auto; }

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
          <a class="btn-ghost" href="#logs">View all logs →</a>
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

  <!-- ================== BUCKET DETAIL VIEW (Finder-style) ================== -->
  <div id="view-bucket" style="display:none">
    <section class="section">
      <div class="section-head">
        <div style="min-width:0;flex:1">
          <a class="back-link" data-goto="">← Back to dashboard</a>
          <div class="section-title" style="margin-top:6px" id="bv-title">Bucket</div>
          <div class="crumbs" id="bv-crumbs"></div>
          <div class="detail-sub" id="bv-sub">—</div>
        </div>
        <div class="section-actions">
          <span class="live"><span class="dot"></span>Live</span>
        </div>
      </div>
      <table class="explorer">
        <thead>
          <tr>
            <th>Name</th>
            <th style="width: 110px; text-align: right;">Size</th>
            <th style="width: 180px;">Last Modified</th>
          </tr>
        </thead>
        <tbody id="bv-body">
          <tr><td colspan="3" class="empty-state">Loading…</td></tr>
        </tbody>
      </table>
    </section>
  </div>

  <!-- ================== LOGS VIEW ================== -->
  <div id="view-logs" style="display:none">
    <section class="section">
      <div class="section-head">
        <div>
          <a class="back-link" data-goto="">← Back to dashboard</a>
          <div class="section-title" style="margin-top:6px">Request Logs</div>
          <div class="detail-sub" id="lv-sub">—</div>
        </div>
        <div class="section-actions">
          <span class="live"><span class="dot"></span>Live</span>
        </div>
      </div>
      <div class="logs-toolbar">
        <input id="lv-q" type="search" placeholder="Search bucket, key, IP, method, status…" autocomplete="off">
        <select id="lv-op">
          <option value="">All operations</option>
        </select>
        <select id="lv-status">
          <option value="">All statuses</option>
          <option value="success">Success</option>
          <option value="error">Error</option>
        </select>
        <span class="logs-count" id="lv-count"></span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Time</th><th>Operation</th><th>Bucket</th><th>Object</th>
            <th>Status</th><th>Code</th><th>Size</th><th>Client IP</th><th>Latency</th>
          </tr>
        </thead>
        <tbody id="lv-body">
          <tr><td colspan="9" class="empty-state">Loading…</td></tr>
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

  <!-- ================== METRIC DETAIL VIEW ================== -->
  <div id="view-metric" style="display:none">
    <a class="back-link" data-goto="" style="display:inline-block;margin-bottom:12px">← Back to dashboard</a>
    <section class="section">
      <div class="metric-head">
        <div class="metric-head-text">
          <div class="metric-hero-label" id="mv-label">—</div>
          <div class="metric-hero-value">
            <span id="mv-value">—</span><span class="card-unit" id="mv-unit"></span>
          </div>
          <div class="metric-hero-delta" id="mv-delta">&nbsp;</div>
        </div>
      </div>
      <div class="metric-grid">
        <div class="metric-chart-col">
          <div class="chart-header">
            <span class="chart-title" id="m-charttitle">Over time</span>
            <span class="chart-subtle" id="m-chartmeta">&nbsp;</span>
          </div>
          <div class="chart-wrap chart-wrap--big" id="m-chartwrap">
            <svg id="m-chart" viewBox="0 0 600 260" preserveAspectRatio="none"></svg>
            <div class="chart-tooltip" id="m-tip"></div>
          </div>
        </div>
        <div class="metric-breakdown-col">
          <h4 class="breakdown-heading" id="m-breakdown-title">Breakdown</h4>
          <div id="m-expand" class="breakdown-bars"></div>
        </div>
      </div>
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
  const API_STREAM = "__STREAM_URL__";
  const API_BUCKET = "__BUCKET_URL__";      // expects /bucket appended
  const API_OBJECT = "__OBJECT_URL__";      // expects /bucket/key appended
  const API_LOGS   = "__LOGS_URL__";
  const VIEW_POLL_MS = 2000;
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

  // ------------------- Chart with X/Y axis and hover tooltip -------------------
  const SVG_NS = "http://www.w3.org/2000/svg";
  const CHART_STATE = {}; // per-card state for hover lookup
  const CHART_HOVER = {}; // prefix -> true while user's pointer is over the chart

  function formatNumber(v) {
    if (v >= 1e9) return (v / 1e9).toFixed(1) + "B";
    if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (v >= 1e3) return (v / 1e3).toFixed(1) + "k";
    if (Math.abs(v) < 1 && v !== 0) return v.toFixed(2);
    return Math.round(v).toString();
  }

  function niceCeil(x) {
    if (x <= 0) return 1;
    const exp = Math.floor(Math.log10(x));
    const base = Math.pow(10, exp);
    const norm = x / base;
    let step;
    if (norm <= 1) step = 1;
    else if (norm <= 2) step = 2;
    else if (norm <= 2.5) step = 2.5;
    else if (norm <= 5) step = 5;
    else step = 10;
    return step * base;
  }

  function niceTicks(maxV, target = 5) {
    const niceMax = niceCeil(maxV);
    const rawStep = niceMax / target;
    const exp = Math.floor(Math.log10(rawStep || 1));
    const base = Math.pow(10, exp);
    const norm = rawStep / base;
    let step;
    if (norm <= 1) step = 1 * base;
    else if (norm <= 2) step = 2 * base;
    else if (norm <= 2.5) step = 2.5 * base;
    else if (norm <= 5) step = 5 * base;
    else step = 10 * base;
    const ticks = [];
    for (let v = 0; v <= niceMax + 1e-9; v += step) ticks.push(v);
    return {ticks, niceMax};
  }

  function formatTime(epochSeconds) {
    const d = new Date(epochSeconds * 1000);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }

  function drawChart(prefix, values, times, unit, yLabel) {
    const svg = $(prefix + "-chart");
    const wrap = $(prefix + "-chartwrap");
    const tip = $(prefix + "-tip");
    if (!svg || !wrap || !tip) return;
    svg.innerHTML = "";

    const W = 600, H = 260;
    const PAD_L = 52, PAD_R = 14, PAD_T = 16, PAD_B = 30;
    const plotW = W - PAD_L - PAD_R;
    const plotH = H - PAD_T - PAD_B;

    const vals = (values && values.length) ? values.slice() : [];
    const ts   = (times && times.length === vals.length) ? times.slice() : [];
    const n = vals.length;

    // Gradient defs
    const defs = document.createElementNS(SVG_NS, "defs");
    defs.innerHTML =
      '<linearGradient id="chartGradient" x1="0" x2="0" y1="0" y2="1">' +
      '<stop offset="0%" stop-color="#2563eb" stop-opacity="0.22"/>' +
      '<stop offset="100%" stop-color="#2563eb" stop-opacity="0"/>' +
      '</linearGradient>';
    svg.appendChild(defs);

    if (n < 2 || !ts.length) {
      const msg = document.createElementNS(SVG_NS, "text");
      msg.setAttribute("x", W / 2);
      msg.setAttribute("y", H / 2);
      msg.setAttribute("text-anchor", "middle");
      msg.setAttribute("fill", "#9ca3af");
      msg.setAttribute("font-size", "12");
      msg.textContent = "Collecting data… (points appear as traffic flows)";
      svg.appendChild(msg);
      CHART_STATE[prefix] = null;
      setText(prefix + "-chartmeta", "");
      return;
    }

    // Y scale with nice ticks
    const rawMax = Math.max(...vals, 1);
    const {ticks, niceMax} = niceTicks(rawMax, 5);

    // X scale over time
    const tMin = ts[0];
    const tMax = ts[n - 1];
    const tSpan = Math.max(1, tMax - tMin);

    const xFor = (t) => PAD_L + ((t - tMin) / tSpan) * plotW;
    const yFor = (v) => PAD_T + plotH - (v / niceMax) * plotH;

    // Grid + Y axis labels
    const gGrid = document.createElementNS(SVG_NS, "g");
    gGrid.setAttribute("class", "grid");
    const gAxis = document.createElementNS(SVG_NS, "g");
    gAxis.setAttribute("class", "axis");

    for (const tv of ticks) {
      const y = yFor(tv);
      const line = document.createElementNS(SVG_NS, "line");
      line.setAttribute("x1", PAD_L); line.setAttribute("x2", PAD_L + plotW);
      line.setAttribute("y1", y); line.setAttribute("y2", y);
      gGrid.appendChild(line);
      const lbl = document.createElementNS(SVG_NS, "text");
      lbl.setAttribute("x", PAD_L - 8);
      lbl.setAttribute("y", y + 3);
      lbl.setAttribute("text-anchor", "end");
      lbl.textContent = formatNumber(tv);
      gAxis.appendChild(lbl);
    }

    // X axis baseline
    const xLine = document.createElementNS(SVG_NS, "line");
    xLine.setAttribute("x1", PAD_L); xLine.setAttribute("x2", PAD_L + plotW);
    xLine.setAttribute("y1", PAD_T + plotH); xLine.setAttribute("y2", PAD_T + plotH);
    gAxis.appendChild(xLine);

    // X labels: pick ~5 positions spread across the range, at real sample indexes
    const desired = 5;
    const xIdxs = [];
    for (let i = 0; i < desired; i++) {
      const idx = Math.round((i * (n - 1)) / (desired - 1));
      if (xIdxs[xIdxs.length - 1] !== idx) xIdxs.push(idx);
    }
    for (let i = 0; i < xIdxs.length; i++) {
      const idx = xIdxs[i];
      const x = xFor(ts[idx]);
      const lbl = document.createElementNS(SVG_NS, "text");
      const anchor = i === 0 ? "start" : (i === xIdxs.length - 1 ? "end" : "middle");
      lbl.setAttribute("x", x);
      lbl.setAttribute("y", PAD_T + plotH + 16);
      lbl.setAttribute("text-anchor", anchor);
      lbl.textContent = formatTime(ts[idx]);
      gAxis.appendChild(lbl);

      // tick mark
      const tick = document.createElementNS(SVG_NS, "line");
      tick.setAttribute("x1", x); tick.setAttribute("x2", x);
      tick.setAttribute("y1", PAD_T + plotH); tick.setAttribute("y2", PAD_T + plotH + 4);
      gAxis.appendChild(tick);
    }

    // Y-axis label
    if (yLabel) {
      const yl = document.createElementNS(SVG_NS, "text");
      yl.setAttribute("class", "axis-label");
      yl.setAttribute("x", PAD_L);
      yl.setAttribute("y", PAD_T - 6);
      yl.textContent = yLabel;
      svg.appendChild(yl);
    }

    svg.appendChild(gGrid);
    svg.appendChild(gAxis);

    // Area fill + line
    const ptsArr = vals.map((v, i) => [xFor(ts[i]), yFor(v)]);
    const pts = ptsArr.map((p) => p[0] + "," + p[1]).join(" ");
    const fillPts = `${PAD_L},${PAD_T + plotH} ${pts} ${PAD_L + plotW},${PAD_T + plotH}`;
    const area = document.createElementNS(SVG_NS, "polygon");
    area.setAttribute("class", "series-fill");
    area.setAttribute("points", fillPts);
    svg.appendChild(area);
    const line = document.createElementNS(SVG_NS, "polyline");
    line.setAttribute("class", "series");
    line.setAttribute("points", pts);
    svg.appendChild(line);

    // Cursor line + dot (hidden until hover)
    const cursor = document.createElementNS(SVG_NS, "line");
    cursor.setAttribute("class", "cursor-line");
    cursor.setAttribute("y1", PAD_T); cursor.setAttribute("y2", PAD_T + plotH);
    svg.appendChild(cursor);
    const dot = document.createElementNS(SVG_NS, "circle");
    dot.setAttribute("class", "cursor-dot");
    dot.setAttribute("r", "4");
    svg.appendChild(dot);

    CHART_STATE[prefix] = {
      values: vals, times: ts, unit: unit || "",
      geom: {W, H, PAD_L, PAD_R, PAD_T, PAD_B, plotW, plotH, n, xFor, yFor, tMin, tMax, tSpan, niceMax},
      cursor, dot, tip, wrap,
    };
    const lastV = vals[n - 1];
    setText(
      prefix + "-chartmeta",
      "peak " + formatNumber(rawMax) + (unit ? " " + unit : "") +
      " · latest " + formatNumber(lastV) + (unit ? " " + unit : "")
    );
  }

  function handleChartHover(prefix, evt) {
    CHART_HOVER[prefix] = true;
    const st = CHART_STATE[prefix];
    if (!st) return;
    const rect = st.wrap.getBoundingClientRect();
    const svgEl = st.wrap.querySelector("svg");
    const svgRect = svgEl.getBoundingClientRect();
    const xRel = evt.clientX - svgRect.left;
    // convert screen x to SVG x
    const xSvg = (xRel / svgRect.width) * st.geom.W;
    const { PAD_L, plotW, n, xFor, yFor, tMin, tSpan } = st.geom;
    const frac = Math.max(0, Math.min(1, (xSvg - PAD_L) / plotW));
    // pick the sample nearest to the hovered time
    const targetT = tMin + frac * tSpan;
    let idx = 0, best = Infinity;
    for (let i = 0; i < n; i++) {
      const d = Math.abs(st.times[i] - targetT);
      if (d < best) { best = d; idx = i; }
    }
    const v = st.values[idx];
    const cx = xFor(st.times[idx]);
    const cy = yFor(v);
    st.cursor.setAttribute("x1", cx); st.cursor.setAttribute("x2", cx);
    st.cursor.setAttribute("style", "opacity: 1");
    st.dot.setAttribute("cx", cx); st.dot.setAttribute("cy", cy);
    st.dot.setAttribute("style", "opacity: 1");
    const scaleX = svgRect.width / st.geom.W;
    const scaleY = svgRect.height / st.geom.H;
    const left = (cx * scaleX) + (svgRect.left - rect.left);
    const top  = (cy * scaleY) + (svgRect.top - rect.top);
    st.tip.style.display = "block";
    st.tip.style.left = left + "px";
    st.tip.style.top = top + "px";
    st.tip.innerHTML =
      `<span class="tip-v">${escapeHtml(formatNumber(v))}${st.unit ? " " + escapeHtml(st.unit) : ""}</span>` +
      `<span class="tip-t">${escapeHtml(formatTime(st.times[idx]))}</span>`;
  }

  function handleChartLeave(prefix) {
    CHART_HOVER[prefix] = false;
    const st = CHART_STATE[prefix];
    if (!st) return;
    st.cursor.setAttribute("style", "opacity: 0");
    st.dot.setAttribute("style", "opacity: 0");
    st.tip.style.display = "none";
  }

  // ------------------- Hash routing -------------------
  // #, #logs, #bucket=X, #bucket=X&prefix=P/, #bucket=X&object=Y
  function parseHash() {
    const h = (location.hash || "").replace(/^#/, "");
    if (!h) return {view: "dashboard"};
    if (h === "logs" || h.startsWith("logs&") || h.startsWith("logs?")) {
      const body = h.replace(/^logs[&?]?/, "");
      const params = new URLSearchParams(body);
      return {
        view: "logs",
        q: params.get("q") || "",
        op: params.get("op") || "",
        status: params.get("status") || "",
      };
    }
    if (h.startsWith("metric=")) {
      const key = h.substring("metric=".length);
      return {view: "metric", metric: key};
    }
    const params = new URLSearchParams(h);
    const bucket = params.get("bucket");
    const object = params.get("object");
    const prefix = params.get("prefix") || "";
    if (bucket && object) return {view: "object", bucket, object};
    if (bucket) return {view: "bucket", bucket, prefix};
    return {view: "dashboard"};
  }

  function showView(name) {
    for (const v of ["dashboard", "bucket", "object", "logs", "metric"]) {
      const el = $("view-" + v);
      if (el) el.style.display = (v === name) ? "" : "none";
    }
  }

  let currentRoute = {view: "dashboard"};
  let LAST_STATUS = null;

  async function navigateFromHash() {
    const route = parseHash();
    currentRoute = route;
    showView(route.view);
    if (route.view === "bucket")  await loadBucket(route.bucket, route.prefix || "");
    if (route.view === "object")  await loadObject(route.bucket, route.object);
    if (route.view === "logs")    await loadLogs();
    if (route.view === "metric") {
      if (LAST_STATUS && LAST_STATUS.cards[route.metric]) {
        // Force a redraw on view switch so the chart paints into its now-visible SVG.
        CHART_FP.m = null;
        BREAKDOWN_FP.m = null;
        renderMetric(route.metric, LAST_STATUS.cards[route.metric]);
      } else {
        await refresh();
      }
    }
  }

  function gotoDashboard() { location.hash = ""; }

  // ------------------- Dashboard rendering -------------------
  const CARD_FP = {};
  const BREAKDOWN_FP = {};
  const CHART_FP = {};

  function renderCard(prefix, card) {
    // Top-line metric (idempotent text writes don't flash).
    setText(prefix + "-label", card.label + (prefix === "c4" ? "" : " (1h)"));
    setText(prefix + "-value", card.value);
    setText(prefix + "-unit", card.unit || "");
    setText(prefix + "-delta", card.detail || "");

    // Sparkline: only redraw when data changes.
    if (card.spark !== undefined) {
      const sparkFp = JSON.stringify(card.spark);
      if (CARD_FP[prefix + ":spark"] !== sparkFp) {
        CARD_FP[prefix + ":spark"] = sparkFp;
        drawSpark(prefix + "-spark", card.spark);
      }
    }
  }

  const BREAKDOWN_TITLES = {
    requests: "Breakdown by method",
    data_encrypted: "Breakdown by direction",
    errors: "Breakdown by status code",
    active_buckets: "Breakdown by bucket",
  };

  function renderBreakdownBars(items) {
    if (!items || items.length === 0) {
      return '<div class="empty-state" style="padding:8px 0">Nothing to show yet.</div>';
    }
    const weights = items.map(b => typeof b.weight === "number" ? b.weight : 0);
    const max = Math.max(...weights, 0);
    const total = weights.reduce((a, b) => a + b, 0);
    return items.map((b, i) => {
      const w = weights[i];
      const pct = max > 0 ? (w / max) * 100 : 0;
      const share = total > 0 ? (w / total) * 100 : 0;
      const showPct = total > 0 && items.length > 1;
      return `
        <div class="bb-row">
          <div class="bb-head">
            <span class="bb-label">${escapeHtml(b.label)}</span>
            <span>
              <span class="bb-value">${escapeHtml(b.value)}</span>${showPct ? `<span class="bb-percent">${share.toFixed(1)}%</span>` : ""}
            </span>
          </div>
          <div class="bb-track"><div class="bb-fill" style="width:${pct.toFixed(1)}%"></div></div>
        </div>`;
    }).join("");
  }

  function renderMetric(key, card) {
    if (!card) return;
    setText("mv-label", card.label);
    setText("mv-value", card.value);
    setText("mv-unit", card.unit || "");
    setText("mv-delta", card.detail || "");
    setText("m-charttitle", card.label + " over time");
    setText("m-breakdown-title", BREAKDOWN_TITLES[key] || "Breakdown");

    // Full-axis chart: skip redraw if data identical, or if user is hovering.
    if (card.spark !== undefined) {
      const chartFp = JSON.stringify([card.spark, card.spark_times, card.unit, card.y_label]);
      if (CHART_FP.m !== chartFp && !CHART_HOVER.m) {
        CHART_FP.m = chartFp;
        drawChart("m", card.spark, card.spark_times || [], card.unit || "", card.y_label || "");
      }
    }

    // Breakdown list (proportional bars).
    if (card.breakdown) {
      const bFp = JSON.stringify(card.breakdown);
      if (BREAKDOWN_FP.m !== bFp) {
        BREAKDOWN_FP.m = bFp;
        const expand = $("m-expand");
        if (expand) expand.innerHTML = renderBreakdownBars(card.breakdown);
      }
    }
  }

  const SECTION_FP = {};
  function renderActivity(rows) {
    const tbody = $("activity-body");
    if (!tbody) return;
    const fp = JSON.stringify(rows || []);
    if (SECTION_FP.activity === fp) return;
    SECTION_FP.activity = fp;
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
    const fp = JSON.stringify(rows || []);
    if (SECTION_FP.buckets === fp) return;
    SECTION_FP.buckets = fp;
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
    const fp = JSON.stringify(rows || []);
    if (SECTION_FP.keys === fp) return;
    SECTION_FP.keys = fp;
    tbody.innerHTML = (rows || []).map(k => `
      <tr>
        <td class="mono">${escapeHtml(k.id)}</td>
        <td>${escapeHtml(k.type)}</td>
        <td><span class="pill ok">${escapeHtml(k.status)}</span></td>
        <td>${escapeHtml(k.created)}</td>
      </tr>
    `).join("");
  }

  function renderFromStatus(d) {
    LAST_STATUS = d;
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
    if (currentRoute.view === "metric" && d.cards[currentRoute.metric]) {
      renderMetric(currentRoute.metric, d.cards[currentRoute.metric]);
    }
  }

  async function refresh() {
    try {
      const r = await fetch(API_STATUS, {credentials: "same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) return;
      renderFromStatus(await r.json());
    } catch (e) { /* ignore */ }
  }

  let _es = null;
  let _esRetry = 0;
  function connectStream() {
    if (_es) return;
    try {
      _es = new EventSource(API_STREAM, {withCredentials: true});
    } catch (e) {
      setTimeout(connectStream, 3000);
      return;
    }
    _es.addEventListener("status", (e) => {
      _esRetry = 0;
      try { renderFromStatus(JSON.parse(e.data)); } catch {}
    });
    _es.onerror = () => {
      if (_es) { _es.close(); _es = null; }
      const delay = Math.min(1000 * Math.pow(2, _esRetry++), 15000);
      setTimeout(connectStream, delay);
    };
  }

  // ------------------- Bucket detail (list-style) -------------------
  const FOLDER_ICON = '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 6a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6z"/></svg>';
  const FILE_ICON   = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6z"/><polyline points="14 2 14 8 20 8"/></svg>';

  function renderCrumbs(bucket, prefix) {
    const crumbs = $("bv-crumbs");
    const parts = (prefix || "").split("/").filter(Boolean);
    const out = [];
    out.push(`<a href="#bucket=${encodeURIComponent(bucket)}">${escapeHtml(bucket)}</a>`);
    let built = "";
    for (let i = 0; i < parts.length; i++) {
      built += parts[i] + "/";
      out.push('<span class="sep">›</span>');
      const isLast = i === parts.length - 1;
      if (isLast) {
        out.push(`<span class="curr">${escapeHtml(parts[i])}</span>`);
      } else {
        out.push(`<a href="#bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(built)}">${escapeHtml(parts[i])}</a>`);
      }
    }
    crumbs.innerHTML = out.join(" ");
  }

  const BUCKET_FP = {};

  function formatIsoShort(iso) {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      return d.toLocaleString(undefined, {
        year: "numeric", month: "short", day: "2-digit",
        hour: "2-digit", minute: "2-digit",
      });
    } catch { return iso; }
  }

  async function loadBucket(bucket, prefix) {
    prefix = prefix || "";
    setText("bv-title", bucket);
    renderCrumbs(bucket, prefix);
    const tbody = $("bv-body");
    const fpKey = bucket + "|" + prefix;
    // Only show "Loading" if we're coming from a different bucket/prefix.
    if (BUCKET_FP._last !== fpKey) {
      BUCKET_FP._last = fpKey;
      tbody.innerHTML = '<tr><td colspan="3" class="empty-state">Loading…</td></tr>';
      BUCKET_FP[fpKey] = null;
    }
    try {
      const url = API_BUCKET + "/" + encodeURIComponent(bucket) +
                  "?prefix=" + encodeURIComponent(prefix) + "&delimiter=/";
      const r = await fetch(url, {credentials:"same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) {
        tbody.innerHTML = `<tr><td colspan="3" class="empty-state">Failed to load: ${r.status}</td></tr>`;
        return;
      }
      const d = await r.json();
      setText(
        "bv-sub",
        `${d.folders.length} folder${d.folders.length === 1 ? "" : "s"}, ` +
        `${d.objects.length} object${d.objects.length === 1 ? "" : "s"}` +
        (d.is_truncated ? " (truncated — showing first 500)" : "")
      );
      const fp = JSON.stringify([d.folders, d.objects, d.is_truncated]);
      if (BUCKET_FP[fpKey] === fp) return;
      BUCKET_FP[fpKey] = fp;

      const total = d.folders.length + d.objects.length;
      if (total === 0) {
        tbody.innerHTML = '<tr><td colspan="3" class="empty-state">Empty folder.</td></tr>';
        return;
      }
      const rows = [];
      for (const f of d.folders) {
        const href = "#bucket=" + encodeURIComponent(bucket) + "&prefix=" + encodeURIComponent(f.prefix);
        rows.push(`
          <tr class="row" onclick="location.hash='${'bucket=' + encodeURIComponent(bucket) + '&prefix=' + encodeURIComponent(f.prefix)}'">
            <td class="col-name">
              <span class="row-icon folder">${FOLDER_ICON}</span>
              <a href="${href}" title="${escapeHtml(f.prefix)}">${escapeHtml((f.name || f.prefix) + "/")}</a>
            </td>
            <td class="col-size">—</td>
            <td class="col-modified">—</td>
          </tr>`);
      }
      for (const o of d.objects) {
        const href = "#bucket=" + encodeURIComponent(bucket) + "&object=" + encodeURIComponent(o.key);
        rows.push(`
          <tr class="row" onclick="location.hash='${'bucket=' + encodeURIComponent(bucket) + '&object=' + encodeURIComponent(o.key)}'">
            <td class="col-name">
              <span class="row-icon file">${FILE_ICON}</span>
              <a href="${href}" title="${escapeHtml(o.key)}">${escapeHtml(o.name || o.key)}</a>
            </td>
            <td class="col-size">${escapeHtml(o.size_h || "—")}</td>
            <td class="col-modified">${escapeHtml(formatIsoShort(o.last_modified))}</td>
          </tr>`);
      }
      tbody.innerHTML = rows.join("");
    } catch (e) {
      // silent — next tick will retry
    }
  }

  // ------------------- Logs view -------------------
  async function loadLogs() {
    const q = $("lv-q").value;
    const op = $("lv-op").value;
    const stt = $("lv-status").value;
    const params = new URLSearchParams({q, operation: op, status: stt, limit: "200"});
    try {
      const r = await fetch(API_LOGS + "?" + params.toString(), {credentials: "same-origin"});
      if (r.status === 401) { location.href = "__LOGIN_URL__"; return; }
      if (!r.ok) return;
      const d = await r.json();
      // Populate operation filter (preserve current selection)
      const opSel = $("lv-op");
      const currentOp = opSel.value;
      const existing = new Set(Array.from(opSel.options).map(o => o.value));
      for (const op of d.operations) {
        if (!existing.has(op)) {
          const o = document.createElement("option");
          o.value = op; o.textContent = op;
          opSel.appendChild(o);
        }
      }
      opSel.value = currentOp;
      setText("lv-count", `${d.count} of ${d.total} entries`);
      setText("lv-sub", `${d.total} entries in ring buffer`);
      const tbody = $("lv-body");
      const fp = JSON.stringify(d.entries || []);
      if (SECTION_FP.logs === fp) return;
      SECTION_FP.logs = fp;
      if (!d.entries.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No log entries match.</td></tr>';
        return;
      }
      tbody.innerHTML = d.entries.map(r => {
        const hasBucket = !!r.bucket;
        const hasObject = hasBucket && !!r.object;
        const bucketCell = hasBucket
          ? `<a class="linkish" href="#bucket=${encodeURIComponent(r.bucket)}">${escapeHtml(r.bucket)}</a>`
          : '<span style="color:var(--text-subtle)">—</span>';
        const objectCell = hasObject
          ? `<a class="linkish mono" href="#bucket=${encodeURIComponent(r.bucket)}&object=${encodeURIComponent(r.object)}" title="${escapeHtml(r.object)}">${escapeHtml(r.object)}</a>`
          : '<span style="color:var(--text-subtle)">—</span>';
        return `
          <tr>
            <td style="color:var(--text-muted)">${escapeHtml(r.time)}</td>
            <td class="mono">${escapeHtml(r.operation)}</td>
            <td>${bucketCell}</td>
            <td class="truncate">${objectCell}</td>
            <td><span class="pill ${r.status === "Success" ? "ok" : "err"}">${escapeHtml(r.status)}</span></td>
            <td class="mono">${escapeHtml(String(r.status_code))}</td>
            <td>${escapeHtml(r.size)}</td>
            <td class="mono">${escapeHtml(r.client_ip || "—")}</td>
            <td>${escapeHtml(r.latency)}</td>
          </tr>`;
      }).join("");
    } catch (e) { /* retry next tick */ }
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
  const mWrap = $("m-chartwrap");
  if (mWrap) {
    mWrap.addEventListener("mousemove", (e) => handleChartHover("m", e));
    mWrap.addEventListener("mouseleave", () => handleChartLeave("m"));
  }
  document.querySelectorAll("[data-goto]").forEach(el => {
    el.addEventListener("click", (e) => { e.preventDefault(); gotoDashboard(); });
  });
  $("refresh-btn").addEventListener("click", () => {
    refresh();
    if (currentRoute.view === "bucket") loadBucket(currentRoute.bucket, currentRoute.prefix || "");
    if (currentRoute.view === "logs")   loadLogs();
  });
  window.addEventListener("hashchange", navigateFromHash);

  // Logs toolbar: debounced search + filter changes
  let _logsDebounce = 0;
  function onLogsFilterChange() {
    clearTimeout(_logsDebounce);
    _logsDebounce = setTimeout(loadLogs, 150);
  }
  $("lv-q").addEventListener("input", onLogsFilterChange);
  $("lv-op").addEventListener("change", loadLogs);
  $("lv-status").addEventListener("change", loadLogs);

  // SSE pushes header/footer/cards/activity/buckets/keys on change.
  // The active bucket/logs view still polls at a low rate since those aren't streamed.
  async function viewTick() {
    if (currentRoute.view === "bucket") await loadBucket(currentRoute.bucket, currentRoute.prefix || "");
    if (currentRoute.view === "logs")   await loadLogs();
  }

  refresh();            // immediate paint while the stream is connecting
  navigateFromHash();
  connectStream();
  setInterval(viewTick, VIEW_POLL_MS);
</script>
</body>
</html>
"""


def _card_html(num: str, label: str, icon_svg: str, key: str) -> str:
    return f"""
      <a class="card" data-card="{num}" data-metric="{key}" href="#metric={key}">
        <div class="card-head">
          <div class="card-head-left">
            <span class="card-icon">{icon_svg}</span>
            <span id="c{num}-label">{label}</span>
          </div>
          <svg class="card-chevron" width="14" height="14" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="9 6 15 12 9 18"></polyline>
          </svg>
        </div>
        <div><span class="card-value" id="c{num}-value">—</span><span class="card-unit" id="c{num}-unit"></span></div>
        <div class="card-delta" id="c{num}-delta">&nbsp;</div>
        <svg class="spark" id="c{num}-spark" viewBox="0 0 100 28" preserveAspectRatio="none"></svg>
      </a>
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
    html = html.replace("__STREAM_URL__", f"{prefix}/api/stream")
    html = html.replace("__BUCKET_URL__", f"{prefix}/api/buckets")
    html = html.replace("__OBJECT_URL__", f"{prefix}/api/objects")
    html = html.replace("__LOGS_URL__", f"{prefix}/api/logs")
    html = html.replace("__LOGIN_URL__", f"{prefix}/login")
    html = html.replace("__LOGOUT_URL__", f"{prefix}/logout")
    html = html.replace(
        "__CARD_REQUESTS__", _card_html("1", "Requests", _ICON_REQUESTS, "requests")
    )
    html = html.replace(
        "__CARD_DATA__", _card_html("2", "Data Encrypted", _ICON_LOCK, "data_encrypted")
    )
    html = html.replace("__CARD_ERRORS__", _card_html("3", "Errors", _ICON_ERR, "errors"))
    html = html.replace(
        "__CARD_BUCKETS__", _card_html("4", "Active Buckets", _ICON_BUCKET, "active_buckets")
    )
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
