<script lang="ts">
  import { untrack } from 'svelte';
  import { fetchLogs } from '$lib/api';
  import { createLoader } from '$lib/loader.svelte';
  import { bucketHref, objectHref } from '$lib/route';
  import type { LogsPayload } from '$lib/types';
  import Pager from './Pager.svelte';

  // Initial filters come from the hash (#logs&q=..&op=..&status=..).
  let { initialQ = '', initialOp = '', initialStatus = '' }: {
    initialQ?: string;
    initialOp?: string;
    initialStatus?: string;
  } = $props();

  const LIMIT = 25;
  const POLL_MS = 2000;

  // Seed the editable filters once from the route; the parent remounts this
  // component (via {#key}) when the route changes, so a one-time capture is right.
  let q = $state(untrack(() => initialQ));
  let op = $state(untrack(() => initialOp));
  let statusFilter = $state(untrack(() => initialStatus));
  let offset = $state(0);
  let data = $state<LogsPayload | null>(null);
  // Operation options accumulate across responses (never shrink), matching the
  // old behaviour of appending newly-seen operations to the <select>.
  let operations = $state<string[]>([]);

  let debounce: ReturnType<typeof setTimeout> | undefined;

  let countText = $derived.by(() => {
    if (!data) return '';
    const from = data.total === 0 ? 0 : data.offset + 1;
    const to = data.offset + data.count;
    return `${from}–${to} of ${data.total} entries`;
  });
  let subText = $derived(data ? `${data.total} entries (24h, capped)` : '—');

  function goOffset(o: number) {
    offset = o;
    void load();
  }

  const loader = createLoader();

  async function load() {
    // The poll and manual loads race; run() drops a superseded/errored response so
    // a slow poll can't overwrite a fresher filtered result. Errors retry next tick.
    const d = await loader.run(() =>
      fetchLogs({ q, operation: op, status: statusFilter, limit: LIMIT, offset })
    );
    if (!d) return;
    const seen = new Set(operations);
    for (const o of d.operations) if (!seen.has(o)) operations.push(o);
    data = d;
  }

  function reloadFromStart() {
    offset = 0;
    void load();
  }
  function onSearchInput() {
    clearTimeout(debounce);
    debounce = setTimeout(reloadFromStart, 150);
  }

  // Mount-only: initial load + a low-rate poll. untrack() so the effect doesn't
  // depend on q/op/status/offset (load reads them) — otherwise every filter or
  // page change would tear down and rebuild the interval and double-load.
  $effect(() => {
    untrack(() => void load());
    const id = setInterval(() => void load(), POLL_MS);
    return () => {
      clearInterval(id);
      clearTimeout(debounce);
    };
  });
</script>

<section class="section">
  <div class="section-head">
    <div>
      <a class="back-link" href="#">← Back to dashboard</a>
      <div class="section-title" style="margin-top:6px">Request Logs</div>
      <div class="detail-sub">{subText}</div>
    </div>
    <div class="section-actions">
      <span class="live"><span class="dot"></span>Live</span>
    </div>
  </div>
  <div class="logs-toolbar">
    <input
      type="search"
      placeholder="Search bucket, key, IP, method, status…"
      autocomplete="off"
      bind:value={q}
      oninput={onSearchInput} />
    <select bind:value={op} onchange={reloadFromStart}>
      <option value="">All operations</option>
      {#each operations as o}
        <option value={o}>{o}</option>
      {/each}
    </select>
    <select bind:value={statusFilter} onchange={reloadFromStart}>
      <option value="">All statuses</option>
      <option value="success">Success</option>
      <option value="error">Error</option>
    </select>
    <span class="logs-count">{countText}</span>
  </div>
  {#if data}
    <Pager total={data.total} pageSize={LIMIT} offset={data.offset} onGo={goOffset} />
  {/if}
  <div class="scroll-x">
    <table>
      <thead>
        <tr>
          <th>Time</th><th>Operation</th><th>Bucket</th><th>Object</th>
          <th>Status</th><th>Code</th><th>Size</th><th>Client IP</th><th>Latency</th>
        </tr>
      </thead>
      <tbody>
        {#if !data}
          <tr><td colspan="9" class="empty-state">Loading…</td></tr>
        {:else if data.entries.length === 0}
          <tr><td colspan="9" class="empty-state">No log entries match.</td></tr>
        {:else}
          {#each data.entries as r}
            {@const hasBucket = !!r.bucket}
            {@const hasObject = hasBucket && !!r.object}
            <tr>
              <td class="mono" style="color:var(--text-muted)" title={r.time_relative || ''}>{r.time}</td>
              <td class="mono">{r.operation}</td>
              <td>
                {#if hasBucket}
                  <a class="linkish" href={bucketHref(r.bucket)}>{r.bucket}</a>
                {:else}<span style="color:var(--text-subtle)">—</span>{/if}
              </td>
              <td class="truncate">
                {#if hasObject}
                  <a class="linkish mono" href={objectHref(r.bucket, r.object)} title={r.object}>{r.object}</a>
                {:else}<span style="color:var(--text-subtle)">—</span>{/if}
              </td>
              <td><span class="pill {r.status === 'Success' ? 'ok' : 'err'}">{r.status}</span></td>
              <td class="mono">{r.status_code}</td>
              <td>{r.size}</td>
              <td class="mono">{r.client_ip || '—'}</td>
              <td>{r.latency}</td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
</section>
