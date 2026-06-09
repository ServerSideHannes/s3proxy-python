<script lang="ts">
  import { untrack } from 'svelte';
  import { fetchBucket } from '$lib/api';
  import { formatIsoShort } from '$lib/format';
  import { FILE_ICON, FOLDER_ICON, LOCK_ICON } from '$lib/icons';
  import { bucketHref, objectHref, prefixHref } from '$lib/route';
  import type { BucketPayload } from '$lib/types';

  let { bucket, prefix }: { bucket: string; prefix: string } = $props();

  const PAGE = 20;
  let offset = $state(0);
  let data = $state<BucketPayload | null>(null);
  let loading = $state(true);
  let errorStatus = $state<number | null>(null);

  let crumbs = $derived.by(() => {
    const parts = (prefix || '').split('/').filter(Boolean);
    const out: { label: string; href?: string; current?: boolean }[] = [
      { label: bucket, href: bucketHref(bucket) }
    ];
    let built = '';
    for (let i = 0; i < parts.length; i++) {
      built += parts[i] + '/';
      const isLast = i === parts.length - 1;
      out.push(
        isLast
          ? { label: parts[i], current: true }
          : { label: parts[i], href: prefixHref(bucket, built) }
      );
    }
    return out;
  });

  let summary = $derived.by(() => {
    if (!data) return '—';
    const totalObj = data.total_objects != null ? data.total_objects : data.objects.length;
    const from = totalObj === 0 ? 0 : data.offset + 1;
    const to = data.offset + data.objects.length;
    return (
      `${data.folders.length} folder${data.folders.length === 1 ? '' : 's'}, ` +
      `${totalObj} object${totalObj === 1 ? '' : 's'}` +
      (totalObj > data.objects.length ? ` (showing ${from}–${to})` : '') +
      (data.is_truncated ? ' · bucket truncated at 1000 keys' : '')
    );
  });

  let pageStatus = $derived.by(() => {
    if (!data) return '—';
    const totalObj = data.total_objects != null ? data.total_objects : data.objects.length;
    if (totalObj === 0) return '—';
    const from = data.offset + 1;
    const to = data.offset + data.objects.length;
    return `${from}–${to} of ${totalObj}`;
  });

  // Page-number navigation, derived from the server's offset/total.
  let totalObjects = $derived(
    data ? (data.total_objects != null ? data.total_objects : data.objects.length) : 0
  );
  let totalPages = $derived(Math.max(1, Math.ceil(totalObjects / PAGE)));
  let currentPage = $derived(data ? Math.floor(data.offset / PAGE) + 1 : 1);
  // A compact window of page numbers around the current page (with first/last).
  let pageNumbers = $derived.by(() => {
    const tp = totalPages;
    if (tp <= 1) return [] as number[];
    const cur = currentPage;
    const set = new Set<number>([1, tp, cur, cur - 1, cur + 1, cur - 2, cur + 2]);
    return [...set].filter((p) => p >= 1 && p <= tp).sort((a, b) => a - b);
  });

  async function load() {
    loading = true;
    errorStatus = null;
    try {
      data = await fetchBucket(bucket, prefix, offset, PAGE);
    } catch (e) {
      errorStatus = e instanceof Error && e.message.startsWith('HTTP ') ? Number(e.message.slice(5)) : -1;
    } finally {
      loading = false;
    }
  }

  function goToPage(p: number) {
    const target = Math.min(Math.max(1, p), totalPages);
    offset = (target - 1) * PAGE;
    void load();
  }

  // Reset to the first page ONLY when the bucket or prefix changes. load() reads
  // offset, so without untrack() the effect would also depend on offset — and
  // every page click would re-run it, reset offset to 0, and snap back to page 1
  // (the pagination bug). Track bucket/prefix explicitly; do the rest untracked.
  $effect(() => {
    bucket;
    prefix;
    untrack(() => {
      offset = 0;
      void load();
    });
  });
</script>

{#snippet pager()}
  {#if data && totalPages > 1}
    <div class="logs-pager">
      <button type="button" class="pager-btn" disabled={currentPage <= 1} onclick={() => goToPage(currentPage - 1)}>← Prev</button>
      {#each pageNumbers as p, i}
        {#if i > 0 && p - pageNumbers[i - 1] > 1}<span class="pager-gap">…</span>{/if}
        <button type="button" class="pager-num" class:active={p === currentPage} onclick={() => goToPage(p)}>{p}</button>
      {/each}
      <button type="button" class="pager-btn" disabled={currentPage >= totalPages} onclick={() => goToPage(currentPage + 1)}>Next →</button>
      <span class="pager-jump">
        Go to
        <input
          type="number"
          min="1"
          max={totalPages}
          placeholder={String(currentPage)}
          onkeydown={(e) => {
            if (e.key === 'Enter') {
              const v = Number((e.currentTarget as HTMLInputElement).value);
              if (v) goToPage(v);
            }
          }} />
        <span class="pager-status">/ {totalPages} ({pageStatus})</span>
      </span>
    </div>
  {/if}
{/snippet}

<section class="section">
  <div class="section-head">
    <div style="min-width:0;flex:1">
      <a class="back-link" href="#">← Back to dashboard</a>
      <div class="section-title" style="margin-top:6px">{bucket}</div>
      <div class="crumbs">
        {#each crumbs as c, i}
          {#if i > 0}<span class="sep">›</span>{/if}
          {#if c.current}
            <span class="curr">{c.label}</span>
          {:else}
            <a href={c.href}>{c.label}</a>
          {/if}
        {/each}
      </div>
      <div class="detail-sub">{summary}</div>
    </div>
    <div class="section-actions">
      <span class="live"><span class="dot"></span>Live</span>
    </div>
  </div>
  {@render pager()}
  <table class="explorer">
    <thead>
      <tr>
        <th>Name</th>
        <th style="width: 150px;">Encryption</th>
        <th style="width: 110px; text-align: right;">Size</th>
        <th style="width: 180px;">Last Modified</th>
      </tr>
    </thead>
    <tbody>
      {#if loading && !data}
        <tr><td colspan="4" class="empty-state">Loading…</td></tr>
      {:else if errorStatus != null}
        <tr><td colspan="4" class="empty-state">Failed to load: {errorStatus}</td></tr>
      {:else if data && data.folders.length + data.objects.length === 0}
        <tr><td colspan="4" class="empty-state">Empty folder.</td></tr>
      {:else if data}
        {#each data.folders as f}
          <tr class="row" onclick={() => (location.hash = prefixHref(bucket, f.prefix).slice(1))}>
            <td class="col-name">
              <span class="row-icon folder">{@html FOLDER_ICON}</span>
              <a href={prefixHref(bucket, f.prefix)} title={f.prefix}>{(f.name || f.prefix) + '/'}</a>
            </td>
            <td>—</td>
            <td class="col-size">—</td>
            <td class="col-modified">—</td>
          </tr>
        {/each}
        {#each data.objects as o}
          <tr class="row" onclick={() => (location.hash = objectHref(bucket, o.key).slice(1))}>
            <td class="col-name">
              <span class="row-icon file">{@html FILE_ICON}</span>
              <a href={objectHref(bucket, o.key)} title={o.key}>{o.name || o.key}</a>
            </td>
            <td>
              {#if o.encrypted === true}
                <span class="enc-cell on">{@html LOCK_ICON}Encrypted</span>
              {:else if o.encrypted === false}
                <span class="enc-cell off">Not Encrypted</span>
              {:else}
                <span class="enc-cell off">—</span>
              {/if}
            </td>
            <td class="col-size">{o.size_h || '—'}</td>
            <td class="col-modified">{formatIsoShort(o.last_modified)}</td>
          </tr>
        {/each}
      {/if}
    </tbody>
  </table>
  {@render pager()}
</section>
