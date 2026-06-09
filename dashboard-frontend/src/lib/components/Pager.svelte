<script lang="ts">
  // Reusable, layout-stable pager. Page-number buttons are centered in a fixed
  // grid so the row never shifts when the window of numbers changes; the status
  // text sits in a reserved-width slot on the right.
  let {
    total,
    pageSize,
    offset,
    onGo
  }: {
    total: number;
    pageSize: number;
    offset: number;
    onGo: (offset: number) => void;
  } = $props();

  let totalPages = $derived(Math.max(1, Math.ceil(total / pageSize)));
  let currentPage = $derived(Math.floor(offset / pageSize) + 1);
  let from = $derived(total === 0 ? 0 : offset + 1);
  let to = $derived(Math.min(total, offset + pageSize));

  // Windowed page list with a CONSTANT number of slots so the bar never changes
  // width / recenters as you page. Always: first, last, and a fixed-size run
  // around the current page; gaps render as "…". For small page counts we just
  // show them all.
  const WINDOW = 5; // pages shown around (and including) the current page
  let pages = $derived.by(() => {
    const tp = totalPages;
    if (tp <= 1) return [] as number[];
    if (tp <= WINDOW + 2) return Array.from({ length: tp }, (_, i) => i + 1);
    const half = Math.floor(WINDOW / 2);
    let start = Math.max(2, currentPage - half);
    let end = Math.min(tp - 1, currentPage + half);
    // Keep the middle run a constant length even near the ends.
    if (currentPage - half < 2) end = Math.min(tp - 1, 1 + WINDOW);
    if (currentPage + half > tp - 1) start = Math.max(2, tp - WINDOW);
    const out: number[] = [1];
    if (start > 2) out.push(-1); // left gap sentinel
    for (let p = start; p <= end; p++) out.push(p);
    if (end < tp - 1) out.push(-2); // right gap sentinel
    out.push(tp);
    return out;
  });

  function go(p: number) {
    const t = Math.min(Math.max(1, p), totalPages);
    onGo((t - 1) * pageSize);
  }
</script>

{#if totalPages > 1}
  <div class="pager">
    <button type="button" class="pager-btn" disabled={currentPage <= 1} onclick={() => go(currentPage - 1)}>← Prev</button>
    <div class="pager-nums">
      {#each pages as p}
        {#if p < 0}
          <span class="pager-gap">…</span>
        {:else}
          <button type="button" class="pager-num" class:active={p === currentPage} onclick={() => go(p)}>{p}</button>
        {/if}
      {/each}
    </div>
    <button type="button" class="pager-btn" disabled={currentPage >= totalPages} onclick={() => go(currentPage + 1)}>Next →</button>
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
            if (v) go(v);
          }
        }} />
      <span class="pager-status">/ {totalPages} ({from}–{to} of {total})</span>
    </span>
  </div>
{/if}

<style>
  .pager {
    display: flex; align-items: center;
    gap: 10px; margin-top: 14px;
  }
  /* Left-anchored, fixed-width number group so buttons keep stable positions —
     centering re-centered (and moved) every button when the window length
     changed. The constant-size window (Pager.svelte) keeps the count steady;
     the reserved width + left justify keep the X positions steady. */
  .pager-nums {
    display: flex; align-items: center; justify-content: flex-start; gap: 6px;
    flex: 1; min-width: 340px;
  }
  .pager-status {
    color: var(--text-muted); font-size: 12px;
    font-variant-numeric: tabular-nums;
    min-width: 150px; text-align: left; white-space: nowrap;
  }
  .pager-jump {
    display: inline-flex; align-items: center; gap: 6px;
    color: var(--text-muted); font-size: 12px;
  }
  .pager-jump input {
    width: 56px; padding: 4px 6px;
    border: 1px solid var(--border-strong); border-radius: 7px;
    font: inherit; font-size: 13px; text-align: center;
  }
  .pager-jump input:focus { outline: none; border-color: var(--dark); }
</style>
