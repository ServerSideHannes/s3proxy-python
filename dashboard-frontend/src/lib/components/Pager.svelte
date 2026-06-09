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

  // Windowed page list with first/last + neighbours; gaps become "…".
  let pages = $derived.by(() => {
    const tp = totalPages;
    if (tp <= 1) return [] as number[];
    const cur = currentPage;
    const set = new Set<number>([1, tp, cur, cur - 1, cur + 1, cur - 2, cur + 2]);
    return [...set].filter((p) => p >= 1 && p <= tp).sort((a, b) => a - b);
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
      {#each pages as p, i}
        {#if i > 0 && p - pages[i - 1] > 1}<span class="pager-gap">…</span>{/if}
        <button type="button" class="pager-num" class:active={p === currentPage} onclick={() => go(p)}>{p}</button>
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
  /* Fixed-flex layout so the number group is centered and the row doesn't jump
     as the windowed page list / status text change width. */
  .pager-nums {
    display: flex; align-items: center; justify-content: center; gap: 6px;
    flex: 1;
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
