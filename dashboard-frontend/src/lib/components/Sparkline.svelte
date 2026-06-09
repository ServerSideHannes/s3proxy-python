<script lang="ts">
  // Tiny inline-SVG sparkline, identical to the old drawSpark(): a single
  // polyline scaled into a 100x28 box. Kept as SVG (not uPlot) because it's
  // trivial and matches the previous output exactly.
  let { values }: { values?: number[] } = $props();

  const W = 100;
  const H = 28;

  let points = $derived.by(() => {
    if (!values || values.length < 2) return '';
    const max = Math.max(...values, 1);
    const step = W / (values.length - 1);
    return values
      .map((v, i) => {
        const y = H - (v / max) * (H - 4) - 2;
        return i * step + ',' + y;
      })
      .join(' ');
  });
</script>

<svg class="spark" viewBox="0 0 {W} {H}" preserveAspectRatio="none">
  {#if points}
    <polyline fill="none" stroke="currentColor" stroke-width="1.2" points={points} />
  {/if}
</svg>
