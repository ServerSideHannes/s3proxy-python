<script lang="ts">
  import { untrack } from 'svelte';
  import { fetchSeries } from '$lib/api';
  import { createChart, type ChartHandle } from '$lib/chart';
  import { formatBytes, formatNumber } from '$lib/format';
  import type { Card } from '$lib/types';

  let { metricKey, card }: { metricKey: string; card: Card | undefined } = $props();

  // metric-card key -> /api/series metric name (verbatim from the old SERIES_METRIC).
  const SERIES_METRIC: Record<string, string> = {
    requests: 'requests',
    data_encrypted: 'crypto',
    errors: 'errors'
  };
  const BREAKDOWN_TITLES: Record<string, string> = {
    requests: 'Breakdown by method',
    data_encrypted: 'Breakdown by direction',
    errors: 'Breakdown by status code',
    active_buckets: 'Breakdown by bucket'
  };
  const RANGES = ['1h', '3h', '7h', '24h', '7d'];

  let range = $state('1h');
  let direction = $state<'put' | 'get'>('put'); // data_encrypted only
  let chartMeta = $state('');

  let chartEl: HTMLDivElement;
  let chart: ChartHandle | null = null;

  let isData = $derived(metricKey === 'data_encrypted');
  let unit = $derived(card?.unit ?? '');
  let label = $derived(card?.label ?? '—');
  let chartTitle = $derived(
    isData ? (direction === 'get' ? 'Data Decrypted' : 'Data Encrypted') + ' over time' : label + ' over time'
  );
  let breakdownTitle = $derived(BREAKDOWN_TITLES[metricKey] || 'Breakdown');

  // Proportional breakdown bars — ported from renderBreakdownBars().
  let bars = $derived.by(() => {
    const items = card?.breakdown ?? [];
    const weights = items.map((b) => (typeof b.weight === 'number' ? b.weight : 0));
    const max = Math.max(...weights, 0);
    const total = weights.reduce((a, b) => a + b, 0);
    const showPct = total > 0 && items.length > 1;
    return items.map((b, i) => ({
      label: b.label,
      value: b.value,
      pct: max > 0 ? (weights[i] / max) * 100 : 0,
      share: total > 0 ? (weights[i] / total) * 100 : 0,
      showPct
    }));
  });

  async function loadRange() {
    let metric = SERIES_METRIC[metricKey] || 'requests';
    if (metricKey === 'data_encrypted') {
      metric = direction === 'get' ? 'bytes_get' : 'bytes_put';
    }
    // The bytes_* series are raw byte counts -> byte units. Other metrics are
    // plain counts -> formatNumber + the card's unit (e.g. "req"). The formatter
    // returns a complete string incl. unit, so the chart never appends one.
    const fmt: (v: number) => string =
      metricKey === 'data_encrypted'
        ? formatBytes
        : (v) => formatNumber(v) + (unit ? ' ' + unit : '');
    try {
      const d = await fetchSeries(metric, range);
      const vals = d.spark || [];
      const times = d.spark_times || [];
      if (chart) {
        if (vals.length < 2 || times.length !== vals.length) {
          chart.setData([], [], fmt);
          chartMeta = '';
        } else {
          chart.setData(times, vals, fmt);
          const rawMax = Math.max(...vals, 1);
          const lastV = vals[vals.length - 1];
          chartMeta = 'peak ' + fmt(rawMax) + ' · latest ' + fmt(lastV);
        }
      }
    } catch {
      /* leave chart as-is */
    }
  }

  function setRange(r: string) {
    range = r;
    void loadRange();
  }
  function setDirection(d: 'put' | 'get') {
    direction = d;
    void loadRange();
  }

  // Mount once. This effect must NOT read range/direction reactively — it writes
  // them, and if it also tracked them, every tab click would re-run the effect,
  // reset the selection back to 1h/put, and recreate the chart. The component is
  // already remounted per metric via {#key} in the parent, so a plain mount hook
  // is the right scope.
  $effect(() => {
    chart = createChart(chartEl);
    untrack(() => {
      range = '1h';
      direction = 'put';
      void loadRange();
    });
    const onResize = () => chart?.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      chart?.destroy();
      chart = null;
    };
  });
</script>

<a class="back-link" href="#" style="display:inline-block;margin-bottom:12px">← Back to dashboard</a>
<section class="section">
  <div class="metric-head">
    <div class="metric-head-text">
      <div class="metric-hero-label">{label}</div>
      <div class="metric-hero-value">
        <span>{card?.value ?? '—'}</span><span class="card-unit">{unit}</span>
      </div>
      <div class="metric-hero-delta">{card?.detail || ' '}</div>
    </div>
  </div>
  <div class="metric-grid">
    <div class="metric-chart-col">
      <div class="chart-header">
        <span class="chart-title">{chartTitle}</span>
        {#if isData}
          <span class="range-tabs">
            <button type="button" class="range-tab" class:active={direction === 'put'} onclick={() => setDirection('put')}>Encrypted</button>
            <button type="button" class="range-tab" class:active={direction === 'get'} onclick={() => setDirection('get')}>Decrypted</button>
          </span>
        {/if}
        <span class="range-tabs">
          {#each RANGES as r}
            <button type="button" class="range-tab" class:active={range === r} onclick={() => setRange(r)}>{r}</button>
          {/each}
        </span>
        <span class="chart-subtle">{chartMeta || ' '}</span>
      </div>
      <div class="chart-wrap chart-wrap--big" bind:this={chartEl}></div>
    </div>
    <div class="metric-breakdown-col">
      <h4 class="breakdown-heading">{breakdownTitle}</h4>
      <div class="breakdown-bars">
        {#if bars.length === 0}
          <div class="empty-state" style="padding:8px 0">Nothing to show yet.</div>
        {:else}
          {#each bars as b}
            <div class="bb-row">
              <div class="bb-head">
                <span class="bb-label">{b.label}</span>
                <span>
                  <span class="bb-value">{b.value}</span>{#if b.showPct}<span class="bb-percent">{b.share.toFixed(1)}%</span>{/if}
                </span>
              </div>
              <div class="bb-track"><div class="bb-fill" style="width:{b.pct.toFixed(1)}%"></div></div>
            </div>
          {/each}
        {/if}
      </div>
    </div>
  </div>
</section>
