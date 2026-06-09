<script lang="ts">
  import { CHEVRON } from '$lib/icons';
  import { metricHref } from '$lib/route';
  import type { Card } from '$lib/types';
  import Sparkline from './Sparkline.svelte';

  let {
    metricKey,
    icon,
    fallbackLabel,
    card,
    showRange = true
  }: {
    metricKey: string;
    icon: string;
    fallbackLabel: string;
    card: Card | undefined;
    showRange?: boolean;
  } = $props();

  // Cards other than Active Buckets are labelled "(1h)" to match the old UI.
  let label = $derived((card?.label ?? fallbackLabel) + (showRange ? ' (1h)' : ''));
</script>

<a class="card" href={metricHref(metricKey)}>
  <div class="card-head">
    <div class="card-head-left">
      <span class="card-icon">{@html icon}</span>
      <span>{label}</span>
    </div>
    {@html CHEVRON}
  </div>
  <div>
    <span class="card-value">{card?.value ?? '—'}</span><span class="card-unit">{card?.unit ?? ''}</span>
  </div>
  <div class="card-delta">{card?.detail || ' '}</div>
  <Sparkline values={card?.spark} />
</a>
