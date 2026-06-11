<script lang="ts">
  import BucketView from '$lib/components/BucketView.svelte';
  import Dashboard from '$lib/components/Dashboard.svelte';
  import Footer from '$lib/components/Footer.svelte';
  import Header from '$lib/components/Header.svelte';
  import LogsView from '$lib/components/LogsView.svelte';
  import MetricView from '$lib/components/MetricView.svelte';
  import ObjectView from '$lib/components/ObjectView.svelte';
  import { parseHash, type Route } from '$lib/route';
  import { statusFeed } from '$lib/status.svelte';

  let route = $state<Route>({ view: 'dashboard' });

  function sync() {
    route = parseHash(location.hash);
  }

  $effect(() => {
    sync();
    statusFeed.start();
    window.addEventListener('hashchange', sync);
    return () => {
      window.removeEventListener('hashchange', sync);
      statusFeed.stop();
    };
  });

  let status = $derived(statusFeed.current);
  let authed = $derived(statusFeed.auth === 'ok');

  function refresh() {
    void statusFeed.refresh();
    // Detail views re-fetch on their own $effect when the route changes; the
    // Refresh button additionally re-pulls the live status payload.
  }
</script>

{#if authed}
<div class="page">
  <Header {status} />

  {#if route.view === 'dashboard'}
    <Dashboard {status} />
  {:else if route.view === 'metric'}
    {#key route.metric}
      <MetricView
        metricKey={route.metric}
        card={status?.cards[route.metric]}
        clusterWide={status?.header.cluster_wide ?? true}
      />
    {/key}
  {:else if route.view === 'bucket'}
    {#key route.bucket + '|' + route.prefix}
      <BucketView bucket={route.bucket} prefix={route.prefix} />
    {/key}
  {:else if route.view === 'object'}
    {#key route.bucket + '|' + route.object}
      <ObjectView bucket={route.bucket} object={route.object} />
    {/key}
  {:else if route.view === 'logs'}
    {#key route.q + '|' + route.op + '|' + route.status}
      <LogsView initialQ={route.q} initialOp={route.op} initialStatus={route.status} />
    {/key}
  {/if}
</div>

<Footer {status} onRefresh={refresh} />
{/if}
