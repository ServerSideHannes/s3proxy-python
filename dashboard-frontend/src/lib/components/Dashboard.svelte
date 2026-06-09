<script lang="ts">
  import { ICON_BUCKET, ICON_ERR, ICON_LOCK, ICON_REQUESTS } from '$lib/icons';
  import { bucketHref, objectHref } from '$lib/route';
  import type { StatusPayload } from '$lib/types';
  import MetricCard from './MetricCard.svelte';

  let { status }: { status: StatusPayload | null } = $props();

  let cards = $derived(status?.cards);
  let activity = $derived(status?.activity ?? []);
  let buckets = $derived(status?.buckets ?? []);
  let keys = $derived(status?.keys ?? []);
</script>

<section class="cards">
  <MetricCard
    metricKey="requests"
    icon={ICON_REQUESTS}
    fallbackLabel="Requests"
    card={cards?.requests}
  />
  <MetricCard
    metricKey="data_encrypted"
    icon={ICON_LOCK}
    fallbackLabel="Data Encrypted"
    card={cards?.data_encrypted}
  />
  <MetricCard metricKey="errors" icon={ICON_ERR} fallbackLabel="Errors" card={cards?.errors} />
  <MetricCard
    metricKey="active_buckets"
    icon={ICON_BUCKET}
    fallbackLabel="Active Buckets"
    card={cards?.active_buckets}
    showRange={false}
  />
</section>

<section class="section">
  <div class="section-head">
    <div class="section-title">Recent Activity</div>
    <div class="section-actions">
      <span class="live"><span class="dot"></span>Live</span>
      <a class="btn-ghost" href="#logs">View all logs →</a>
    </div>
  </div>
  <div class="scroll-x">
    <table>
      <thead>
        <tr>
          <th>Time</th><th>Operation</th><th>Bucket</th><th>Object</th>
          <th>Status</th><th>Size</th><th>Client IP</th><th>Latency</th>
        </tr>
      </thead>
      <tbody>
        {#if activity.length === 0}
          <tr><td colspan="8" class="empty-state">No requests yet — traffic will appear here.</td></tr>
        {:else}
          {#each activity as r}
            {@const hasBucket = r.bucket && r.bucket !== '—'}
            {@const hasObject = hasBucket && r.object && r.object !== '—'}
            <tr>
              <td class="mono" style="color:var(--text-muted)" title={r.time_relative || ''}>{r.time}</td>
              <td class="mono">{r.operation}</td>
              <td>
                {#if hasBucket}
                  <a class="linkish" href={bucketHref(r.bucket)}>{r.bucket}</a>
                {:else}{r.bucket}{/if}
              </td>
              <td class="truncate">
                {#if hasObject}
                  <a class="linkish mono" href={objectHref(r.bucket, r.object)} title={r.object}>{r.object}</a>
                {:else}<span class="mono">{r.object}</span>{/if}
              </td>
              <td><span class="pill {r.status === 'Success' ? 'ok' : 'err'}">{r.status}</span></td>
              <td>{r.size}</td>
              <td class="mono">{r.client_ip}</td>
              <td>{r.latency}</td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
</section>

<section class="split">
  <div class="section" style="margin-bottom:0">
    <div class="section-head">
      <div class="section-title">Buckets</div>
    </div>
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th title="Distinct objects seen in recent requests — not the full bucket count">Objects (seen)</th>
          <th>Size</th>
        </tr>
      </thead>
      <tbody>
        {#if buckets.length === 0}
          <tr><td colspan="3" class="empty-state">No buckets observed yet.</td></tr>
        {:else}
          {#each buckets as b}
            <tr>
              <td><a class="linkish" href={bucketHref(b.name)}>{b.name}</a></td>
              <td>{b.objects}</td>
              <td>{b.size}</td>
            </tr>
          {/each}
        {/if}
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
      <tbody>
        {#each keys as k}
          <tr>
            <td class="mono">{k.id}</td>
            <td>{k.type}</td>
            <td><span class="pill ok">{k.status}</span></td>
            <td>{k.created}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
