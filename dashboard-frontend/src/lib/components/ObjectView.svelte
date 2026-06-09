<script lang="ts">
  import { fetchObject } from '$lib/api';
  import { bucketHref } from '$lib/route';
  import type { ObjectDetail } from '$lib/types';

  let { bucket, object }: { bucket: string; object: string } = $props();

  let rows = $state<[string, string][]>([]);
  let state_ = $state<'loading' | 'ok' | 'error' | 'neterror'>('loading');
  let errorStatus = $state(0);

  let title = $derived(object.split('/').pop() || object);

  async function load() {
    state_ = 'loading';
    try {
      const d: ObjectDetail = await fetchObject(bucket, object);
      const out: [string, string][] = [
        ['Bucket', d.bucket],
        ['Key', d.key],
        ['Size (stored)', d.size_h],
        ['Content-Type', d.content_type || '—'],
        ['ETag', d.etag || '—'],
        ['Last Modified', d.last_modified || '—'],
        [
          'Encrypted',
          d.encrypted
            ? 'Yes (AES-256-GCM' + (d.encryption_source === 'sidecar' ? ', multipart sidecar' : '') + ')'
            : 'No'
        ]
      ];
      for (const [k, v] of Object.entries(d.metadata || {})) {
        out.push(['x-amz-meta-' + k, v]);
      }
      rows = out;
      state_ = 'ok';
    } catch (e) {
      if (e instanceof Error && e.message.startsWith('HTTP ')) {
        errorStatus = Number(e.message.slice(5));
        state_ = 'error';
      } else {
        state_ = 'neterror';
      }
    }
  }

  $effect(() => {
    bucket;
    object;
    void load();
  });
</script>

<section class="section">
  <div class="section-head">
    <div>
      <a class="back-link" href={bucketHref(bucket)}>← Back</a>
      <div class="section-title" style="margin-top:6px">{title}</div>
      <div class="detail-sub mono">{bucket} / {object}</div>
    </div>
  </div>
  <table>
    <tbody>
      {#if state_ === 'loading'}
        <tr><td colspan="2" class="empty-state">Loading…</td></tr>
      {:else if state_ === 'error'}
        <tr><td colspan="2" class="empty-state">Failed to load: {errorStatus}</td></tr>
      {:else if state_ === 'neterror'}
        <tr><td colspan="2" class="empty-state">Network error.</td></tr>
      {:else}
        {#each rows as [k, v]}
          <tr>
            <td style="color:var(--text-muted);width:200px;vertical-align:top;white-space:normal">{k}</td>
            <td class="mono ov-value">{v}</td>
          </tr>
        {/each}
      {/if}
    </tbody>
  </table>
</section>
