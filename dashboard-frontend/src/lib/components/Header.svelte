<script lang="ts">
  import { LOGOUT_URL } from '$lib/api';
  import { LOCK_BRAND } from '$lib/icons';
  import type { StatusPayload } from '$lib/types';

  let { status }: { status: StatusPayload | null } = $props();

  let title = $derived(status?.header.title ?? 'S3 Encryption Proxy');
  let statusText = $derived(status?.header.status ?? 'Running');
  let uptime = $derived(status?.header.uptime ?? '—');
</script>

<header class="app-head">
  <div class="brand">
    <span class="brand-mark" aria-hidden="true">{@html LOCK_BRAND}</span>
    <span class="brand-name">{title}</span>
  </div>
  <div class="head-right">
    <span class="status-pill"><span class="dot"></span><span>{statusText}</span></span>
    <span class="uptime">Uptime: <span>{uptime}</span></span>
    <span class="head-user">Signed in · <a href={LOGOUT_URL}>Logout</a></span>
  </div>
</header>
