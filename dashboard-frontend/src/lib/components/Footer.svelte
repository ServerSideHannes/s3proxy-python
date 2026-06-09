<script lang="ts">
  import { REFRESH, SHIELD } from '$lib/icons';
  import type { StatusPayload } from '$lib/types';

  let { status, onRefresh }: { status: StatusPayload | null; onRefresh: () => void } = $props();

  let version = $derived(status ? 'v' + status.footer.version : '—');
  let rps = $derived(status ? String(status.footer.req_per_s) : '0');
  let throughput = $derived(status?.footer.throughput ?? '0 B/s');
  let lastError = $derived(status?.footer.last_error ?? 'never');
</script>

<footer class="footer">
  <span class="brand-mini">{@html SHIELD} Proxy Version: <span>{version}</span></span>
  <span>Requests: <span>{rps}</span> req/s</span>
  <span>Throughput: <span>{throughput}</span></span>
  <span>Last error: <span>{lastError}</span></span>
  <span class="spacer"></span>
  <button class="btn-ghost" type="button" onclick={onRefresh}>
    {@html REFRESH} Refresh
  </button>
</footer>
