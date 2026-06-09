import { fetchStatus, LOGIN_URL, STREAM_URL } from './api';
import type { StatusPayload } from './types';

// Live status feed. SSE pushes the full payload on change + heartbeats; we also
// do one immediate fetch so the UI paints while the stream connects, exactly as
// the old dashboard did. Backoff on stream error caps at 15s.

class StatusFeed {
  current = $state<StatusPayload | null>(null);
  // 'pending' until the first status fetch resolves; 'ok' once authenticated.
  // The page gates its chrome on this so an unauthenticated visit redirects to
  // the login page instead of flashing an empty dashboard.
  auth = $state<'pending' | 'ok'>('pending');

  #es: EventSource | null = null;
  #retry = 0;
  #started = false;

  start() {
    if (this.#started) return;
    this.#started = true;
    void this.refresh();
    this.#connect();
  }

  stop() {
    this.#started = false;
    if (this.#es) {
      this.#es.close();
      this.#es = null;
    }
  }

  async refresh() {
    try {
      this.current = await fetchStatus();
      this.auth = 'ok';
    } catch {
      // 401 has already navigated to the login page (see api.ts toLogin()).
      // Transient errors are ignored — the SSE stream / next refresh retries.
    }
  }

  #connect() {
    if (this.#es) return;
    try {
      this.#es = new EventSource(STREAM_URL, { withCredentials: true });
    } catch {
      setTimeout(() => this.#connect(), 3000);
      return;
    }
    this.#es.addEventListener('status', (e) => {
      this.#retry = 0;
      try {
        this.current = JSON.parse((e as MessageEvent).data) as StatusPayload;
      } catch {
        /* ignore malformed frame */
      }
    });
    this.#es.onerror = () => {
      if (this.#es) {
        this.#es.close();
        this.#es = null;
      }
      const delay = Math.min(1000 * Math.pow(2, this.#retry++), 15000);
      setTimeout(() => this.#connect(), delay);
    };
  }
}

export const statusFeed = new StatusFeed();
export { LOGIN_URL };
