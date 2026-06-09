// Hash routing ported verbatim from the inline dashboard JS:
//   #, #logs, #logs&q=..&op=..&status=..,
//   #metric=KEY, #bucket=X, #bucket=X&prefix=P/, #bucket=X&object=Y

export type Route =
  | { view: 'dashboard' }
  | { view: 'logs'; q: string; op: string; status: string }
  | { view: 'metric'; metric: string }
  | { view: 'bucket'; bucket: string; prefix: string }
  | { view: 'object'; bucket: string; object: string };

export function parseHash(hash: string): Route {
  const h = (hash || '').replace(/^#/, '');
  if (!h) return { view: 'dashboard' };
  if (h === 'logs' || h.startsWith('logs&') || h.startsWith('logs?')) {
    const body = h.replace(/^logs[&?]?/, '');
    const params = new URLSearchParams(body);
    return {
      view: 'logs',
      q: params.get('q') || '',
      op: params.get('op') || '',
      status: params.get('status') || ''
    };
  }
  if (h.startsWith('metric=')) {
    const metric = h.substring('metric='.length);
    // Only these metrics have a chart detail view. active_buckets was removed;
    // anything unknown falls through to the dashboard.
    if (metric === 'requests' || metric === 'data_encrypted' || metric === 'errors') {
      return { view: 'metric', metric };
    }
    return { view: 'dashboard' };
  }
  const params = new URLSearchParams(h);
  const bucket = params.get('bucket');
  const object = params.get('object');
  const prefix = params.get('prefix') || '';
  if (bucket && object) return { view: 'object', bucket, object };
  if (bucket) return { view: 'bucket', bucket, prefix };
  return { view: 'dashboard' };
}

export function bucketHref(bucket: string): string {
  return `#bucket=${encodeURIComponent(bucket)}`;
}
export function prefixHref(bucket: string, prefix: string): string {
  return `#bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(prefix)}`;
}
export function objectHref(bucket: string, object: string): string {
  return `#bucket=${encodeURIComponent(bucket)}&object=${encodeURIComponent(object)}`;
}
export function metricHref(key: string): string {
  return `#metric=${key}`;
}
