import { base } from '$app/paths';
import type {
  BucketPayload,
  LogsPayload,
  ObjectDetail,
  SeriesPayload,
  StatusPayload
} from './types';

// All requests are same-origin under the dashboard base path. The ingress routes
// `${base}/api/*` and `${base}/api/stream` to the proxy service, so relative
// URLs + the session cookie work without CORS. A 401 means the session lapsed —
// bounce to the login page, mirroring the old dashboard behaviour.

const API = `${base}/api`;
// The login page is served statically at ${base}/login, but the form POST and
// logout hit the proxy under ${base}/api so the ingress's /api rule reaches it.
export const LOGIN_URL = `${base}/login`;
export const LOGIN_ACTION = `${API}/login`;
export const LOGOUT_URL = `${API}/logout`;
export const STREAM_URL = `${API}/stream`;
// SSO start endpoint (proxy redirects the browser to the IdP).
export const OIDC_LOGIN_URL = `${API}/oidc/login`;

export interface AuthModes {
  password: boolean;
  oidc: boolean;
  oidc_label: string;
}

export async function fetchAuthModes(): Promise<AuthModes> {
  const r = await fetch(`${API}/authmodes`, { credentials: 'same-origin' });
  if (!r.ok) return { password: true, oidc: false, oidc_label: 'Sign in with SSO' };
  return (await r.json()) as AuthModes;
}

class Unauthorized extends Error {}

// Carries the HTTP status as a field instead of encoding it in the message, so
// callers (createLoader) can branch on `err.status` without string-parsing.
export class HttpError extends Error {
  constructor(readonly status: number) {
    super(`HTTP ${status}`);
    this.name = 'HttpError';
  }
}

function toLogin(): never {
  window.location.href = LOGIN_URL;
  throw new Unauthorized();
}

async function getJSON<T>(url: string): Promise<T> {
  const r = await fetch(url, { credentials: 'same-origin' });
  if (r.status === 401) toLogin();
  if (!r.ok) throw new HttpError(r.status);
  return (await r.json()) as T;
}

export function fetchStatus(): Promise<StatusPayload> {
  return getJSON<StatusPayload>(`${API}/status`);
}

export function fetchSeries(metric: string, range: string): Promise<SeriesPayload> {
  const params = new URLSearchParams({ metric, range });
  return getJSON<SeriesPayload>(`${API}/series?${params}`);
}

export function fetchLogs(opts: {
  q: string;
  operation: string;
  status: string;
  limit: number;
  offset: number;
}): Promise<LogsPayload> {
  const params = new URLSearchParams({
    q: opts.q,
    operation: opts.operation,
    status: opts.status,
    limit: String(opts.limit),
    offset: String(opts.offset)
  });
  return getJSON<LogsPayload>(`${API}/logs?${params}`);
}

export function fetchBucket(
  bucket: string,
  prefix: string,
  offset: number,
  pageSize: number
): Promise<BucketPayload> {
  const params = new URLSearchParams({
    prefix,
    delimiter: '/',
    offset: String(offset),
    page_size: String(pageSize)
  });
  return getJSON<BucketPayload>(`${API}/buckets/${encodeURIComponent(bucket)}?${params}`);
}

export function fetchObject(bucket: string, object: string): Promise<ObjectDetail> {
  // Key segment is a {key:path} on the backend — encode the bucket, but keep the
  // object's slashes intact (encodeURI, matching the old client).
  const url = `${API}/objects/${encodeURIComponent(bucket)}/${encodeURI(object)}`;
  return getJSON<ObjectDetail>(url);
}
