# S3 Encryption Proxy — Dashboard

The admin dashboard, rewritten as a SvelteKit single-page app and deployed
separately from the proxy data path (see issue #50). It consumes the proxy's
existing JSON + SSE API (`/dashboard/api/*`) — the backend is the contract; this
app does not reshape it.

- **Framework**: SvelteKit + `adapter-static` (pure SPA, hash-based in-app
  routing, no SSR).
- **Charts**: [uPlot](https://github.com/leeoniya/uPlot) — small and fast for
  the live time-series.
- **Auth**: session cookie + Basic Auth, identical to the proxy's flow. The
  login page posts to `/dashboard/api/login`.

## Layout

```
src/
  app.css                 design tokens + styles, ported 1:1 from the old UI
  lib/
    api.ts                fetch wrappers (relative URLs, 401 -> login, typed HttpError)
    loader.svelte.ts      shared load state: loading/errorStatus + stale-response guard
    status.svelte.ts      SSE feed + initial fetch
    chart.ts              uPlot wrapper matching the old chart look
    route.ts              hash-route parser (#, #logs, #metric=, #bucket=, ...)
    format.ts             number/time/byte formatting
    components/           Header, Footer, Dashboard, MetricView, BucketView,
                          ObjectView, LogsView, MetricCard, Sparkline
  routes/
    +page.svelte          the SPA shell + hash router
    login/+page.svelte    login page
nginx.conf                standalone serving config (chart overrides it)
Dockerfile                static build -> nginx
```

## Develop

Point the dev server at a running proxy that has the dashboard enabled
(`S3PROXY_DASHBOARD_UI=true`, a `S3PROXY_DASHBOARD_SECRET`, and credentials):

```bash
npm install
PROXY_ORIGIN=http://localhost:4433 npm run dev
```

`vite.config.ts` proxies `/dashboard/api`, `/dashboard/login`, and
`/dashboard/logout` to `PROXY_ORIGIN`, so the app uses the same relative URLs it
uses in production.

## Build

```bash
npm run build           # -> build/  (static, base path baked in)
npm run check           # svelte-check
```

The base path defaults to `/dashboard` and must match `dashboard.path` in the
Helm chart. Override at build time:

```bash
DASHBOARD_BASE_PATH=/ops/dash npm run build
```

## Deploy

The Helm chart (`chart/`) builds this into the `s3proxy-dashboard` image and runs
it as its own Deployment + Service. That pod's nginx serves the static build and
reverse-proxies `dashboard.path/api` (JSON + SSE) and the auth endpoints to the
proxy service — so the dashboard is single-origin behind one backend. Enable it
with `--set dashboard.enabled=true` (and a stable `dashboard.secret`).
