// Pure SPA: the dashboard is driven by a live JSON/SSE API and uses hash-based
// in-page routing, so there is nothing to prerender or server-render. Emit a
// single index.html (via adapter-static fallback) and run everything client-side.
export const ssr = false;
export const prerender = false;
