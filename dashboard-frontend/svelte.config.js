import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({
      pages: 'build',
      assets: 'build',
      fallback: 'index.html',
      precompress: false,
      strict: true
    }),
    // The dashboard is mounted under dashboard.path (default /dashboard) by the ingress.
    // Built assets are served from there, so the app base must match.
    paths: {
      base: process.env.DASHBOARD_BASE_PATH ?? '/dashboard'
    }
  }
};

export default config;
