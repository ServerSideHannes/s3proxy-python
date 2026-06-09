import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    // Local dev: proxy the dashboard API + SSE to a running proxy instance so the
    // app can use same-origin relative URLs exactly as it does in production.
    proxy: {
      '/dashboard/api': {
        target: process.env.PROXY_ORIGIN ?? 'http://localhost:8000',
        changeOrigin: true
      },
      '/dashboard/login': {
        target: process.env.PROXY_ORIGIN ?? 'http://localhost:8000',
        changeOrigin: true
      },
      '/dashboard/logout': {
        target: process.env.PROXY_ORIGIN ?? 'http://localhost:8000',
        changeOrigin: true
      }
    }
  }
});
