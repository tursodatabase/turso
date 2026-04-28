import { defineConfig } from 'vitest/config'

const DEFAULT_LOCAL_SYNC_SERVER_URL = 'http://localhost:10001';
const tursoDbUrl = process.env.VITE_TURSO_DB_URL || DEFAULT_LOCAL_SYNC_SERVER_URL;

export default defineConfig({
  define: {
    'process.env.NODE_DEBUG_NATIVE': 'false',
    'process.env.VITE_TURSO_DB_URL': JSON.stringify(tursoDbUrl),
  },
  server: {
    headers: {
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cross-Origin-Opener-Policy": "same-origin"
    },
  },
  test: {
    globalSetup: './turso-server-setup.ts',
    browser: {
      enabled: true,
      provider: 'playwright',
      instances: [
        { browser: 'chromium' },
        { browser: 'firefox' }
      ],
    },
  },
})
