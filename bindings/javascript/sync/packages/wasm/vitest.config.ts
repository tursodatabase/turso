import { defineConfig } from 'vitest/config'

export default defineConfig({
  define: {
    'process.env.NODE_DEBUG_NATIVE': 'false',
  },
  server: {
    headers: {
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cross-Origin-Opener-Policy": "same-origin"
    },
  },
  test: {
    globalSetup: './globalSetup.ts',
    browser: {
      enabled: true,
      provider: 'playwright',
      instances: [
        { browser: 'chromium' }
      ],
    },
  },
})
