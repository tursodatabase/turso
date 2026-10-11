import { defineConfig } from 'vitest/config'
import { playwright } from '@vitest/browser-playwright'

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
    testTimeout: 120_000,
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [
        { browser: 'chromium' },
        { browser: 'firefox' }
      ],
    },
  },
})
