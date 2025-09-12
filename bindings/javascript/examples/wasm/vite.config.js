import { defineConfig, searchForWorkspaceRoot } from 'vite'

export default defineConfig({
  build: {
    minify: false, // Set this to false to disable minification
  },
  resolve: {
    alias: {
      '@tursodatabase/database-wasm32-wasi': '../../turso.wasi-browser.js'
    },
  },
  server: {
    fs: {
      allow: ['.']
    },
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    }
  },
  optimizeDeps: {
      exclude: [
          "@tursodatabase/database-wasm32-wasi",
      ]
  },
})
