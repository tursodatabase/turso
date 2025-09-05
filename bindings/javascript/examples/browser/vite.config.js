import { defineConfig, searchForWorkspaceRoot } from 'vite'

export default defineConfig({
  resolve: {
      alias: {
          "@tursodatabase/database-wasm32-wasi": "../../npm/wasm32-wasi"
      }
  },
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
    fs: {
      allow: ['.', '../../']
    },
      define: 
     {
        'process.env.NODE_DEBUG_NATIVE': 'false', // string replace at build-time
      }
  },
  optimizeDeps: {
      exclude: [
          "@tursodatabase/database-wasm32-wasi",
      ],
      esbuildOptions: {
        define: { 'process.env.NODE_DEBUG_NATIVE': 'false' },
      },
  },
})
