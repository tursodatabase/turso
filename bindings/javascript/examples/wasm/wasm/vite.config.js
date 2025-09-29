import { defineConfig, searchForWorkspaceRoot } from 'vite'

export default defineConfig({
  server: {
    fs: {
      allow: ['.', '../../']
    },
      define: 
     {
        'process.env.NODE_DEBUG_NATIVE': 'false', // string replace at build-time
      },
          headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    }
  },
  optimizeDeps: {
      esbuildOptions: {
        define: { 'process.env.NODE_DEBUG_NATIVE': 'false' },
      },
  },
})
