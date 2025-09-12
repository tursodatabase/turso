import { resolve } from 'path';
import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  build: {
    lib: {
      entry: resolve(__dirname, 'promise-bundle.ts'),
      name: 'database-browser',
      fileName: format => `main.${format}.js`,
      formats: ['es'],
    },
    rollupOptions: {
      output: {
        dir: 'bundle',
      }
    },
  },
});
