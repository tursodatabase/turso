import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/compat/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  outDir: 'dist',
  target: 'es2020',
  clean: true,
  sourcemap: false,
  splitting: false,
  treeshake: true,
});
