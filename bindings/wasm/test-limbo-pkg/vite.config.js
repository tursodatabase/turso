import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  plugins: [wasm()],
  server: {
    headers: {
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Resource-Policy": "cross-origin",
      //Cross-Origin-Embedder-Policy: require-corp
    },
    fs: {
      allow: ["./node_modules/limbo-wasm/web/dist"],
    },
  },
  optimizeDeps: {
    exclude: ["limbo-wasm"],
  },
  worker: {
    format: "es",
    rollupOptions: {
      input: {
        main: "index.html",
        worker: "src/limbo-worker.js",
      },
      output: {
        format: "es",
      },
    },
  },
});
