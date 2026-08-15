import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [tailwindcss(), solid()],
  server: {
    port: 5173,
    // The worker imports wasm bindings from ../pkg outside this package root.
    fs: {
      allow: [".."],
    },
  },
  worker: {
    format: "es",
  },
  build: {
    target: "esnext",
  },
});
