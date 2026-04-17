import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],

  // DuckDB-WASM requires SharedArrayBuffer which is gated behind COOP/COEP.
  // Set these headers in the dev server so the app behaves the same locally
  // as on Cloudflare Pages (where they are set via _headers file).
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "credentialless",
    },
  },

  // WASM and worker files are loaded from CDN at runtime — exclude from
  // pre-bundling and from the rollup output entirely.
  optimizeDeps: {
    exclude: ["@duckdb/duckdb-wasm"],
  },

  build: {
    target: "es2020",
  },
});
