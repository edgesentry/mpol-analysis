import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { VitePWA } from "vite-plugin-pwa";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(),
    VitePWA({
      registerType: "autoUpdate",
      // Inject the SW registration snippet into index.html automatically.
      injectRegister: "auto",
      // Include all built assets in the precache manifest.
      includeAssets: ["icon-192.svg", "favicon.ico"],
      manifest: {
        name: "MPOL Watchlist",
        short_name: "MPOL",
        description: "Maritime Pattern of Life — vessel risk watchlist",
        theme_color: "#1a1f2e",
        background_color: "#0f1117",
        display: "standalone",
        orientation: "landscape",
        icons: [
          {
            src: "icon-192.svg",
            sizes: "192x192",
            type: "image/svg+xml",
            purpose: "any maskable",
          },
        ],
      },
      workbox: {
        // Precache the entire built bundle (JS, CSS, HTML).
        // Cache-first: app shell loads instantly offline.
        globPatterns: ["**/*.{js,css,html,ico,svg}"],
        // Service worker claims clients immediately on activation.
        clientsClaim: true,
        skipWaiting: true,
        // Runtime caching rules.
        runtimeCaching: [
          {
            // DuckDB WASM + worker files from jsDelivr CDN.
            // Cache-first with a 30-day TTL — these rarely change for a pinned version.
            urlPattern: /^https:\/\/cdn\.jsdelivr\.net\/npm\/@duckdb\/duckdb-wasm/,
            handler: "CacheFirst",
            options: {
              cacheName: "duckdb-wasm-cdn",
              expiration: { maxAgeSeconds: 30 * 24 * 60 * 60 },
              cacheableResponse: { statuses: [0, 200] },
            },
          },
          {
            // R2 manifest — network-first so the browser always checks for
            // updated data, but falls back to cache when offline.
            urlPattern: /^https:\/\/arktrace-public\.edgesentry\.io\/ducklake_manifest\.json/,
            handler: "NetworkFirst",
            options: {
              cacheName: "r2-manifest",
              expiration: { maxAgeSeconds: 60 * 60 },
              cacheableResponse: { statuses: [0, 200] },
            },
          },
          {
            // R2 Parquet files — served from OPFS by the app layer, but also
            // cached here as a secondary offline layer for the raw bytes.
            urlPattern: /^https:\/\/arktrace-public\.edgesentry\.io\/data\//,
            handler: "CacheFirst",
            options: {
              cacheName: "r2-parquet",
              expiration: {
                maxEntries: 50,
                maxAgeSeconds: 7 * 24 * 60 * 60,
              },
              cacheableResponse: { statuses: [0, 200] },
            },
          },
        ],
      },
    }),
  ],

  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "credentialless",
    },
  },

  optimizeDeps: {
    exclude: ["@duckdb/duckdb-wasm"],
  },

  build: {
    target: "es2020",
  },
});
