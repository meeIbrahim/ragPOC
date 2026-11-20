import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The center API on :8080. The dev server proxies the API paths so the SPA keeps one origin;
// the presigned PUT goes straight to MinIO, whose CORS allows :5173 (docker-compose.yml).
const api = "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/upload": api,
      "/documents": api,
      "/ask": api,
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
  },
});
