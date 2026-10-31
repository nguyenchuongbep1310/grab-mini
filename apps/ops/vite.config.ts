import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// ksqlDB's REST API and the Go services send no CORS headers, so in dev the
// browser talks to this origin only and Vite forwards to each of them.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ksql-api": { target: "http://localhost:8088", rewrite: (p) => p.replace(/^\/ksql-api/, "") },
      "/promotion-api": { target: "http://localhost:3010", rewrite: (p) => p.replace(/^\/promotion-api/, "") },
      "/auth-api": { target: "http://localhost:3005", rewrite: (p) => p.replace(/^\/auth-api/, "") },
    },
  },
});
