import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// ksqlDB's REST API sends no CORS headers, so in dev the browser talks to
// this origin only and Vite forwards to ksqlDB.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ksql-api": { target: "http://localhost:8088", rewrite: (p) => p.replace(/^\/ksql-api/, "") },
    },
  },
});
