import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The Go services don't send CORS headers, so in dev the browser talks to
// this origin only and Vite forwards to each service.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5171,
    proxy: {
      "/order-api": { target: "http://localhost:3001", rewrite: (p) => p.replace(/^\/order-api/, "") },
      "/trip-api": { target: "http://localhost:3002", rewrite: (p) => p.replace(/^\/trip-api/, "") },
      "/payment-api": { target: "http://localhost:3008", rewrite: (p) => p.replace(/^\/payment-api/, "") },
      "/pricing-api": { target: "http://localhost:3007", rewrite: (p) => p.replace(/^\/pricing-api/, "") },
      "/auth-api": { target: "http://localhost:3005", rewrite: (p) => p.replace(/^\/auth-api/, "") },
      "/ws": { target: "ws://localhost:3003", ws: true },
    },
  },
});
