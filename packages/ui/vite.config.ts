import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Library build: `pnpm --filter @grab-mini/ui build` emits dist/ (ES module + style.css + .d.ts).
// Apps inside this workspace import the TypeScript source directly (see package.json "exports").
export default defineConfig({
  plugins: [react()],
  build: {
    lib: { entry: "src/index.ts", formats: ["es"], fileName: "index", cssFileName: "style" },
    rollupOptions: {
      external: ["react", "react-dom", "react/jsx-runtime", "leaflet", "react-leaflet"],
    },
  },
});
