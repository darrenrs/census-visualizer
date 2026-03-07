import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  base: "/census-visualizer/",
  plugins: [react()],
  server: {
    proxy: {
      "/census-visualizer/api": {
        target: "http://127.0.0.1:3020",
        rewrite: (path) => path.replace(/^\/census-visualizer/, ""),
      },
    },
  },
});
