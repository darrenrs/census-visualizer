import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const envDir = fileURLToPath(new URL("../..", import.meta.url));
  const env = loadEnv(mode, envDir, "");
  const base = env.VITE_BASE_PATH || "/";
  const clientPort = Number(env.PORT_CLIENT || 5173);
  const geoBase = env.VITE_GEO_BASE || "/geo";
  const apiTarget =
    env.VITE_DEV_API_TARGET ||
    `http://127.0.0.1:${Number(env.PORT_SERVER || 3000)}`;

  const proxy: Record<string, { target: string }> = {
    "/api": { target: apiTarget },
  };

  if (geoBase.startsWith("/")) {
    proxy[geoBase] = { target: apiTarget };
  }

  return {
    envDir,
    base,
    plugins: [react()],
    resolve: {
      alias: {
        "@": fileURLToPath(new URL("./src", import.meta.url)),
      },
    },
    server: {
      host: "127.0.0.1",
      port: clientPort,
      proxy,
    },
  };
});
