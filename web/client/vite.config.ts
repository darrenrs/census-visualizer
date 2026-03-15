import { defineConfig, loadEnv, type ProxyOptions } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const escapeRegex = (value: string) =>
    value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const envDir = fileURLToPath(new URL("../..", import.meta.url));
  const env = loadEnv(mode, envDir, "");
  const basePathRaw = env.VITE_BASE_PATH || "/";
  const base = basePathRaw.endsWith("/") ? basePathRaw : `${basePathRaw}/`;
  const clientPort = Number(env.PORT_CLIENT || 5173);
  const apiBase = (env.VITE_API_BASE || "/api").replace(/\/+$/, "") || "/api";
  const geoBase = (env.VITE_GEO_BASE || "/geo").replace(/\/+$/, "") || "/geo";
  const apiTarget = `http://127.0.0.1:${Number(env.PORT_SERVER || 3000)}`;

  const proxy: Record<string, ProxyOptions> = {};

  if (apiBase.startsWith("/")) {
    const apiBaseRegex = new RegExp(`^${escapeRegex(apiBase)}(?=/|$)`);
    proxy[apiBase] = {
      target: apiTarget,
      rewrite: (path) => path.replace(apiBaseRegex, "/api"),
    };
  }

  if (geoBase.startsWith("/")) {
    proxy[geoBase] = {
      target: apiTarget,
    };
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
