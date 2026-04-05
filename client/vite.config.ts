import { defineConfig } from "vite";

export default defineConfig({
  server: {
    port: 5173,
    host: "0.0.0.0",
    proxy: {
      "/v1": {
        target: "http://localhost:3001",
        changeOrigin: true
      }
    }
  }
});
