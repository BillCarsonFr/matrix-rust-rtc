import { defineConfig } from "vite";

export default defineConfig({
  // the generated entrypoint imports the .wasm as an asset URL
  assetsInclude: ["**/*.wasm"],
});
