import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

// The suites import the package by name and run against `dist/` — the build
// output that gets published — never against the TypeScript sources.
const dist = (p: string) => resolve(import.meta.dirname, "dist", p);

export default defineConfig({
  resolve: {
    alias: [
      { find: /^@element-hq\/matrix-rtc$/, replacement: dist("index.node.js") },
      { find: /^@element-hq\/matrix-rtc\/log-sink$/, replacement: dist("log-sink.js") },
      { find: /^@element-hq\/matrix-rtc\/driver\/matrix-js-sdk$/, replacement: dist("drivers/matrix-js-sdk.js") },
      { find: /^@element-hq\/matrix-rtc\/testing$/, replacement: dist("testing/mock-driver.js") },
    ],
  },
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
