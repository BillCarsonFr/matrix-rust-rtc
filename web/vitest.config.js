import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Use node environment since we're testing Node.js bindings
    environment: 'node',
    // Global setup for tests
    globals: true,
    // Include test files
    include: ['test/**/*.test.mjs', 'test/**/*.test.js'],
    // Timeout for async tests
    testTimeout: 10000,
  },
});
