// Loads the published build (`dist/`) for the acceptance suites, through the
// package's own Node entry point. Warnings and errors only: the crate is
// chatty at info while a test runs.
import { FfiLogLevel, initAsync } from "@element-hq/matrix-rtc";
import { installConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";

let sinkInstalled = false;

export async function initWasm(): Promise<void> {
  await initAsync();
  if (!sinkInstalled) {
    installConsoleLogSink(FfiLogLevel.Warn);
    sinkInstalled = true;
  }
}
