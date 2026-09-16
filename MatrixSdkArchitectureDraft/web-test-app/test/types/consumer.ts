// Compiled with `skipLibCheck: false` against dist/ — proves the shipped
// .d.ts files are valid TypeScript and that every subpath export types.
import { initAsync, isInitialized, defaultWasmUrl, FfiElementCallCompat, computeSessionsFromEvents } from "@element-hq/matrix-rtc";
import { installConsoleLogSink, ConsoleLogSink } from "@element-hq/matrix-rtc/log-sink";
import { JsSdkMatrixDriver, createJsSdkBackend } from "@element-hq/matrix-rtc/driver/matrix-js-sdk";
import { MockMatrixDriver, ROOM_ID } from "@element-hq/matrix-rtc/testing";

export async function smoke(): Promise<number> {
  await initAsync(defaultWasmUrl());
  if (!isInitialized()) throw new Error("not initialised");
  installConsoleLogSink();
  const sink: ConsoleLogSink = new ConsoleLogSink();
  void sink;
  const driver: MockMatrixDriver = new MockMatrixDriver();
  void driver;
  void JsSdkMatrixDriver;
  void createJsSdkBackend;
  void ROOM_ID;
  return computeSessionsFromEvents([], FfiElementCallCompat.Off).length;
}
