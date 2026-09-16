// The crate logs through Rust's `log` facade and has no output of its own:
// a host installs a LogSink. This one writes to the console, prefixed with
// the Rust module the line came from.
import { FfiLogLevel, type LogSink, setLogSink } from "./generated/matrix_rtc.js";

export class ConsoleLogSink implements LogSink {
  log(level: FfiLogLevel, target: string, message: string): void {
    const line = `[matrix-rtc ${target}] ${message}`;
    switch (level) {
      case FfiLogLevel.Error:
        console.error(line);
        break;
      case FfiLogLevel.Warn:
        console.warn(line);
        break;
      case FfiLogLevel.Info:
        console.info(line);
        break;
      default:
        console.debug(line);
    }
  }
}

/** Route the crate's log lines to the console, at `maxLevel` and above. */
export function installConsoleLogSink(maxLevel = FfiLogLevel.Debug): void {
  setLogSink(new ConsoleLogSink(), maxLevel);
}
