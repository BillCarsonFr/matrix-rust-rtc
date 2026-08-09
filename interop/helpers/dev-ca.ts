/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

import { existsSync } from "node:fs";
import { join } from "node:path";

/**
 * The CA the interop stack mints into `demo/backend/data/tls/` at up time, or
 * `undefined` if it is not there (the stack is down, or the certificate came
 * from mkcert, whose CA is already trusted machine-wide).
 */
export function devCaPath(): string | undefined {
  const path = join(__dirname, "..", "..", "demo", "backend", "data", "tls", "local-ca.crt");
  return existsSync(path) ? path : undefined;
}
