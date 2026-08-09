/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * The Rust half of the interop test, as a child process.
 *
 * Wraps `crates/matrix-rtc-livekit/examples/interop_peer.rs`: commands go in on
 * stdin, JSON-line events come back on stdout, human logs on stderr. See that
 * file for the protocol.
 */

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";

import { devCaPath } from "./dev-ca";

export interface PeerEvent {
  event: string;
  [key: string]: unknown;
}

const DEFAULT_BIN = join(__dirname, "..", "..", "target", "debug", "examples", "interop_peer");

export class RustPeer {
  private readonly child: ChildProcessWithoutNullStreams;
  /** Everything seen so far, so `waitFor` can match events already past. */
  private readonly seen: PeerEvent[] = [];
  private readonly waiters: Array<(event: PeerEvent) => void> = [];
  private exited?: { code: number | null; signal: NodeJS.Signals | null };
  /** Kept verbatim so a failing test can attach it to the report. */
  public stderr = "";

  private constructor(child: ChildProcessWithoutNullStreams) {
    this.child = child;

    createInterface({ input: child.stdout }).on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed.startsWith("{")) {
        // Not protocol: keep it, rather than silently dropping something that
        // may explain a failure.
        this.stderr += `[peer stdout] ${trimmed}\n`;
        return;
      }
      let parsed: PeerEvent;
      try {
        parsed = JSON.parse(trimmed) as PeerEvent;
      } catch {
        this.stderr += `[peer stdout, unparsed] ${trimmed}\n`;
        return;
      }
      this.seen.push(parsed);
      for (const waiter of this.waiters.splice(0)) waiter(parsed);
    });

    createInterface({ input: child.stderr }).on("line", (line) => {
      this.stderr += `${line}\n`;
    });

    child.on("exit", (code, signal) => {
      this.exited = { code, signal };
      // Unblock anything waiting, so the failure is "peer exited" and not a
      // timeout that says nothing.
      for (const waiter of this.waiters.splice(0)) {
        waiter({ event: "__exited", code, signal });
      }
    });
  }

  static spawn(env: Record<string, string>): RustPeer {
    const bin = process.env.INTEROP_PEER_BIN ?? DEFAULT_BIN;
    if (!existsSync(bin)) {
      throw new Error(
        `interop peer binary not found at ${bin}. Build it with:\n` +
          `  cargo build -p matrix-rtc-livekit --features matrix-sdk,testing --example interop_peer`,
      );
    }
    // The peer validates the stack's dev CA on two legs: the homeserver
    // (reqwest) and the wss:// SFU connection (livekit). Both go through
    // rustls-native-certs, which — on every platform, macOS included — loads
    // *only* from SSL_CERT_FILE when it is set, in place of the platform
    // store. Scoping it to this child process means the test needs no
    // machine-wide trust install; the peer talks to nothing but this stack, so
    // replacing its root store outright is not a limitation.
    const ca = process.env.SSL_CERT_FILE ?? devCaPath();
    const child = spawn(bin, [], {
      env: { ...process.env, ...(ca ? { SSL_CERT_FILE: ca } : {}), ...env },
      stdio: ["pipe", "pipe", "pipe"],
    }) as ChildProcessWithoutNullStreams;
    return new RustPeer(child);
  }

  send(command: string): void {
    if (this.exited) throw new Error(`peer already exited, cannot send ${command}`);
    this.child.stdin.write(`${command}\n`);
  }

  /**
   * Resolve with the first event of `name` satisfying `predicate` — including
   * one already received before this call, so a test can never lose a race
   * against a fast peer.
   */
  async waitFor(
    name: string,
    options: { timeout?: number; predicate?: (event: PeerEvent) => boolean } = {},
  ): Promise<PeerEvent> {
    const { timeout = 90_000, predicate = () => true } = options;
    const matches = (event: PeerEvent) => event.event === name && predicate(event);

    const already = this.seen.find(matches);
    if (already) return already;

    const error = this.seen.find((event) => event.event === "error");
    if (error) throw new Error(`peer reported an error: ${JSON.stringify(error)}`);

    return new Promise<PeerEvent>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(
          new Error(
            `timed out after ${timeout}ms waiting for peer event "${name}".\n` +
              `Events seen: ${JSON.stringify(this.seen)}\n` +
              `--- peer stderr ---\n${this.stderr}`,
          ),
        );
      }, timeout);

      const onEvent = (event: PeerEvent) => {
        if (matches(event)) {
          cleanup();
          resolve(event);
        } else if (event.event === "error") {
          cleanup();
          reject(new Error(`peer reported an error: ${JSON.stringify(event)}`));
        } else if (event.event === "__exited") {
          cleanup();
          reject(
            new Error(
              `peer exited (code ${event.code}, signal ${event.signal}) while waiting for "${name}".\n` +
                `--- peer stderr ---\n${this.stderr}`,
            ),
          );
        } else {
          // Not ours: keep listening.
          this.waiters.push(onEvent);
        }
      };
      const cleanup = () => {
        clearTimeout(timer);
        const index = this.waiters.indexOf(onEvent);
        if (index >= 0) this.waiters.splice(index, 1);
      };

      this.waiters.push(onEvent);
    });
  }

  /** Ask the peer to quit, and wait for it. Kills it if it will not go. */
  async dispose(timeout = 30_000): Promise<void> {
    if (this.exited) return;
    try {
      this.send("quit");
    } catch {
      // Already gone.
    }
    this.child.stdin.end();

    const exited = new Promise<void>((resolve) => {
      if (this.exited) return resolve();
      this.child.once("exit", () => resolve());
    });
    const killer = setTimeout(() => this.child.kill("SIGKILL"), timeout);
    await exited;
    clearTimeout(killer);
  }
}
