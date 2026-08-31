/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * The web half of the interop test, as a browser page.
 *
 * Wraps `web/demo` in test mode (`?test=1`): commands go in through
 * `window.webPeer.command`, JSON events come back through an exposed
 * `window.__onWebPeerEvent`. Deliberately symmetrical to {@link RustPeer} —
 * same `waitFor` semantics (seen-replay, error events reject, page loss
 * rejects), same disposal shape — so a spec reads the same whichever peer it
 * drives.
 */

import type { Browser, BrowserContext, Page } from "@playwright/test";

import type { PeerEvent } from "./rust-peer";

/** Where the vite `webServer` in playwright.config.ts serves the demo. */
export const WEB_PEER_URL = process.env.WEB_PEER_URL ?? "http://localhost:5199";

export class WebPeer {
  /** Everything seen so far, so `waitFor` can match events already past. */
  private readonly seen: PeerEvent[] = [];
  private readonly waiters: Array<(event: PeerEvent) => void> = [];
  private gone?: string;
  /** The page's own log lines, verbatim, for the test report. */
  public log = "";

  private constructor(
    private readonly context: BrowserContext,
    public readonly page: Page,
  ) {}

  static async open(browser: Browser, url: string = WEB_PEER_URL): Promise<WebPeer> {
    const context = await browser.newContext();
    const page = await context.newPage();
    const peer = new WebPeer(context, page);

    page.on("console", (message) => {
      peer.log += `[console.${message.type()}] ${message.text()}\n`;
    });
    page.on("pageerror", (error) => {
      peer.log += `[pageerror] ${error.message}\n`;
    });
    page.on("crash", () => peer.abort("page crashed"));
    page.on("close", () => peer.abort("page closed"));

    await page.exposeFunction("__onWebPeerEvent", (line: string) => {
      let parsed: PeerEvent;
      try {
        parsed = JSON.parse(line) as PeerEvent;
      } catch {
        peer.log += `[unparsed event] ${line}\n`;
        return;
      }
      peer.seen.push(parsed);
      for (const waiter of peer.waiters.splice(0)) waiter(parsed);
    });

    await page.goto(`${url}/?test=1`);
    await peer.waitFor("page_ready", { timeout: 30_000 });
    return peer;
  }

  private abort(reason: string): void {
    if (this.gone) return;
    this.gone = reason;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ event: "__gone", reason });
    }
  }

  /**
   * Send a command and resolve when the page has finished executing it. Most
   * outcomes should still be asserted through `waitFor` on the corresponding
   * event, like the rust peer.
   */
  async send(command: Record<string, unknown>): Promise<void> {
    if (this.gone) throw new Error(`web peer is gone (${this.gone}), cannot send ${command.cmd}`);
    await this.page.evaluate(
      (json) => (window as never as { webPeer: { command(c: string): Promise<unknown> } }).webPeer.command(json),
      JSON.stringify(command),
    );
  }

  /**
   * Resolve with the first event of `name` satisfying `predicate` — including
   * one already received before this call, so a test can never lose a race
   * against a fast page.
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
    if (error) throw new Error(`web peer reported an error: ${JSON.stringify(error)}`);

    return new Promise<PeerEvent>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(
          new Error(
            `timed out after ${timeout}ms waiting for web peer event "${name}".\n` +
              `Events seen: ${JSON.stringify(this.seen)}\n` +
              `--- page log ---\n${this.log}`,
          ),
        );
      }, timeout);

      const onEvent = (event: PeerEvent) => {
        if (matches(event)) {
          cleanup();
          resolve(event);
        } else if (event.event === "error") {
          cleanup();
          reject(new Error(`web peer reported an error: ${JSON.stringify(event)}`));
        } else if (event.event === "__gone") {
          cleanup();
          reject(
            new Error(
              `web peer page is gone (${event.reason}) while waiting for "${name}".\n` +
                `--- page log ---\n${this.log}`,
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

  /** Hang up if still in a call, then close the page's context. */
  async dispose(timeout = 30_000): Promise<void> {
    if (!this.gone) {
      try {
        await Promise.race([
          this.send({ cmd: "leave" }),
          new Promise((resolve) => setTimeout(resolve, timeout)),
        ]);
      } catch {
        // Leaving is best-effort; the report has the page log.
      }
    }
    await this.context.close();
  }
}
