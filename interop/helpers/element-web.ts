/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * Driving Element Web, and the Element Call widget it bundles.
 *
 * Adapted from Element Call's own `playwright/widget/test-helpers.ts` (AGPL,
 * element-hq/element-call, `livekit` branch). Every selector here is an
 * upstream UI string and is the most brittle part of this suite — they all
 * live in this one file on purpose. When Element Web's develop image moves and
 * the test breaks on a locator, this is the file to fix.
 */

import { expect, type Browser, type FrameLocator, type Page } from "@playwright/test";

import type { RegisteredUser } from "./register";

/** Element Call's RTC dialect selector, in the widget's Developer settings. */
export type RtcMode = "legacy" | "compat" | "2_0";

/**
 * Labels in Element Call's Developer tab, and what they mean for us:
 *
 * - `Compatibility: state events` ↔ `ElementCallCompat::StateEvents`
 * - `Matrix 2.0`                  ↔ `ElementCallCompat::StickyEvents`
 * - `Legacy: state events`        ↔ (not implemented on our side)
 */
const RTC_MODE_LABEL: Record<RtcMode, string> = {
  legacy: "Legacy: state events",
  compat: "Compatibility: state events",
  "2_0": "Matrix 2.0",
};

/** The Element Call widget's iframe. Its title comes from `element_call.brand`. */
export function callFrame(page: Page): FrameLocator {
  return page.locator('iframe[title="Element Call"]').contentFrame();
}

/** Log an already-registered user in through the Element Web sign-in form. */
export async function loginToElementWeb(
  browser: Browser,
  user: RegisteredUser,
): Promise<Page> {
  const context = await browser.newContext({ reducedMotion: "reduce" });
  const page = await context.newPage();
  await page.goto("/#/welcome");

  await page.getByRole("link", { name: "Sign in" }).click({ timeout: 30_000 });
  await page.getByRole("textbox", { name: "Username" }).fill(user.localpart, { timeout: 20_000 });
  await page.getByRole("textbox", { name: "Password" }).fill(user.password, { timeout: 20_000 });
  await page.getByRole("button", { name: "Sign in" }).click();

  // A fresh account lands on the welcome screen; this is also where Element
  // Web finishes bootstrapping cross-signing, which MSC4153 needs before any
  // media key we send will be accepted.
  await expect(page.getByRole("heading", { name: `Welcome ${user.localpart}` })).toBeVisible({
    timeout: 60_000,
  });
  await dismissStartupToasts(page);
  return page;
}

/**
 * Dismiss the toasts a fresh Element Web login stacks up. Only known ones —
 * an unknown toast is left alone rather than blind-clicked, since it may be
 * the actual failure.
 */
export async function dismissStartupToasts(page: Page): Promise<void> {
  const expected = [
    { title: "Failed to load service worker", button: "OK" },
    { title: "Back up your chats", button: "Dismiss" },
    { title: "Turn on key storage", button: "Dismiss" },
    { title: "Element does not support this browser", button: "Dismiss" },
  ];
  const toast = page.locator(".mx_Toast_toast");

  for (;;) {
    try {
      await toast.waitFor({ state: "visible", timeout: 1_000 });
      const title = await toast.locator(".mx_Toast_title h2").textContent();
      const match = expected.find((candidate) => title?.includes(candidate.title));
      if (!match) return;
      await toast.getByRole("button", { name: match.button }).click();
    } catch {
      return;
    }
  }
}

/** Accept a room invite from the room list, by room name. */
export async function acceptRoomInvite(page: Page, roomName: string): Promise<void> {
  await page.getByRole("option", { name: roomName }).click({ timeout: 60_000 });
  await page.getByRole("button", { name: "Accept" }).click({ timeout: 20_000 });
  await expect(page.getByRole("main").getByRole("heading", { name: roomName })).toBeVisible();
  await dismissStartupToasts(page);
}

/**
 * Open the widget, set Element Call's RTC dialect, and close the lobby again.
 *
 * Must run **before** joining: the dialect decides the carrier of a
 * membership, the SFU participant identity and the token endpoint all at once,
 * so it cannot be changed on a call in progress.
 */
export async function setRtcModeBeforeJoining(page: Page, mode: RtcMode): Promise<void> {
  await openCallWidget(page);

  const frame = callFrame(page);
  await frame.getByRole("button", { name: "Settings" }).click({ timeout: 60_000 });
  await frame.getByRole("tab", { name: "Preferences" }).click();
  // `.check()` rather than `.click()`: idempotent, so it will not toggle
  // developer mode back off if a previous step already enabled it.
  await frame.getByText("Developer mode", { exact: true }).check();

  await frame.getByRole("tab", { name: "Developer" }).click();
  await frame.getByText(RTC_MODE_LABEL[mode]).click();
  await frame.getByTestId("modal_close").click();

  await page.getByRole("button", { name: "Close lobby" }).click();
}

/**
 * Get Element Call into the call, whichever way Element Web is offering.
 *
 * Our Rust peer joins first, so Element Web may or may not have noticed an
 * ongoing call by now — and which it is depends on the dialect and on the
 * Element Web version. Rather than assert one of those paths (that is a
 * different feature, worth its own test), take whichever is on offer:
 *
 * 1. a "Group call started" toast with a Join button,
 * 2. an already-open widget sitting in its lobby,
 * 3. otherwise start the call from the room header.
 */
export async function joinCall(page: Page): Promise<void> {
  // `isVisible()` does not auto-wait, so give Element Web a moment to notice
  // the call our peer started before deciding which affordance to use.
  // Element Call's own helper does the same, for the same reason.
  await page.waitForTimeout(3_000);

  // The notification toast joins directly, with no lobby in between.
  const toastJoin = page.getByRole("alert").getByRole("button", { name: "Join" });
  if (await isVisible(toastJoin)) {
    await toastJoin.click();
    return;
  }

  await openCallWidget(page);
  await joinFromLobby(page);
}

/**
 * Get the Element Call widget on screen, whichever affordance Element Web is
 * currently offering: it is already open, or there is an in-room `Join` for a
 * call in progress, or the call has to be started from the room header.
 */
async function openCallWidget(page: Page): Promise<void> {
  const iframe = page.locator('iframe[title="Element Call"]');
  if (await isVisible(iframe)) return;

  const bannerJoin = page.getByRole("button", { name: "Join", exact: true });
  if (await isVisible(bannerJoin)) {
    await bannerJoin.click();
  } else {
    await page.getByRole("button", { name: "Video call" }).click({ timeout: 20_000 });
    // The call-type menu only appears when Element Web has more than one
    // option to offer (Jitsi configured, say). With Element Call as the only
    // one — or with a call already in progress — clicking goes straight to the
    // widget and no menu is ever rendered, so this is best-effort.
    await page
      .getByRole("menuitem", { name: "Element Call" })
      .click({ timeout: 5_000 })
      .catch(() => {});
  }

  await expect(iframe).toBeVisible({ timeout: 30_000 });
}

/** Immediate visibility check, without Playwright's auto-waiting. */
function isVisible(locator: { isVisible(): Promise<boolean> }): Promise<boolean> {
  return locator.isVisible().catch(() => false);
}

/** Click through Element Call's lobby ("join call") screen. */
export async function joinFromLobby(page: Page): Promise<void> {
  const joinButton = callFrame(page).getByTestId("lobby_joinCall");
  await expect(joinButton).toBeVisible({ timeout: 60_000 });
  await joinButton.click();
}
