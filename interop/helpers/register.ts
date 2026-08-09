/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

/**
 * User provisioning for the interop test.
 *
 * Uses plain **open registration**, which the dev homeserver leaves on
 * (`enable_registration_without_verification`) — the same endpoint
 * `crates/matrix-rtc-livekit/tests/e2e_call/provision.rs` calls, so both halves
 * of this test create users the same way.
 *
 * Element Call's own Playwright suite uses the Synapse *admin* HMAC API
 * instead. That would mean porting the nonce/HMAC dance and duplicating
 * `registration_shared_secret` in a second place, to gain nothing we need here.
 */

export const HOMESERVER_URL = process.env.HOMESERVER_URL ?? "https://synapse.m.localhost";

export interface RegisteredUser {
  /** Localpart, which is what the Element Web sign-in form wants. */
  localpart: string;
  /** Full Matrix ID, which is what the Rust peer invites. */
  userId: string;
  password: string;
}

/**
 * Register a throwaway user `{prefix}-{unique}`.
 *
 * The suffix keeps reruns against a long-lived stack from colliding — the same
 * reason `provision.rs` has one.
 */
export async function registerUser(prefix: string): Promise<RegisteredUser> {
  const suffix = `${Date.now().toString(16)}${Math.floor(Math.random() * 0x1000).toString(16)}`;
  const localpart = `${prefix}-${suffix}`;
  const password = `test-${suffix}`;

  const response = await fetch(`${HOMESERVER_URL.replace(/\/$/, "")}/_matrix/client/v3/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: localpart,
      password,
      auth: { type: "m.login.dummy" },
      // Element Web logs in itself, for its own device.
      inhibit_login: true,
    }),
  });

  if (!response.ok) {
    throw new Error(
      `registration of ${localpart} failed: ${response.status} ${await response.text()}`,
    );
  }

  const body = (await response.json()) as { user_id?: string };
  const userId = body.user_id;
  if (!userId) {
    throw new Error(`registration of ${localpart} returned no user_id`);
  }
  return { localpart, userId, password };
}
