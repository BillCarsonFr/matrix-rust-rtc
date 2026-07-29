// Copyright 2026 Valere Fedronic
//
// This file is part of matrix-rust-rtc.
//
// matrix-rust-rtc is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// matrix-rust-rtc is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

//! On-the-fly user provisioning against the `demo/backend` Synapse.
//!
//! Users are registered through the plain client-server registration endpoint,
//! which the dev homeserver leaves open (`enable_registration_without_
//! verification`). Should that ever be turned off, the fallback is the admin
//! shared-secret flow — see `demo/backend/README.md`.

use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Credentials {
    pub user: String,
    pub password: String,
}

/// A suffix unique per test run, so reruns against a long-lived stack never
/// collide on usernames.
pub fn run_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    format!("{nanos:x}")
}

/// Register a fresh throwaway user `{prefix}-{suffix}`.
pub async fn register_user(
    http: &reqwest::Client,
    homeserver: &str,
    prefix: &str,
    suffix: &str,
) -> Result<Credentials, Box<dyn Error>> {
    let user = format!("{prefix}-{suffix}");
    let password = format!("test-{suffix}");

    let response = http
        .post(format!(
            "{}/_matrix/client/v3/register",
            homeserver.trim_end_matches('/')
        ))
        .json(&serde_json::json!({
            "username": user,
            "password": password,
            "auth": {"type": "m.login.dummy"},
            // The test logs in itself (it wants a fresh device + sync service),
            // so skip the access token this call would otherwise mint.
            "inhibit_login": true,
        }))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("registration of {user} failed: {status} {body}").into());
    }

    println!("[provision] registered {user}");
    Ok(Credentials { user, password })
}
