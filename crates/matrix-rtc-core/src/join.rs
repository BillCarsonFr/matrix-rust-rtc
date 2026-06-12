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

//! Join functionality for RTC sessions.
//!
//! This module provides the data structures and parameters needed for joining
//! an RTC session, including user information, transport configuration, and
//! call intent.

use crate::transport::RtcTransport;

/// Default keep-alive timeout in milliseconds (30 seconds).
///
/// This is the delay before the cleanup event would fire if not restarted.
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_MS: u64 = 30_000;

/// Parameters for joining an RTC session.
///
/// Contains all the information needed to construct and send a membership event
/// to join a call, including user identification, transport details, and call intent.
#[derive(Clone, Debug)]
pub struct JoinSessionParams {
    /// The Matrix user ID of the user joining the session (e.g., "@alice:example.org").
    pub user_id: String,

    /// The device ID of the user's device joining the session.
    ///
    /// This is used to create a unique sticky key for this membership.
    pub device_id: String,

    /// The membership ID (sticky key) for this join.
    ///
    /// This uniquely identifies this user/device's membership in the session.
    /// If not provided, it will be generated as "{user_id}-{device_id}".
    pub membership_id: Option<String>,

    /// The room ID where the session is taking place.
    pub room_id: String,

    /// The slot ID for the session (e.g., "m.call#ROOM").
    pub slot_id: String,

    /// The application type, usually "m.call".
    pub application: String,

    /// The RTC transport to use for this session.
    pub transport: RtcTransport,

    /// Keep-alive timeout in milliseconds.
    ///
    /// Defaults to `DEFAULT_KEEP_ALIVE_TIMEOUT_MS` if not specified.
    pub keep_alive_timeout_ms: Option<u64>,
}

impl JoinSessionParams {
    /// Creates new join parameters with defaults.
    ///
    /// The membership_id will be generated from user_id and device_id if not provided.
    pub fn new(
        user_id: String,
        device_id: String,
        room_id: String,
        slot_id: String,
        application: String,
        transport: RtcTransport,
    ) -> Self {
        Self {
            user_id,
            device_id,
            membership_id: None,
            room_id,
            slot_id,
            application,
            transport,
            keep_alive_timeout_ms: None,
        }
    }

    /// Gets the membership ID (sticky key) to use for this join.
    ///
    /// If a membership_id was explicitly set, returns that.
    /// Otherwise, generates one from user_id and device_id.
    pub fn membership_id(&self) -> String {
        self.membership_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", self.user_id, self.device_id))
    }

    /// Gets the keep-alive timeout to use.
    ///
    /// Returns the configured timeout or the default.
    pub fn keep_alive_timeout_ms(&self) -> u64 {
        self.keep_alive_timeout_ms
            .unwrap_or(DEFAULT_KEEP_ALIVE_TIMEOUT_MS)
    }

    /// Validates the parameters.
    ///
    /// Returns `Ok(())` if all required fields are present and valid.
    /// Returns `Err` with a description of the validation error otherwise.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.user_id.is_empty() {
            return Err("user_id is required");
        }
        if self.device_id.is_empty() {
            return Err("device_id is required");
        }
        if self.room_id.is_empty() {
            return Err("room_id is required");
        }
        if self.slot_id.is_empty() {
            return Err("slot_id is required");
        }
        if self.application.is_empty() {
            return Err("application is required");
        }
        Ok(())
    }
}

/// Parameters for leaving an RTC session.
#[derive(Clone, Debug, Default)]
pub struct LeaveSessionParams {
    /// Optional disconnect reason (e.g., "user_left", "ice_failed").
    pub disconnect_reason: Option<String>,
}

impl LeaveSessionParams {
    /// Creates new leave parameters with no disconnect reason.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates new leave parameters with a disconnect reason.
    pub fn with_reason(reason: String) -> Self {
        Self {
            disconnect_reason: Some(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LiveKitTransport, RtcTransport};

    #[test]
    fn test_membership_id_generation() {
        let params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );

        assert_eq!(params.membership_id(), "@alice:example.org-device123");
    }

    #[test]
    fn test_explicit_membership_id() {
        let mut params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );
        params.membership_id = Some("custom-id".to_string());

        assert_eq!(params.membership_id(), "custom-id");
    }

    #[test]
    fn test_keep_alive_timeout_default() {
        let params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );

        assert_eq!(
            params.keep_alive_timeout_ms(),
            DEFAULT_KEEP_ALIVE_TIMEOUT_MS
        );
    }

    #[test]
    fn test_keep_alive_timeout_custom() {
        let mut params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );
        params.keep_alive_timeout_ms = Some(60_000);

        assert_eq!(params.keep_alive_timeout_ms(), 60_000);
    }

    #[test]
    fn test_validate_success() {
        let params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );

        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_user_id() {
        let params = JoinSessionParams::new(
            "".to_string(),
            "device123".to_string(),
            "!room:example.org".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );

        assert_eq!(params.validate(), Err("user_id is required"));
    }

    #[test]
    fn test_validate_empty_room_id() {
        let params = JoinSessionParams::new(
            "@alice:example.org".to_string(),
            "device123".to_string(),
            "".to_string(),
            "m.call#ROOM".to_string(),
            "m.call".to_string(),
            RtcTransport::LiveKit(LiveKitTransport {
                livekit_service_url: "https://example.com".to_string(),
            }),
        );

        assert_eq!(params.validate(), Err("room_id is required"));
    }
}
