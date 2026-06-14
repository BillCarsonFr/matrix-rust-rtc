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

//! RTC transport types for MatrixRTC.
//!
//! This module defines the transport types that can appear in `m.rtc.member` events
//! as specified in MSC4143 and MSC4195.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single RTC transport specification from an m.rtc.member event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RtcTransport {
    /// LiveKit SFU transport as defined in MSC4195.
    LiveKit(LiveKitTransport),
    /// An unsupported or unknown transport type.
    /// Holds the raw transport data for forward compatibility.
    Unsupported(UnsupportedTransport),
}

/// LiveKit-specific transport configuration (MSC4195).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveKitTransport {
    /// URL of the service that issues JWT tokens for connecting to the LiveKit SFU.
    pub livekit_service_url: String,
}

/// An unsupported transport type, storing raw data for forward compatibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedTransport {
    /// The transport type string, e.g., "webrtc", "p2p", etc.
    pub transport_type: String,
    /// The raw JSON fields for this transport (excluding "type").
    pub extra_fields: BTreeMap<String, serde_json::Value>,
}

/// Raw transport data as it appears in the JSON, before parsing into typed variants.
/// Used for deserialization from SDK events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawRtcTransport {
    /// The transport type, e.g., "livekit".
    #[serde(rename = "type")]
    pub transport_type: String,
    /// Additional transport-specific fields.
    #[serde(flatten)]
    pub extra_fields: BTreeMap<String, serde_json::Value>,
}

impl RawRtcTransport {
    /// Convert into a typed RtcTransport.
    /// Known transport types are parsed into their specific variants,
    /// while unknown types become UnsupportedTransport.
    pub fn into_typed(self) -> RtcTransport {
        match self.transport_type.as_str() {
            "livekit" => {
                let url = self
                    .extra_fields
                    .get("livekit_service_url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if let Some(url) = url {
                    RtcTransport::LiveKit(LiveKitTransport {
                        livekit_service_url: url,
                    })
                } else {
                    // LiveKit transport without required field -> unsupported
                    RtcTransport::Unsupported(UnsupportedTransport {
                        transport_type: self.transport_type,
                        extra_fields: self.extra_fields,
                    })
                }
            }
            _ => RtcTransport::Unsupported(UnsupportedTransport {
                transport_type: self.transport_type,
                extra_fields: self.extra_fields,
            }),
        }
    }
}
