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

//! LiveKit SFU media session.
//!
//! Wraps a connected LiveKit [`Room`] and surfaces its [`RoomEvent`] stream so
//! the host can react to participants and subscribed tracks. This phase is
//! **subscribe-only**: the session does not publish local media, which is the
//! default behaviour of a LiveKit room with `auto_subscribe` enabled and is the
//! shape a recording/transcription bot needs.

use livekit::{Room, RoomEvent, RoomOptions};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{Error, SfuToken};

/// A connected LiveKit session plus its event stream.
///
/// The [`UnboundedReceiver`] yields [`RoomEvent`]s (e.g.
/// [`RoomEvent::TrackSubscribed`]) for the lifetime of the connection.
pub struct LiveKitConnection {
    /// The connected session.
    pub session: LiveKitSession,
    /// Stream of room events (participants joining, tracks subscribed, ...).
    pub events: UnboundedReceiver<RoomEvent>,
}

/// A connected LiveKit SFU session.
pub struct LiveKitSession {
    room: Room,
}

impl LiveKitSession {
    /// Connect to the SFU using a previously obtained [`SfuToken`], with
    /// default [`RoomOptions`] (auto-subscribe, no local publication).
    pub async fn connect(token: &SfuToken) -> Result<LiveKitConnection, Error> {
        Self::connect_with_options(token, RoomOptions::default()).await
    }

    /// Connect to the SFU with caller-provided [`RoomOptions`].
    pub async fn connect_with_options(
        token: &SfuToken,
        options: RoomOptions,
    ) -> Result<LiveKitConnection, Error> {
        let (room, events) = Room::connect(&token.url, &token.jwt, options).await?;
        Ok(LiveKitConnection {
            session: LiveKitSession { room },
            events,
        })
    }

    /// Access the underlying LiveKit [`Room`] (participants, publications, ...).
    pub fn room(&self) -> &Room {
        &self.room
    }

    /// Disconnect from the SFU.
    pub async fn close(self) -> Result<(), Error> {
        self.room.close().await?;
        Ok(())
    }
}
