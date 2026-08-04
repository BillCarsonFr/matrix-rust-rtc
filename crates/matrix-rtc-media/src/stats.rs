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

//! Receive-side statistics for a subscribed remote track.
//!
//! These exist to make one specific failure diagnosable from outside the
//! library. The receive path produces frames at a fixed cadence whether or not
//! RTP is arriving — an audio track with no incoming packets still emits 10 ms
//! buffers, filled by the jitter buffer's concealment (silence). So "the call
//! is silent" and "the call is silent *because nothing is arriving*" look
//! identical at the frame level, and telling them apart used to mean reading
//! this crate's own log output.
//!
//! The counters below are cumulative since subscription and come straight from
//! the transport's RTP layer, so a host can distinguish:
//!
//! - **Nothing arriving** — [`ReceiveStats::packets_received`] flat across two
//!   samples. Network, subscription, or SFU-side problem.
//! - **Arriving but not decrypting** — packets climbing while
//!   [`ReceiveStats::frames_decoded`] (video) stays flat, or
//!   [`ReceiveStats::concealed_samples`] climbs in step with
//!   [`ReceiveStats::total_samples_received`] (audio). Usually a key problem,
//!   corroborated by [`CallEvent::FrameEncryptionState`].
//! - **Arriving and decoding, but lossy** — packets and frames both climbing
//!   with [`ReceiveStats::packets_lost`] or `jitter` rising.
//!
//! Sample twice and compare: every field is a monotonic total, not a rate.
//!
//! [`CallEvent::FrameEncryptionState`]: crate::event::CallEvent::FrameEncryptionState

/// Cumulative receive-side counters for one subscribed track.
///
/// Fields that don't apply to the track's media kind stay `0` (a host reading
/// `concealed_samples` on video learns nothing). Transports report what their
/// RTP layer exposes; see the module docs for how to read them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReceiveStats {
    /// RTP packets received on this track since subscribing. Flat across two
    /// samples means nothing is arriving at all.
    pub packets_received: u64,
    /// Packets the receiver expected and never got. Signed: reordering can
    /// briefly make it negative.
    pub packets_lost: i64,
    /// Payload bytes received.
    pub bytes_received: u64,
    /// Packet-arrival jitter in seconds.
    pub jitter: f64,
    /// Video frames the decoder produced. Flat while `packets_received`
    /// climbs is the signature of frames arriving but not decrypting.
    pub frames_decoded: u64,
    /// Video frames dropped before rendering (late, or the consumer is slow).
    pub frames_dropped: u64,
    /// Audio samples handed to the output, whether real or concealed.
    pub total_samples_received: u64,
    /// Audio samples the jitter buffer invented because the real ones never
    /// arrived. Climbing in step with `total_samples_received` means the
    /// "audio" being played is entirely fabricated.
    pub concealed_samples: u64,
    /// The subset of `concealed_samples` that was emitted as pure silence
    /// rather than interpolated from neighbouring audio.
    pub silent_concealed_samples: u64,
    /// How many separate times concealment kicked in — a better gap counter
    /// than the sample totals, which one long outage inflates.
    pub concealment_events: u64,
}
