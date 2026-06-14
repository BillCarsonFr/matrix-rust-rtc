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

//! Error types for the MatrixRTC core crate.
//!
//! This module defines error types used throughout the core crate,
//! particularly for command execution and session operations.

use thiserror::Error;

/// Errors that can occur when executing commands via the `RtcCommandSender`.
#[derive(Debug, Error)]
pub enum CommandError {
    /// The command was rejected by the client SDK.
    #[error("command rejected by client: {0}")]
    ClientRejected(String),

    /// Failed to serialize event content to JSON.
    #[error("failed to serialize event content: {0}")]
    SerializationError(String),

    /// The room ID is invalid or missing.
    #[error("invalid room ID")]
    InvalidRoomId,

    /// The event type is invalid or unsupported.
    #[error("invalid event type: {0}")]
    InvalidEventType(String),

    /// Failed to send the event (network or SDK error).
    #[error("failed to send event: {0}")]
    SendError(String),

    /// Failed to schedule the delayed event.
    #[error("failed to schedule delayed event: {0}")]
    SchedulingError(String),

    /// Failed to cancel the delayed event (event ID not found or already fired).
    #[error("failed to cancel delayed event: {0}")]
    CancelError(String),

    /// The client SDK does not support delayed events.
    #[error("delayed events not supported by client")]
    DelayedEventsNotSupported,
}

/// Errors that can occur when attempting to join an RTC session.
#[derive(Debug, Error)]
pub enum JoinError {
    /// A command execution error occurred while joining.
    #[error("command error while joining: {0}")]
    CommandError(#[from] CommandError),

    /// The session is already joined with the given membership ID.
    #[error("already joined with membership ID: {0}")]
    AlreadyJoined(String),

    /// Required parameter is missing.
    #[error("missing required parameter: {0}")]
    MissingParameter(&'static str),

    /// Invalid transport configuration.
    #[error("invalid transport configuration")]
    InvalidTransport,
}

/// Errors that can occur when attempting to leave an RTC session.
#[derive(Debug, Error)]
pub enum LeaveError {
    /// A command execution error occurred while leaving.
    #[error("command error while leaving: {0}")]
    CommandError(#[from] CommandError),

    /// The session is not currently joined.
    #[error("not joined")]
    NotJoined,
}

impl CommandError {
    /// Create a generic command error from a string message.
    pub fn from_message(msg: impl Into<String>) -> Self {
        CommandError::SendError(msg.into())
    }
}
