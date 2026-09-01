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

//! Thread-safety bounds that hold off `wasm32` and vanish on it.
//!
//! The host-facing traits in this crate ([`RtcCommandSender`],
//! [`EncryptionKeySignalHandler`]) are implemented on native by types uniffi
//! moves between threads, and on `wasm32` by types holding a `JsValue`, which is
//! `!Send`, `!Sync`, and cannot be made otherwise. So a `Send + Sync` supertrait
//! is unsatisfiable on the web, while native needs it: it is what lets uniffi
//! spawn the futures an async export returns.
//!
//! matrix-rust-sdk solves the same problem with `SendOutsideWasm` /
//! `SyncOutsideWasm` in `matrix-sdk-common`.
//!
//! [`RtcCommandSender`]: crate::RtcCommandSender
//! [`EncryptionKeySignalHandler`]: crate::EncryptionKeySignalHandler

/// `Send + Sync` off `wasm32`, and no constraint at all on it.
///
/// Use as a supertrait wherever a host implements a trait this crate calls
/// into. Blanket-implemented, so no downstream type ever names it.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send + Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> MaybeSend for T {}

/// `Send + Sync` off `wasm32`, and no constraint at all on it.
///
/// Use as a supertrait wherever a host implements a trait this crate calls
/// into. Blanket-implemented, so no downstream type ever names it.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}
