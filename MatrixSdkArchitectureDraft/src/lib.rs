//! # matrix-rtc — architecture draft skeleton
//!
//! One crate, one Matrix seam ([`driver::MatrixDriver`]), no media plane.
//! The SDK's outputs are the [`connections::ConnectionData`] list and the
//! [`encryption::KeyMap`]; the host feeds both into its own LiveKit SDK.
//!
//! One binding surface ([`uniffi_api`]) serves every platform: Swift and
//! Kotlin via uniffi-bindgen, React Native and web/wasm via
//! uniffi-bindgen-react-native — so the generated (and documented) API is
//! identical on all of them.
//!
//! Plans and status per module: `src/session/SessionImplementationPlan.md`,
//! `src/own_membership/OwnMembershipImplementationPlan.md`,
//! `src/encryption/README.md`, `src/participation/ParticipationImplementationPlan.md`.

pub mod connections;
pub mod driver;
pub mod encryption;
pub mod executor;
pub mod own_membership;
pub mod participation;
pub mod session;
pub mod types;

#[cfg(feature = "uniffi")]
pub mod uniffi_api;

pub use driver::MatrixDriver;
pub use own_membership::{JoinParams, OwnIdentity};
pub use participation::{ParticipationConfig, ParticipationManager, Status};
pub use session::{ElementCallCompat, Session, SessionSnapshot, compute_sessions_from_events};

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();
