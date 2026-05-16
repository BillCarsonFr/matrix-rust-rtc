//! MatrixRTC machine entry points.
//!
//! This type is intentionally scoped to a single RTC session.
//! Cross-session routing is handled by `RtcSessionManager`.

use crate::event::{EventConversionError, RawStickyEvent, StickyEventsUpdate};
use crate::session::RtcSession;

/// Main MatrixRTC state machine for sticky membership ingestion.
pub struct MatrixRtcMachine {
    session: RtcSession,
}

impl Default for MatrixRtcMachine {
    fn default() -> Self {
        Self {
            session: RtcSession::new(),
        }
    }
}

impl MatrixRtcMachine {
    /// Creates an empty machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies the initial sticky events for this single session.
    pub fn initial_events(
        &mut self,
        events: impl IntoIterator<Item = RawStickyEvent>,
    ) -> Result<(), EventConversionError> {
        for event in events {
            self.session.update(event.try_into_call_membership_event()?);
        }

        Ok(())
    }

    /// Applies a sticky update batch for this single session.
    pub fn handle_update(
        &mut self,
        update: StickyEventsUpdate,
    ) -> Result<(), EventConversionError> {
        for event in update.added {
            self.session.update(event.try_into_call_membership_event()?);
        }

        for changed in update.updated {
            self.session
                .update(changed.current.try_into_call_membership_event()?);
        }

        for event in update.removed {
            self.session.update(event.try_into_left_membership_event()?);
        }

        Ok(())
    }

    /// Returns the current number of members in this session.
    pub fn member_count(&self) -> usize {
        self.session.member_count()
    }
}
