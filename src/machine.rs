use crate::session::CallMembershipInfo;
use crate::store::MemoryStore;
use matrix_sdk::deserialized_responses::ProcessedToDeviceEvent;
use matrix_sdk::ruma::RoomId;
use std::sync::Arc;
use crate::session::CallMembershipInfo::Joined;

pub struct RTCMachine {
    inner: Arc<InnerMachine>,
}

impl RTCMachine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerMachine::new()),
        }
    }
}

struct InnerMachine {
    store: MemoryStore,
}

impl InnerMachine {

    pub fn new() -> Self {
        Self {
            store: MemoryStore::new(),
        }
    }

    pub async fn handle_processed_to_device(&self, _to_device: Vec<ProcessedToDeviceEvent>) {}


    pub fn start_rtc_session(&self, slot_id: &str,  room_id: &RoomId) {

    }


    /// This method processes one or more membership changes (joins/leaves) for a specific room.
    /// Each item in the iterator is converted into a [`CallMembershipInfo`] and processed.
    ///
    /// A MatrixRTC membership is represented by a sticky m.rtc.member event (MSC4354 Sticky Events).
    /// These events describe a participant’s presence in an MatrixRTC slot and provide sufficient
    /// metadata for other room members to detect and join the same slot.
    ///
    /// # Parameters
    ///
    /// * `added` - An item that can be converted into [`CallMembershipInfo`].
    ///   These are events with no predecessor events (matching sender, type, and sticky_key)
    /// * `room_id` - The Matrix room ID where the membership changes occurred.
    ///
    async fn on_sticky_event_added(
        &self,
        added: impl Into<CallMembershipInfo>,
        room_id: &RoomId,
    ) {
        let info = added.into();
        let existing_session = self.store.get_session(
            room_id,
            info.slot_id()
        ).await;
        if let Some(_session) = existing_session {

        } else {
            // Unknown session, create a new one if it is a join event
            if let Joined(_info) = info {

            } else {

            }
        }
    }
    /// The event was removed due to expiry
    async fn on_sticky_event_removed(
        &self,
        removed: impl Iterator<Item = impl Into<CallMembershipInfo>>,
        room_id: &RoomId,
    ) {
    }

    async fn on_sticky_event_update(
        &self,
        update: impl Iterator<Item = impl Into<CallMembershipInfo>>,
        room_id: &RoomId,
    ) {
    }
}
