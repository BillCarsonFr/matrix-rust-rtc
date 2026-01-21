// Maybe a trait?

use crate::session::RtcSession;
use matrix_sdk::ruma::{OwnedRoomId, RoomId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MemoryStore {
    rtc_sessions: Arc<RwLock<HashMap<OwnedRoomId, Vec<Arc<RtcSession>>>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            rtc_sessions: Default::default(),
        }
    }

    pub(crate) async fn get_session(
        &self,
        room_id: &RoomId,
        slot_id: String,
    ) -> Option<Arc<RtcSession>> {
        let sessions = (*self.rtc_sessions).read().await;
        sessions
            .get(room_id)
            .and_then(|room_sessions| {
                room_sessions
                    .iter()
                    .find(|s| s.slot_id == slot_id)
                    .cloned()
            })

    }

    pub(crate) async fn add_session(&self, session: RtcSession) -> Result<(), String> {
        let mut sessions = (*self.rtc_sessions).write().await;
        sessions
            .entry(session.room_id.to_owned())
            .or_default()
            .push(Arc::new(session));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::ruma::{owned_room_id, room_id};
    use crate::session::SessionDescription;
    use crate::types::ApplicationKind;
    use super::*;

    #[tokio::test]
    async fn test_add_session() {
        let store = MemoryStore::new();

        let slot_id_a = "fooAAA";
        let slot_id_b = "fooBBB";
        let room_id = room_id!("!aroom:example.com");

        let session_a = RtcSession::new(
            slot_id_a.to_owned(),
            SessionDescription::new(ApplicationKind::Call),
            room_id.to_owned()
        );

        let session_b = RtcSession::new(
            slot_id_b.to_owned(),
            SessionDescription::new(ApplicationKind::Call),
            room_id.to_owned()
        );


        store.add_session(session_a).await.unwrap();
        store.add_session(session_b).await.unwrap();

        let result = store.get_session(room_id, slot_id_a.to_owned()).await;
        assert!(result.is_some());
        let found = result.unwrap();
        assert_eq!(found.slot_id, slot_id_a);

        let result = store.get_session(room_id, slot_id_b.to_owned()).await;
        assert!(result.is_some());
        let found = result.unwrap();
        assert_eq!(found.slot_id, slot_id_b);


        let result = store.get_session(room_id!("!no:foo.bar"), slot_id_b.to_owned()).await;
        assert!(result.is_none());
    }
}