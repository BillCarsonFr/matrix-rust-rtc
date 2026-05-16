
pub(crate) use crate::types::{ApplicationId, ApplicationKind, Focus};
use matrix_sdk::ruma::{
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedUserId,
};
use tokio::sync::broadcast;

pub type ParticipantId = String;

#[derive(Clone, Debug)]
pub struct SessionDescription {
    pub id: Option<ApplicationId>,
    pub kind: ApplicationKind,
}

impl SessionDescription {
    pub fn new(kind: ApplicationKind) -> Self {
        Self { id: None, kind }
    }
}

pub enum CallMembershipInfo {
    Joined(JoinedInfo),
    Left(LeftInfo),
}

impl CallMembershipInfo {
    pub fn slot_id(&self) -> String {
        match self {
            CallMembershipInfo::Joined(i) => i.slot_id.to_owned(),
            CallMembershipInfo::Left(i) => i.slot_id.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Participant {
    pub device_id: OwnedDeviceId,
    pub user_id: OwnedUserId,
    pub participant_id: String,
}

#[derive(Clone, Debug)]
pub struct JoinedInfo {
    pub event_id: OwnedEventId,
    pub slot_id: String,
    pub application: ApplicationKind,
    pub sticky_key: String,
    pub participant: Participant,
    pub rtc_end_points: Vec<Focus>,
    pub timestamp: MilliSecondsSinceUnixEpoch,
}

#[derive(Clone, Debug)]
pub struct LeftInfo {
    pub event_id: OwnedEventId,
    pub slot_id: String,
    pub sender: OwnedUserId,
    pub sticky_key: String,
    /// Optional reason for disconnection.
    /// Machine-readable code.
    pub disconnect_reason: Option<String>,
}

#[derive(Debug)]
pub struct RtcSession {
    pub slot_id: String,
    pub description: SessionDescription,
    members: Vec<JoinedInfo>,
    pub room_id: OwnedRoomId,

    tx: broadcast::Sender<RtcSessionEvent>,
}

#[derive(Clone, Debug)]
pub enum RtcSessionEvent {
    MemberJoined(Participant),
    MemberLeft(Participant),
}

impl RtcSession {
    pub fn new(slot_id: String, description: SessionDescription, room_id: OwnedRoomId) -> Self {
        let (tx, _) = broadcast::channel(30);
        Self {
            slot_id,
            description,
            members: Vec::new(),
            room_id,
            tx,
        }
    }

    pub fn id(&self) -> Option<ApplicationId> {
        self.description.id.clone()
    }

    pub fn application(&self) -> &ApplicationKind {
        &self.description.kind
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RtcSessionEvent> {
        self.tx.subscribe()
    }

    /// Returns true if there are any active participants in the session and if the current slot is active.
    pub fn is_active(&self) -> bool {
        // TODO check if the slot is active
        !self.members.is_empty()
    }

    pub fn update_membership_info(&mut self, info: impl Into<CallMembershipInfo>) {
        let info = info.into();

        match info {
            CallMembershipInfo::Joined(info) => {
                let participant = info.participant.clone();
                let existing = self
                    .members
                    .iter()
                    .find(|x| x.participant.participant_id == info.participant.participant_id);
                if let Some(existing) = existing {
                    // is this still needed?
                    if existing.timestamp != info.timestamp {
                        self.members.retain(|x| {
                            x.participant.participant_id != info.participant.participant_id
                        });
                        self.members.push(info);
                    }
                } else {
                    self.members.push(info);
                }
                // Best-effort notify: ignore error if no receivers are listening.
                let _ = self.tx.send(RtcSessionEvent::MemberJoined(participant));
            }
            CallMembershipInfo::Left(info) => {
                let matching_joined = self.members.iter().find(|x| {
                    x.sticky_key == info.sticky_key && x.participant.user_id == info.sender
                });
                if let Some(joined) = matching_joined {
                    // Found the joined, let's remove him
                    let participant = joined.participant.clone();
                    let event_id = joined.event_id.clone();
                    self.members.retain(|info| info.event_id != event_id);

                    // Best-effort notify: ignore error if no receivers are listening.
                    let _ = self.tx.send(RtcSessionEvent::MemberLeft(participant));
                } else {
                    // No matching joiner, ignore?
                    log::warn!(
                        "Received a rtc leave event for unknown joined member. User:{}, Key {}",
                        info.sender,
                        info.sticky_key
                    )
                }
            }
        }
    }

    pub fn members(&self) -> &[JoinedInfo] {
        &self.members
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use matrix_sdk::ruma::{RoomId, owned_device_id, owned_event_id, owned_user_id, room_id};

    fn make_session_with(room_id: &RoomId) -> RtcSession {
        RtcSession::new(
            "slotXX".to_owned(),
            SessionDescription {
                id: None,
                kind: ApplicationKind::Call,
            },
            room_id.to_owned(),
        )
    }

    #[test]
    fn emit_member_joined() {
        let a_user_id = owned_user_id!("@user:example.com");
        let a_device_id = owned_device_id!("DEVICE_A");
        let a_device_id_2 = owned_device_id!("DEVICE_B");
        let participant_id = "@user:example.com_DEVICE_A".to_string();
        let participant_id_2 = "@user:example.com_DEVICE_B".to_string();

        let mut s = make_session_with(room_id!("!room:example.com"));
        let mut rx = s.subscribe();

        s.update_membership_info(CallMembershipInfo::Joined(JoinedInfo {
            slot_id: s.slot_id.to_owned(),
            event_id: owned_event_id!("$00"),
            sticky_key: "key0".to_string(),
            application: ApplicationKind::Call,
            participant: Participant {
                device_id: a_device_id.to_owned(),
                user_id: a_user_id.to_owned(),
                participant_id: participant_id.to_owned(),
            },
            rtc_end_points: vec![],
            timestamp: MilliSecondsSinceUnixEpoch::now(),
        }));
        s.update_membership_info(CallMembershipInfo::Joined(JoinedInfo {
            slot_id: s.slot_id.to_owned(),
            event_id: owned_event_id!("$01"),
            sticky_key: "key1".to_string(),
            application: ApplicationKind::Call,
            participant: Participant {
                device_id: a_device_id_2.to_owned(),
                user_id: a_user_id.to_owned(),
                participant_id: participant_id_2.to_owned(),
            },
            rtc_end_points: vec![],
            timestamp: MilliSecondsSinceUnixEpoch::now(),
        }));

        match rx.try_recv() {
            Ok(RtcSessionEvent::MemberJoined(participant)) => {
                assert_eq!(a_user_id, participant.user_id);
                assert_eq!(a_device_id, participant.device_id);
                assert_eq!(participant_id, participant.participant_id);
            }
            other => panic!("expected MemberAdded event, got {:?}", other),
        }
        match rx.try_recv() {
            Ok(RtcSessionEvent::MemberJoined(participant)) => {
                assert_eq!(a_user_id, participant.user_id);
                assert_eq!(a_device_id_2, participant.device_id);
                assert_eq!(participant_id_2, participant.participant_id);
            }
            other => panic!("expected MemberAdded event, got {:?}", other),
        }

        assert_matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty));
        assert_eq!(s.members.len(), 2);
    }

    #[test]
    fn member_left() {
        let a_user_id = owned_user_id!("@user:example.com");
        let a_device_id = owned_device_id!("DEVICE_A");
        let a_device_id_2 = owned_device_id!("DEVICE_B");
        let participant_id = "@user:example.com_DEVICE_A".to_string();
        let participant_id_2 = "@user:example.com_DEVICE_B".to_string();

        let mut s = make_session_with(room_id!("!room:example.com"));
        let mut rx = s.subscribe();

        s.update_membership_info(CallMembershipInfo::Joined(JoinedInfo {
            slot_id: s.slot_id.to_owned(),
            event_id: owned_event_id!("$00"),
            sticky_key: "key0".to_string(),
            application: ApplicationKind::Call,
            participant: Participant {
                device_id: a_device_id.clone(),
                user_id: a_user_id.clone(),
                participant_id: participant_id.clone(),
            },
            rtc_end_points: vec![],
            timestamp: MilliSecondsSinceUnixEpoch::now(),
        }));

        s.update_membership_info(CallMembershipInfo::Joined(JoinedInfo {
            slot_id: s.slot_id.to_owned(),
            event_id: owned_event_id!("$01"),
            sticky_key: "key1".to_string(),
            application: ApplicationKind::Call,
            participant: Participant {
                device_id: a_device_id_2.clone(),
                user_id: a_user_id.clone(),
                participant_id: participant_id_2.clone(),
            },
            rtc_end_points: vec![],
            timestamp: MilliSecondsSinceUnixEpoch::now(),
        }));

        rx.try_recv().unwrap();
        rx.try_recv().unwrap();
        assert_eq!(s.members.len(), 2);

        s.update_membership_info(CallMembershipInfo::Left(LeftInfo {
            slot_id: s.slot_id.to_owned(),
            event_id: owned_event_id!("$01"),
            sender: a_user_id.clone(),
            sticky_key: "key1".to_string(),
            disconnect_reason: None,
        }));

        match rx.try_recv() {
            Ok(RtcSessionEvent::MemberLeft(participant)) => {
                assert_eq!(a_user_id, participant.user_id);
                assert_eq!(a_device_id_2, participant.device_id);
                assert_eq!(participant_id_2, participant.participant_id);
            }
            other => panic!("expected MemberAdded event, got {:?}", other),
        }

        assert_eq!(s.members.len(), 1);
    }
}
