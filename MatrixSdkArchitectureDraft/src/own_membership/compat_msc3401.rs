//! `ElementCallCompat::StateEvents` write side: our membership as an
//! `org.matrix.msc3401.call.member` room-state event. Delete with that
//! generation. The read side lives in `session::convert::msc3401`.

use super::OwnIdentity;
use super::wire::is_leave;
use serde_json::{Value, json};

pub(crate) const EVENT_TYPE: &str = "org.matrix.msc3401.call.member";
/// That generation's validator requires an intent; absent means video.
const DEFAULT_CALL_INTENT: &str = "video";
/// The room call: `call_id` is empty in this generation's room-wide call.
const CALL_ID: &str = "";

/// `{user}:{device}` — this generation's member id *and* LiveKit participant
/// identity (see `connections::legacy_participant_identity`).
pub(crate) fn member_id(own: &OwnIdentity) -> String {
    format!("{}:{}", own.user_id, own.device_id)
}

/// `_{user_id}_{device_id}_{application}{call_id}`. The leading underscore is
/// required: Synapse rejects user-id-shaped state keys from anyone but that
/// user.
pub(crate) fn state_key(own: &OwnIdentity, application_type: &str) -> String {
    format!(
        "_{}_{}_{application_type}{CALL_ID}",
        own.user_id, own.device_id
    )
}

/// Spec content → state content. A leave (or anything without a `slot_id`)
/// is `{}`, the whole protocol for leaving in this dialect. `created_ts` is
/// pinned at the first send of the join (peers pick the oldest member's focus
/// by it); `expires` is measured from it so a refresh moves the deadline:
/// peers read `created_ts + expires = now + lifetime_ms`.
pub(crate) fn member_content(
    spec: &Value,
    own: &OwnIdentity,
    room_id: &str,
    application_type: &str,
    created_ts: u64,
    lifetime_ms: u64,
    now: u64,
) -> Value {
    if is_leave(spec) || spec.get("slot_id").is_none() {
        return json!({});
    }
    let foci: Vec<Value> = spec
        .pointer("/transports/published")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter(|focus| focus.get("type").and_then(Value::as_str) == Some("livekit"))
        .map(|focus| {
            let mut focus = focus.clone();
            if let Some(object) = focus.as_object_mut() {
                // Must equal the `room` we hand `/sfu/get` (the room id): the
                // legacy service derives the LiveKit room name from it alone.
                object
                    .entry("livekit_alias")
                    .or_insert_with(|| Value::String(room_id.to_owned()));
            }
            focus
        })
        .collect();
    let intent = spec
        .pointer("/application/m.call.intent")
        .cloned()
        .unwrap_or_else(|| Value::String(DEFAULT_CALL_INTENT.to_owned()));
    json!({
        "application": application_type,
        "call_id": CALL_ID,
        "scope": "m.room",
        "device_id": own.device_id,
        "membershipID": member_id(own),
        "created_ts": created_ts,
        "expires": now.saturating_sub(created_ts) + lifetime_ms,
        "focus_active": { "type": "livekit", "focus_selection": "multi_sfu" },
        "foci_preferred": foci,
        "m.call.intent": intent,
    })
}

#[cfg(test)]
mod tests {
    use super::super::wire::{Route, WireContext, join_content, leave_content, route};
    use super::*;
    use crate::session::ElementCallCompat;
    use crate::types::{LeaveReason, RtcTransport, TransportIntent};

    fn own() -> OwnIdentity {
        OwnIdentity {
            user_id: "@me:x".into(),
            device_id: "DEV".into(),
        }
    }

    fn join(intent: Option<&str>) -> Value {
        join_content(
            "",
            "@me:x:DEV",
            "m.call",
            intent,
            &TransportIntent::Publish(RtcTransport {
                transport_type: "livekit".into(),
                properties: json!({ "livekit_service_url": "https://lk" }),
            }),
        )
    }

    #[test]
    fn state_key_is_underscore_user_device_application_call_id() {
        assert_eq!(state_key(&own(), "m.call"), "_@me:x_DEV_m.call");
        assert_eq!(member_id(&own()), "@me:x:DEV");
    }

    #[test]
    fn a_join_becomes_state_content_with_pinned_created_ts() {
        let c = member_content(&join(None), &own(), "!r:x", "m.call", 1_000, 240_000, 1_000);
        assert_eq!(c["application"], "m.call");
        assert_eq!(c["call_id"], "");
        assert_eq!(c["scope"], "m.room");
        assert_eq!(c["device_id"], "DEV");
        assert_eq!(c["membershipID"], "@me:x:DEV");
        assert_eq!(c["created_ts"], 1_000);
        assert_eq!(c["expires"], 240_000);
        assert_eq!(
            c["focus_active"],
            json!({ "type": "livekit", "focus_selection": "multi_sfu" })
        );
    }

    #[test]
    fn a_refresh_moves_expires_but_not_created_ts() {
        let c = member_content(
            &join(None),
            &own(),
            "!r:x",
            "m.call",
            1_000,
            240_000,
            121_000,
        );
        assert_eq!(c["created_ts"], 1_000);
        assert_eq!(
            c["expires"], 360_000,
            "created_ts + expires == now + lifetime"
        );
    }

    #[test]
    fn foci_preferred_carries_livekit_alias_equal_to_the_room_id() {
        let c = member_content(&join(None), &own(), "!r:x", "m.call", 0, 1, 0);
        assert_eq!(
            c["foci_preferred"],
            json!([{ "type": "livekit", "livekit_service_url": "https://lk", "livekit_alias": "!r:x" }])
        );
    }

    #[test]
    fn intent_defaults_to_video() {
        assert_eq!(
            member_content(&join(None), &own(), "!r:x", "m.call", 0, 1, 0)["m.call.intent"],
            "video"
        );
        assert_eq!(
            member_content(&join(Some("audio")), &own(), "!r:x", "m.call", 0, 1, 0)["m.call.intent"],
            "audio"
        );
    }

    #[test]
    fn a_leave_is_empty_content() {
        let leave = leave_content("", "@me:x:DEV", &LeaveReason::leave());
        assert_eq!(
            member_content(&leave, &own(), "!r:x", "m.call", 0, 1, 0),
            json!({})
        );
    }

    #[test]
    fn the_route_is_a_state_event() {
        let o = own();
        let ctx = WireContext {
            compat: ElementCallCompat::StateEvents,
            own: &o,
            room_id: "!r:x",
            application_type: "m.call",
            created_ts: 7,
        };
        match route(&ctx, &join(None), 240_000, 7) {
            Route::State {
                event_type,
                state_key,
                content,
            } => {
                assert_eq!(event_type, EVENT_TYPE);
                assert_eq!(state_key, "_@me:x_DEV_m.call");
                assert_eq!(content["expires"], 240_000);
            }
            other => panic!("{other:?}"),
        }
    }
}
