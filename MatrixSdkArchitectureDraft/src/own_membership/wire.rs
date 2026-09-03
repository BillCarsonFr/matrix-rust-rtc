//! Spec-shaped `m.rtc.member` content for *our* membership (join, refresh,
//! leave, delayed leave) and the [`Route`] it travels on. The only place in
//! this module that knows event-type strings; the compat renderers are
//! dispatched from [`route`] so `machine.rs` never sees a dialect.

use super::{OwnIdentity, compat_2025, compat_msc3401};
use crate::session::ElementCallCompat;
use crate::types::{LeaveReason, RtcTransport, TransportIntent, wire_event_type};
use serde_json::{Map, Value, json};

pub(crate) const MEMBER_EVENT_TYPE: &str = "m.rtc.member";

/// `transports.published[..]` entry: `type` plus the type-specific fields.
pub(crate) fn transport_json(transport: &RtcTransport) -> Value {
    let mut object = match &transport.properties {
        Value::Object(map) => map.clone(),
        _ => Map::new(),
    };
    object.insert(
        "type".to_owned(),
        Value::String(transport.transport_type.clone()),
    );
    Value::Object(object)
}

/// `member.transports` from what we intend to publish. Publishing a type
/// declares we can receive it; `None` when both lists would be empty.
pub(crate) fn transports_json(intent: &TransportIntent) -> Option<Value> {
    let (published, can_subscribe): (Vec<Value>, Vec<String>) = match intent {
        TransportIntent::Publish(t) => (vec![transport_json(t)], vec![t.transport_type.clone()]),
        TransportIntent::ReceiveOnly { can_subscribe } => (Vec::new(), can_subscribe.clone()),
    };
    if published.is_empty() && can_subscribe.is_empty() {
        return None;
    }
    Some(json!({ "published": published, "can_subscribe": can_subscribe }))
}

/// MSC4143 join content. `member_id` doubles as the sticky key.
pub(crate) fn join_content(
    slot_id: &str,
    member_id: &str,
    application_type: &str,
    intent: Option<&str>,
    transports: &TransportIntent,
) -> Value {
    let mut application = json!({ "type": application_type });
    if let Some(intent) = intent {
        application["m.call.intent"] = Value::String(intent.to_owned());
    }
    let mut content = json!({
        "slot_id": slot_id,
        "msc4354_sticky_key": member_id,
        "member": { "id": member_id, "membership": "join" },
        "application": application,
    });
    if let Some(transports) = transports_json(transports) {
        content["transports"] = transports;
    }
    content
}

/// MSC4143 leave content (also the delayed leave's).
pub(crate) fn leave_content(slot_id: &str, member_id: &str, reason: &LeaveReason) -> Value {
    json!({
        "slot_id": slot_id,
        "msc4354_sticky_key": member_id,
        "member": { "id": member_id, "membership": "leave" },
        "leave_reason": serde_json::to_value(reason).expect("leave_reason serialises"),
    })
}

pub(crate) fn is_leave(spec: &Value) -> bool {
    spec.pointer("/member/membership").and_then(Value::as_str) == Some("leave")
}

/// How a membership event goes out: as a sticky event (spec and the 2025
/// dialect) or as room state (MSC3401 dialect).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Route {
    Sticky {
        event_type: String,
        content: Value,
        duration_ms: u64,
    },
    State {
        event_type: String,
        state_key: String,
        content: Value,
    },
}

/// Everything the renderers need besides the spec content.
#[derive(Clone, Debug)]
pub(crate) struct WireContext<'a> {
    pub compat: ElementCallCompat,
    pub own: &'a OwnIdentity,
    pub room_id: &'a str,
    pub application_type: &'a str,
    /// Pinned at the first send of this join (MSC3401 `created_ts`).
    pub created_ts: u64,
}

/// Render spec content for the wire in the selected dialect.
pub(crate) fn route(ctx: &WireContext<'_>, spec: &Value, lifetime_ms: u64, now: u64) -> Route {
    let event_type = wire_event_type(MEMBER_EVENT_TYPE).to_owned();
    match ctx.compat {
        ElementCallCompat::Off => Route::Sticky {
            event_type,
            content: spec.clone(),
            duration_ms: lifetime_ms,
        },
        ElementCallCompat::StickyEvents => Route::Sticky {
            event_type,
            content: compat_2025::rewrite(spec, ctx.own),
            duration_ms: lifetime_ms,
        },
        ElementCallCompat::StateEvents => Route::State {
            event_type: compat_msc3401::EVENT_TYPE.to_owned(),
            state_key: compat_msc3401::state_key(ctx.own, ctx.application_type),
            content: compat_msc3401::member_content(
                spec,
                ctx.own,
                ctx.room_id,
                ctx.application_type,
                ctx.created_ts,
                lifetime_ms,
                now,
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lk(url: &str) -> RtcTransport {
        RtcTransport {
            transport_type: "livekit".into(),
            properties: json!({ "livekit_service_url": url }),
        }
    }

    fn own() -> OwnIdentity {
        OwnIdentity {
            user_id: "@me:x".into(),
            device_id: "DEV".into(),
        }
    }

    #[test]
    fn join_content_has_slot_member_application_sticky_key_and_transports() {
        let c = join_content(
            "m.call#ROOM",
            "m-1",
            "m.call",
            None,
            &TransportIntent::Publish(lk("https://lk")),
        );
        assert_eq!(c["slot_id"], "m.call#ROOM");
        assert_eq!(c["msc4354_sticky_key"], "m-1");
        assert_eq!(c["member"], json!({ "id": "m-1", "membership": "join" }));
        assert_eq!(c["application"], json!({ "type": "m.call" }));
        assert_eq!(
            c["transports"]["published"],
            json!([{ "type": "livekit", "livekit_service_url": "https://lk" }])
        );
    }

    #[test]
    fn publishing_a_transport_declares_it_subscribable() {
        let c = join_content("s", "m", "m.call", None, &TransportIntent::Publish(lk("u")));
        assert_eq!(c["transports"]["can_subscribe"], json!(["livekit"]));
    }

    #[test]
    fn receive_only_publishes_nothing_and_keeps_can_subscribe() {
        let c = join_content(
            "s",
            "m",
            "m.call",
            None,
            &TransportIntent::ReceiveOnly {
                can_subscribe: vec!["livekit".into()],
            },
        );
        assert_eq!(c["transports"]["published"], json!([]));
        assert_eq!(c["transports"]["can_subscribe"], json!(["livekit"]));
    }

    #[test]
    fn empty_transports_are_omitted() {
        let c = join_content(
            "s",
            "m",
            "m.call",
            None,
            &TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
        );
        assert!(c.get("transports").is_none());
    }

    #[test]
    fn intent_lands_in_application_m_call_intent() {
        let c = join_content(
            "s",
            "m",
            "m.call",
            Some("audio"),
            &TransportIntent::ReceiveOnly {
                can_subscribe: vec![],
            },
        );
        assert_eq!(c["application"]["m.call.intent"], "audio");
    }

    #[test]
    fn leave_content_carries_only_slot_member_key_and_reason() {
        let c = leave_content("s", "m", &LeaveReason::leave());
        assert_eq!(
            c,
            json!({ "slot_id": "s", "msc4354_sticky_key": "m", "member": { "id": "m", "membership": "leave" }, "leave_reason": { "code": "leave" } })
        );
        assert!(is_leave(&c));
    }

    #[test]
    fn delayed_leave_content_uses_the_delayed_leave_code() {
        let c = leave_content("s", "m", &LeaveReason::delayed_leave());
        assert_eq!(c["leave_reason"]["code"], "delayed_leave");
        assert!(c["leave_reason"]["reason"].is_string());
    }

    #[test]
    fn spec_route_is_sticky_with_the_published_lifetime_in_the_unstable_spelling() {
        let o = own();
        let ctx = WireContext {
            compat: ElementCallCompat::Off,
            own: &o,
            room_id: "!r:x",
            application_type: "m.call",
            created_ts: 0,
        };
        let spec = join_content("s", "m", "m.call", None, &TransportIntent::Publish(lk("u")));
        let r = route(&ctx, &spec, 240_000, 5);
        assert_eq!(
            r,
            Route::Sticky {
                event_type: "org.matrix.msc4143.rtc.member".into(),
                content: spec.clone(),
                duration_ms: 240_000
            }
        );
        let leave = leave_content("s", "m", &LeaveReason::leave());
        assert!(matches!(
            route(&ctx, &leave, 240_000, 5),
            Route::Sticky {
                duration_ms: 240_000,
                ..
            }
        ));
    }
}
