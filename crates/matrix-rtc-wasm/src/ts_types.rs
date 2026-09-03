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

//! The TypeScript declarations for everything that crosses the boundary as a
//! `JsValue`.
//!
//! wasm-bindgen types a `JsValue` parameter or return as `any`; this custom
//! section supplies the real shapes, and the `unchecked_param_type` /
//! `unchecked_return_type` attributes at each call site bind them. The
//! declarations are hand-written and MUST track the serde shapes they
//! describe (the structs are all in this crate — `WasmJoinSessionParams`,
//! `WasmParticipant`, `WasmCallEvent`, the compat carriers, ...); the
//! `web/` package's type-check test catches declarations that stop parsing,
//! but a field rename only a human reads here.
//!
//! Conventions, mirroring the wire: input objects mark serde-defaulted fields
//! `?:`; output objects type `Option` fields `| null` (serde serializes
//! `None` as `null`, not absence).

use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &'static str = r#"
/** The `matrix-js-sdk`-shaped client object `setup_command_sender` dispatches on. */
export interface MatrixClientHost {
    /** MSC4354 sticky send; pass `durationMs` through verbatim. */
    sendStickyEvent(roomId: string, eventType: string, content: Record<string, unknown>, durationMs: number): Promise<{ event_id: string } | { eventId: string } | string>;
    sendStateEvent(roomId: string, eventType: string, stateKey: string, content: Record<string, unknown>): Promise<{ event_id: string } | { eventId: string } | string>;
    /** MSC4140 delayed send; resolves with the bare delay id. */
    sendDelayedEvent(roomId: string, eventType: string, content: Record<string, unknown>, delayMs: number): Promise<string>;
    /** MSC4140 `restart` — never cancel+resend. */
    restartDelayedEvent(roomId: string, delayId: string): Promise<unknown>;
    cancelDelayedEvent(roomId: string, delayId: string): Promise<unknown>;
    /** Olm-encrypted, per specific device. Resolving with nothing reports every recipient served. */
    sendToDeviceMessage(recipients: { userId: string; deviceId: string }[], messageType: string, content: Record<string, unknown>): Promise<{ userId: string; deviceId: string; error?: string }[] | void>;
    /** Plain message-like send (reactions, raised hand); encrypted in an encrypted room. */
    sendRoomEvent(roomId: string, eventType: string, content: Record<string, unknown>): Promise<{ event_id: string } | { eventId: string } | string>;
    /** Redact one of our own events (lowering a raised hand). */
    redactEvent(roomId: string, eventId: string, reason?: string): Promise<unknown>;
    /** Pre-sticky compat only: the delayed leave as a delayed STATE event. */
    sendDelayedStateEvent?(roomId: string, eventType: string, stateKey: string, content: Record<string, unknown>, delayMs: number): Promise<string>;
}

export type RtcStreamKind = "microphone" | "camera" | "screen_share" | "screen_share_audio" | "data";
export type ElementCallCompatMode = "off" | "sticky_events" | "state_events";

/** One roster entry, as `participants()` returns it. */
export interface RtcParticipant {
    member_id: string;
    user_id: string;
    device_id: string | null;
    is_local: boolean;
    reachable: boolean;
    /** The livekit-js participant identity — the join key for `room.getParticipantByIdentity()`. */
    rtc_identity: string | null;
    streams: { kind: RtcStreamKind; muted: boolean }[];
    /** When the participant raised their hand (ms since the epoch); `null` while it is down. */
    hand_raised_at_ms: number | null;
}

/** An event on the unified call stream (`onEvent`). */
export type RtcCallEvent =
    | { type: "participant_joined"; member_id: string; user_id: string }
    | { type: "participant_left"; member_id: string }
    | { type: "stream_started"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_stopped"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_muted"; member_id: string; kind: RtcStreamKind }
    | { type: "stream_unmuted"; member_id: string; kind: RtcStreamKind }
    | { type: "active_speakers"; speakers: { member_id: string; level: number }[] }
    | { type: "key_imported"; member_id: string; key_index: number }
    | { type: "frame_encryption_state"; member_id: string;
        state: "ok" | "missing_key" | "decryption_failed" | "encryption_failed" | "internal_error";
        installed_key_indices: number[] | null }
    | { type: "key_discarded"; member_id: string; key_index: number | null;
        sender_user_id: string | null; sender_device_id: string | null;
        reason_code: "cleartext" | "not_cross_signed" | "room_mismatch" | "sender_mismatch" | "unverifiable_device" | "device_mismatch";
        reason: string }
    | { type: "hand_raised"; member_id: string; raised_at_ms: number }
    | { type: "hand_lowered"; member_id: string }
    /** Transient: show `emoji` for ~3 s; `sound` is the asset base name to play (`null` = silent). */
    | { type: "reaction"; member_id: string; emoji: string; name: string; sound: string | null }
    | { type: "unknown_participant"; identity: string }
    | { type: "media_connection_state"; degraded: boolean }
    | { type: "ended"; reason: string };

/** `connectMedia`'s configuration. */
export interface MediaSessionConfigIn {
    room_id: string;
    slot_id: string;
    user_id: string;
    device_id: string;
    /** The MSC4195 authorisation-service URL of the focus we publish on. */
    livekit_service_url: string;
    /** livekit-js key-provider ring size when configured away from its default of 16. */
    key_ring_size?: number;
    /** Cross-check only: the mode comes from the join. */
    element_call_compat?: ElementCallCompatMode;
}

/** The object driving livekit-js for `connectMedia`. */
export interface MediaDelegate {
    getOpenIdToken(): Promise<{ access_token: string; token_type: string; matrix_server_name: string; expires_in: number }>;
    /** POST `body` as JSON to `url`; resolve with the HTTP status and raw response text. */
    fetchJson(url: string, body: Record<string, unknown>): Promise<{ status: number; body: string }>;
    /** Connect a livekit-js Room and register the RoomEvent translation onto `sink`. */
    connect(request: { connectionKey: string; sfuUrl: string; jwt: string }, sink: WasmConnectionEventSink): Promise<{ close(): Promise<unknown> }>;
    /** Install a media key in livekit-js's key provider (per participant, HKDF material). */
    setKey(identity: string, index: number, key: Uint8Array): Promise<boolean | void>;
    /** Move the local sender onto a rotated key index. */
    setLocalKeyIndex?(index: number): void;
    /** The push half: the roster after each change. */
    onParticipants?(roster: RtcParticipant[]): void;
    /** The push half: the unified call event stream. */
    onEvent?(event: RtcCallEvent): void;
    /** A key's delayBeforeUse window closed: call `flushDueKeyRotation`. */
    onSwitchComplete?(): void;
}

/** `join`'s parameters. */
export interface JoinParamsIn {
    user_id: string;
    device_id: string;
    room_id: string;
    slot_id: string;
    application: string;
    /** The transport to publish on; omit to join receive-only. */
    transport?: { type: string; [key: string]: unknown };
    can_subscribe?: string[];
    keep_alive_timeout_ms?: number;
    sticky_duration_ms?: number;
    degraded_lifetime_ms?: number;
    encryption_config?: {
        delay_before_use_ms?: number;
        key_rotation_grace_period_ms?: number;
        max_key_lifetime_ms?: number;
        manage_media_keys?: boolean;
        require_cross_signed_sender?: boolean;
    };
    notify?: { notification_type?: string; mentions?: Record<string, unknown>; lifetime_ms?: number };
    element_call_compat?: ElementCallCompatMode;
    /** Element Call reactions and raised hand; omitted is enabled with the 3 s window. */
    reactions?: { enabled?: boolean; active_window_ms?: number; send_cooldown_ms?: number };
}

/** `leave`'s parameters (`{}` is a plain hang-up). */
export interface LeaveParamsIn {
    leave_reason?: { code: string; reason?: string };
}

/** One sticky event for the TYPED ingestion (`set_current_sticky_state`). */
export interface StickyEventIn {
    room_id: string;
    /** The event's id; reactions and the raised hand relate to it. Supply it. */
    event_id?: string;
    sender: string;
    /** From decryption metadata, not the payload. */
    sender_device_id?: string;
    /** Omit if unknown — not the same as `false`. */
    was_encrypted?: boolean;
    type: string;
    content: {
        slot_id: string;
        msc4354_sticky_key?: string;
        sticky_key?: string;
        application?: { type: string; [key: string]: unknown };
        member?: { id: string; membership?: string };
        transports?: { published?: { type: string; [key: string]: unknown }[]; can_subscribe?: string[] };
        leave_reason?: { code?: string; reason?: string };
    };
}

/** One raw member event for the compat funnel (`setCurrentMembership`) — content verbatim. */
export interface RawMemberEventIn {
    /** The event's id; reactions and the raised hand relate to it. Supply it. */
    event_id?: string;
    sender: string;
    sender_device_id?: string;
    was_encrypted?: boolean;
    type: string;
    content: Record<string, unknown>;
}

/** One pre-MSC4354 `org.matrix.msc3401.call.member` room-state event. */
export interface LegacyStateMemberEventIn {
    /** The event's id; reactions and the raised hand relate to it. Supply it. */
    event_id?: string;
    sender: string;
    state_key: string;
    /** Load-bearing: the expiry base for a content with no `created_ts`. */
    origin_server_ts: number;
    content: Record<string, unknown>;
}

/** One message-like room event for the reactions intake (`onRoomTimelineEvents`, `onRelationsReceived`). */
export interface TimelineEventIn {
    room_id?: string;
    event_id: string;
    sender: string;
    /** From decryption metadata, not the payload. */
    sender_device_id?: string;
    was_encrypted?: boolean;
    /** `io.element.call.reaction` or `m.reaction`; anything else is ignored. */
    type: string;
    origin_server_ts: number;
    content: Record<string, unknown>;
}

/** A membership event whose annotations the host should fetch (`pendingRelationLookups`). */
export interface RelationLookup {
    member_id: string;
    membership_event_id: string;
}

/** A member whose hand is up (`raisedHands`). */
export interface RaisedHand {
    member_id: string;
    sender: string;
    reaction_event_id: string;
    /** ms since the epoch, by the server's clock; sort ascending to order speakers. */
    raised_at_ms: number;
}

/** One entry of Element Call's reaction catalogue (`reactionCatalog()`). */
export interface ReactionKind {
    name: string;
    emoji: string;
    /** Base name of the sound asset Element Call plays for it, or `null` for a silent reaction. */
    sound: string | null;
}

/** One `m.rtc.slot` state event (`on_room_slots_received`). */
export interface SlotEventIn {
    slot_id: string;
    content: Record<string, unknown>;
}

/** MSC4143 `content.encryption` of an `m.rtc.slot` (`openSlot`). */
export interface SlotEncryptionIn {
    type: string;
    [key: string]: unknown;
}

/** A decrypted spec-current media-key to-device message (`receiveEncryptionKey`). */
export interface ReceivedKeyIn {
    room_id: string;
    member_id: string;
    key_b64: string;
    key_index: number;
    was_encrypted: boolean;
    /** Required when `was_encrypted`; from Olm decryption metadata. */
    sender_user_id?: string;
    sender_device_id?: string;
    /** MSC4153: keys from devices that are not cross-signed are discarded. */
    sender_is_cross_signed?: boolean;
}

/** A decrypted legacy `io.element.call.encryption_keys` message, content raw. */
export interface LegacyKeyIn {
    sender: string;
    content: Record<string, unknown>;
    was_encrypted: boolean;
    sender_device_id?: string;
    sender_is_cross_signed?: boolean;
}

/** One entry of the sink's `activeSpeakers` payload. */
export interface SpeakerIn {
    identity: string;
    /** 0.0 (silent) to 1.0; omit for transports that report no level. */
    level?: number;
}

/** A projected membership, as snapshot subscriptions return them. */
export interface MembershipSnapshot {
    room_id: string;
    slot_id: string;
    sender: string;
    origin: "unknown" | "cleartext"
        | { encrypted: { sender_device_id: string | null } }
        | { claimed: { device_id: string } };
    sticky_key: string;
    member_id: string;
    /** Id of the member's latest membership event; moves on every sticky refresh. */
    membership_event_id: string | null;
    membership_ts: number | null;
    application: string | null;
    /** Externally tagged: the core's typed transports, not the wire shape. */
    transports: (
        | { LiveKit: { livekit_service_url: string } }
        | { Unsupported: { transport_type: string; extra_fields: Record<string, unknown> } }
    )[];
    can_subscribe: string[];
}
"#;
