//! Native FFI bindings for the MatrixRTC core.
//!
//! This module defines ABI-safe transport structs and converts them into core DTOs
//! before calling core APIs, so the core stays free of FFI-specific concerns.

use std::ffi::{CStr, c_char};

use matrix_rtc_core::{
    JoinedMembership, RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate, RtcSession,
    RtcSessionManager, StickyEventsUpdate,
};
use tokio::sync::watch;

const RESULT_OK: i32 = 0;
const RESULT_INVALID_POINTER: i32 = 1;
const RESULT_INVALID_STRING: i32 = 2;
const RESULT_CONVERSION_ERROR: i32 = 3;
const RESULT_NO_UPDATE_AVAILABLE: i32 = 4;

#[repr(C)]
/// ABI-safe sticky event payload passed from native bindings into the core.
pub struct FfiStickyEvent {
    /// Room ID (NUL-terminated UTF-8 C string).
    pub room_id: *const c_char,
    /// Sender user ID (NUL-terminated UTF-8 C string).
    pub sender: *const c_char,
    /// Matrix event type (NUL-terminated UTF-8 C string).
    pub event_type: *const c_char,
    /// Slot ID (NUL-terminated UTF-8 C string).
    pub slot_id: *const c_char,
    /// Sticky key (NUL-terminated UTF-8 C string).
    pub sticky_key: *const c_char,
    /// `application.type` (optional NUL-terminated UTF-8 C string).
    pub application_type: *const c_char,
    /// `member.id` (optional NUL-terminated UTF-8 C string).
    pub member_id: *const c_char,
    /// Disconnect reason (optional NUL-terminated UTF-8 C string).
    pub disconnect_reason: *const c_char,
}

#[repr(C)]
/// ABI-safe sticky update payload where `current` supersedes `previous`.
pub struct FfiStickyEventUpdate {
    /// New value for the sticky key.
    pub current: FfiStickyEvent,
    /// Previous value for the sticky key.
    pub previous: FfiStickyEvent,
}

/// Opaque FFI type representing a membership snapshot subscription.
pub struct FfiMembershipSnapshotSubscription {
    receiver: watch::Receiver<Vec<JoinedMembership>>,
    initial_pending: bool,
}

#[unsafe(no_mangle)]
/// Allocates and returns a new session manager handle.
pub extern "C" fn matrix_rtc_session_manager_new() -> *mut RtcSessionManager {
    Box::into_raw(Box::new(RtcSessionManager::new()))
}

#[unsafe(no_mangle)]
/// Allocates and returns a new single-session handle.
pub extern "C" fn matrix_rtc_session_new() -> *mut RtcSession {
    Box::into_raw(Box::new(RtcSession::new()))
}

#[unsafe(no_mangle)]
/// Frees a session manager handle previously returned by
/// `matrix_rtc_session_manager_new`.
///
/// Passing a null pointer is a no-op.
///
/// # Safety
///
/// `ptr` must either be null or a pointer returned by
/// `matrix_rtc_session_manager_new` that has not been freed yet.
pub unsafe extern "C" fn matrix_rtc_session_manager_free(ptr: *mut RtcSessionManager) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: ptr is checked for null and was allocated by Box::into_raw in matrix_rtc_session_manager_new.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

#[unsafe(no_mangle)]
/// Frees a session handle previously returned by `matrix_rtc_session_new`.
///
/// Passing a null pointer is a no-op.
///
/// # Safety
///
/// `ptr` must either be null or a pointer returned by `matrix_rtc_session_new`
/// that has not been freed yet.
pub unsafe extern "C" fn matrix_rtc_session_free(ptr: *mut RtcSession) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: ptr is checked for null and was allocated by Box::into_raw in matrix_rtc_session_new.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

#[unsafe(no_mangle)]
/// Applies initial sticky events to one single-session handle.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `session` must be a valid pointer returned by `matrix_rtc_session_new`.
/// If `events_len > 0`, `events` must be non-null and point to `events_len`
/// valid `FfiStickyEvent` entries with valid string pointers.
pub unsafe extern "C" fn matrix_rtc_session_on_sticky_events_snapshot_received(
    session: *mut RtcSession,
    events: *const FfiStickyEvent,
    events_len: usize,
) -> i32 {
    if session.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: session is checked for null above and expected to outlive this call.
    let session = unsafe { &mut *session };

    let parsed = match to_core_events(events, events_len) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    match session.initial_events(parsed) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Applies one sticky diff batch to one single-session handle.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `session` must be a valid pointer returned by `matrix_rtc_session_new`.
/// For each array, if its length is greater than zero, the pointer must be
/// non-null and point to a contiguous region of valid entries.
pub unsafe extern "C" fn matrix_rtc_session_on_sticky_events_update_received(
    session: *mut RtcSession,
    added: *const FfiStickyEvent,
    added_len: usize,
    updated: *const FfiStickyEventUpdate,
    updated_len: usize,
    removed: *const FfiStickyEvent,
    removed_len: usize,
) -> i32 {
    if session.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: session is checked for null above and expected to outlive this call.
    let session = unsafe { &mut *session };

    let added = match to_core_events(added, added_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let updated = match to_core_updates(updated, updated_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let removed = match to_core_events(removed, removed_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    match session.handle_update(StickyEventsUpdate {
        added,
        updated,
        removed,
    }) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Creates a membership snapshot subscription for one session.
///
/// # Safety
///
/// `session` must be a valid pointer returned by `matrix_rtc_session_new`.
pub unsafe extern "C" fn matrix_rtc_session_subscribe_membership_snapshots(
    session: *mut RtcSession,
) -> *mut FfiMembershipSnapshotSubscription {
    if session.is_null() {
        return std::ptr::null_mut();
    }

    // SAFETY: session is checked for null above and expected to outlive this call.
    let session = unsafe { &mut *session };
    let subscription = FfiMembershipSnapshotSubscription {
        receiver: session.subscribe_membership_snapshots(),
        initial_pending: true,
    };

    Box::into_raw(Box::new(subscription))
}

#[unsafe(no_mangle)]
/// Frees a snapshot subscription created by
/// `matrix_rtc_session_subscribe_membership_snapshots`.
///
/// Passing a null pointer is a no-op.
///
/// # Safety
///
/// `ptr` must either be null or a pointer returned by
/// `matrix_rtc_session_subscribe_membership_snapshots` that has not been freed yet.
pub unsafe extern "C" fn matrix_rtc_membership_snapshot_subscription_free(
    ptr: *mut FfiMembershipSnapshotSubscription,
) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: ptr is checked for null and was allocated by Box::into_raw.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

#[unsafe(no_mangle)]
/// Retrieves the next full membership snapshot as JSON.
///
/// The first call always returns the current snapshot (which may be empty).
/// Later calls return:
/// - `0` and set `out_json` to a newly allocated UTF-8 JSON string when changed.
/// - `4` (`RESULT_NO_UPDATE_AVAILABLE`) when there is no new snapshot.
///
/// # Safety
///
/// `subscription` must be valid and `out_json` must be a valid, writable pointer.
pub unsafe extern "C" fn matrix_rtc_membership_snapshot_subscription_next_json(
    subscription: *mut FfiMembershipSnapshotSubscription,
    out_json: *mut *mut c_char,
) -> i32 {
    if subscription.is_null() || out_json.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: out_json is validated above and always reset so callers never keep stale pointers.
    unsafe {
        *out_json = std::ptr::null_mut();
    }

    // SAFETY: pointers are validated above.
    let subscription = unsafe { &mut *subscription };

    let snapshot = if subscription.initial_pending {
        subscription.initial_pending = false;
        Some(subscription.receiver.borrow().clone())
    } else {
        match subscription.receiver.has_changed() {
            Ok(true) => Some(subscription.receiver.borrow_and_update().clone()),
            Ok(false) | Err(_) => None,
        }
    };

    let Some(snapshot) = snapshot else {
        return RESULT_NO_UPDATE_AVAILABLE;
    };

    let json = match serde_json::to_string(&snapshot) {
        Ok(value) => value,
        Err(_) => return RESULT_CONVERSION_ERROR,
    };

    let owned = match std::ffi::CString::new(json) {
        Ok(value) => value,
        Err(_) => return RESULT_CONVERSION_ERROR,
    };

    // SAFETY: out_json was validated above.
    unsafe {
        *out_json = owned.into_raw();
    }

    RESULT_OK
}

#[unsafe(no_mangle)]
/// Frees a string previously returned by
/// `matrix_rtc_membership_snapshot_subscription_next_json`.
///
/// # Safety
///
/// `ptr` must be null or a pointer returned by this crate.
pub unsafe extern "C" fn matrix_rtc_string_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: ptr is checked for null and must originate from CString::into_raw in this crate.
    unsafe {
        drop(std::ffi::CString::from_raw(ptr));
    }
}

#[unsafe(no_mangle)]
/// Applies one sticky event to the session manager.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `session_manager` must be a valid pointer returned by
/// `matrix_rtc_session_manager_new`.
/// `event` must be non-null and point to a valid `FfiStickyEvent` whose string
/// pointers are either null (for optional fields) or valid NUL-terminated UTF-8.
pub unsafe extern "C" fn matrix_rtc_session_manager_on_sticky_event_received(
    session_manager: *mut RtcSessionManager,
    event: *const FfiStickyEvent,
) -> i32 {
    if session_manager.is_null() || event.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: pointers are checked for null above and expected to outlive this call.
    let session_manager = unsafe { &mut *session_manager };
    // SAFETY: pointers are checked for null above and expected to outlive this call.
    let event = unsafe { &*event };

    let parsed = match to_core_event(event) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    match session_manager.on_sticky_event_received(parsed) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Applies an initial sticky snapshot to the session manager.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `session_manager` must be a valid pointer returned by
/// `matrix_rtc_session_manager_new`.
/// `room_id` must be a valid NUL-terminated UTF-8 C string.
/// If `events_len > 0`, `events` must be non-null and point to `events_len`
/// valid `FfiStickyEvent` entries with valid string pointers.
pub unsafe extern "C" fn matrix_rtc_session_manager_on_sticky_events_snapshot_received(
    session_manager: *mut RtcSessionManager,
    room_id: *const c_char,
    events: *const FfiStickyEvent,
    events_len: usize,
) -> i32 {
    if session_manager.is_null() || room_id.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: session_manager is checked for null above and expected to outlive this call.
    let session_manager = unsafe { &mut *session_manager };
    let room_id = match c_string_required(room_id) {
        Ok(room_id) => room_id,
        Err(code) => return code,
    };

    let parsed = match to_core_events(events, events_len) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    match session_manager.initial_sticky_for_room(&room_id, parsed) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Applies one sticky diff batch to the session manager.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `session_manager` must be a valid pointer returned by
/// `matrix_rtc_session_manager_new`.
/// `room_id` must be a valid NUL-terminated UTF-8 C string.
/// For each array, if its length is greater than zero, the pointer must be
/// non-null and point to a contiguous region of valid entries.
pub unsafe extern "C" fn matrix_rtc_session_manager_on_sticky_events_update_received(
    session_manager: *mut RtcSessionManager,
    room_id: *const c_char,
    added: *const FfiStickyEvent,
    added_len: usize,
    updated: *const FfiStickyEventUpdate,
    updated_len: usize,
    removed: *const FfiStickyEvent,
    removed_len: usize,
) -> i32 {
    if session_manager.is_null() || room_id.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: session_manager is checked for null above and expected to outlive this call.
    let session_manager = unsafe { &mut *session_manager };
    let room_id = match c_string_required(room_id) {
        Ok(room_id) => room_id,
        Err(code) => return code,
    };

    let added = match to_core_events(added, added_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let updated = match to_core_updates(updated, updated_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let removed = match to_core_events(removed, removed_len) {
        Ok(value) => value,
        Err(code) => return code,
    };

    match session_manager.sticky_update_for_room(
        &room_id,
        StickyEventsUpdate {
            added,
            updated,
            removed,
        },
    ) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

fn to_core_event(event: &FfiStickyEvent) -> Result<RawStickyEvent, i32> {
    let room_id = c_string_required(event.room_id)?;
    let sender = c_string_required(event.sender)?;
    let event_type = c_string_required(event.event_type)?;
    let slot_id = c_string_required(event.slot_id)?;
    let sticky_key = c_string_required(event.sticky_key)?;
    let application_type = c_string_optional(event.application_type)?;
    let member_id = c_string_optional(event.member_id)?;
    let disconnect_reason = c_string_optional(event.disconnect_reason)?;

    Ok(RawStickyEvent {
        room_id,
        sender,
        event_type,
        content: RawStickyEventContent {
            slot_id,
            sticky_key,
            application_type,
            member_id,
            disconnect_reason,
        },
    })
}

fn to_core_events(
    events: *const FfiStickyEvent,
    events_len: usize,
) -> Result<Vec<RawStickyEvent>, i32> {
    if events_len == 0 {
        return Ok(Vec::new());
    }

    if events.is_null() {
        return Err(RESULT_INVALID_POINTER);
    }

    // SAFETY: events is non-null and expected to point to events_len valid FfiStickyEvent values.
    let events = unsafe { std::slice::from_raw_parts(events, events_len) };

    events.iter().map(to_core_event).collect()
}

fn to_core_updates(
    updates: *const FfiStickyEventUpdate,
    updates_len: usize,
) -> Result<Vec<RawStickyEventUpdate>, i32> {
    if updates_len == 0 {
        return Ok(Vec::new());
    }

    if updates.is_null() {
        return Err(RESULT_INVALID_POINTER);
    }

    // SAFETY: updates is non-null and expected to point to updates_len valid FfiStickyEventUpdate values.
    let updates = unsafe { std::slice::from_raw_parts(updates, updates_len) };

    updates
        .iter()
        .map(|update| {
            Ok(RawStickyEventUpdate {
                current: to_core_event(&update.current)?,
                previous: to_core_event(&update.previous)?,
            })
        })
        .collect()
}

fn c_string_required(ptr: *const c_char) -> Result<String, i32> {
    if ptr.is_null() {
        return Err(RESULT_INVALID_POINTER);
    }

    // SAFETY: ptr is checked for null and assumed to point to a valid NUL-terminated string.
    let raw = unsafe { CStr::from_ptr(ptr) };

    raw.to_str()
        .map(str::to_owned)
        .map_err(|_| RESULT_INVALID_STRING)
}

fn c_string_optional(ptr: *const c_char) -> Result<Option<String>, i32> {
    if ptr.is_null() {
        return Ok(None);
    }

    // SAFETY: ptr is checked for null and assumed to point to a valid NUL-terminated string.
    let raw = unsafe { CStr::from_ptr(ptr) };

    raw.to_str()
        .map(|value| {
            if value.is_empty() {
                None
            } else {
                Some(value.to_owned())
            }
        })
        .map_err(|_| RESULT_INVALID_STRING)
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;

    #[test]
    fn ffi_entrypoint_accepts_join_event() {
        let room_id = CString::new("!room:example.org").unwrap();
        let sender = CString::new("@alice:example.org").unwrap();
        let event_type = CString::new("m.rtc.member").unwrap();
        let slot_id = CString::new("m.call#ROOM").unwrap();
        let sticky_key = CString::new("alice-device-a").unwrap();
        let application_type = CString::new("m.call").unwrap();
        let member_id = CString::new("alice-device-a").unwrap();

        let event = FfiStickyEvent {
            room_id: room_id.as_ptr(),
            sender: sender.as_ptr(),
            event_type: event_type.as_ptr(),
            slot_id: slot_id.as_ptr(),
            sticky_key: sticky_key.as_ptr(),
            application_type: application_type.as_ptr(),
            member_id: member_id.as_ptr(),
            disconnect_reason: std::ptr::null(),
        };

        let session_manager = matrix_rtc_session_manager_new();

        // SAFETY: pointers are valid for the duration of the call.
        let result =
            unsafe { matrix_rtc_session_manager_on_sticky_event_received(session_manager, &event) };
        assert_eq!(result, RESULT_OK);

        // SAFETY: session_manager was created by matrix_rtc_session_manager_new.
        unsafe {
            matrix_rtc_session_manager_free(session_manager);
        }
    }

    #[test]
    fn ffi_session_subscription_emits_initial_then_join_snapshot() {
        let room_id = CString::new("!room:example.org").unwrap();
        let sender = CString::new("@alice:example.org").unwrap();
        let event_type = CString::new("m.rtc.member").unwrap();
        let slot_id = CString::new("m.call#ROOM").unwrap();
        let sticky_key = CString::new("alice-device-a").unwrap();
        let application_type = CString::new("m.call").unwrap();
        let member_id = CString::new("alice-device-a").unwrap();

        let event = FfiStickyEvent {
            room_id: room_id.as_ptr(),
            sender: sender.as_ptr(),
            event_type: event_type.as_ptr(),
            slot_id: slot_id.as_ptr(),
            sticky_key: sticky_key.as_ptr(),
            application_type: application_type.as_ptr(),
            member_id: member_id.as_ptr(),
            disconnect_reason: std::ptr::null(),
        };

        let session = matrix_rtc_session_new();
        // SAFETY: session pointer is valid for this test.
        let subscription = unsafe { matrix_rtc_session_subscribe_membership_snapshots(session) };
        assert!(!subscription.is_null());

        let mut json_ptr = std::ptr::null_mut();
        // SAFETY: pointers are valid for this test.
        let initial_result = unsafe {
            matrix_rtc_membership_snapshot_subscription_next_json(subscription, &mut json_ptr)
        };
        assert_eq!(initial_result, RESULT_OK);
        assert!(!json_ptr.is_null());

        // SAFETY: json_ptr comes from this crate and is valid UTF-8 JSON.
        let initial_json = unsafe { CStr::from_ptr(json_ptr).to_str().unwrap().to_owned() };
        assert_eq!(initial_json, "[]");
        // SAFETY: json_ptr was allocated by this crate.
        unsafe {
            matrix_rtc_string_free(json_ptr);
        }

        // SAFETY: pointers are valid for this test.
        let snapshot_result =
            unsafe { matrix_rtc_session_on_sticky_events_snapshot_received(session, &event, 1) };
        assert_eq!(snapshot_result, RESULT_OK);

        let mut joined_json_ptr = std::ptr::null_mut();
        // SAFETY: pointers are valid for this test.
        let joined_result = unsafe {
            matrix_rtc_membership_snapshot_subscription_next_json(
                subscription,
                &mut joined_json_ptr,
            )
        };
        assert_eq!(joined_result, RESULT_OK);

        // SAFETY: joined_json_ptr comes from this crate and is valid UTF-8 JSON.
        let joined_json = unsafe { CStr::from_ptr(joined_json_ptr).to_str().unwrap().to_owned() };
        assert!(joined_json.contains("@alice:example.org"));
        // SAFETY: joined_json_ptr was allocated by this crate.
        unsafe {
            matrix_rtc_string_free(joined_json_ptr);
        }

        // Start from a non-null sentinel to assert the API clears stale pointers.
        let mut no_update_json_ptr = std::ptr::NonNull::<c_char>::dangling().as_ptr();
        // SAFETY: pointers are valid for this test.
        let no_update_result = unsafe {
            matrix_rtc_membership_snapshot_subscription_next_json(
                subscription,
                &mut no_update_json_ptr,
            )
        };
        assert_eq!(no_update_result, RESULT_NO_UPDATE_AVAILABLE);
        assert!(no_update_json_ptr.is_null());

        // SAFETY: pointers were allocated by this crate.
        unsafe {
            matrix_rtc_membership_snapshot_subscription_free(subscription);
            matrix_rtc_session_free(session);
        }
    }
}
