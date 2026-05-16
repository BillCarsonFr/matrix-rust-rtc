//! Native FFI bindings for the MatrixRTC core.
//!
//! This module defines ABI-safe transport structs and converts them into core DTOs
//! before calling core APIs, so the core stays free of FFI-specific concerns.

use std::ffi::{CStr, c_char};

use matrix_rtc_core::{
    RawStickyEvent, RawStickyEventContent, RawStickyEventUpdate, RtcSessionManager,
    StickyEventsUpdate,
};

const RESULT_OK: i32 = 0;
const RESULT_INVALID_POINTER: i32 = 1;
const RESULT_INVALID_STRING: i32 = 2;
const RESULT_CONVERSION_ERROR: i32 = 3;

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

#[unsafe(no_mangle)]
/// Allocates and returns a new machine handle.
pub extern "C" fn matrix_rtc_machine_new() -> *mut RtcSessionManager {
    Box::into_raw(Box::new(RtcSessionManager::new()))
}

#[unsafe(no_mangle)]
/// Frees a machine handle previously returned by `matrix_rtc_machine_new`.
///
/// Passing a null pointer is a no-op.
///
/// # Safety
///
/// `ptr` must either be null or a pointer returned by
/// `matrix_rtc_machine_new` that has not been freed yet.
pub unsafe extern "C" fn matrix_rtc_machine_free(ptr: *mut RtcSessionManager) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: ptr is checked for null and was allocated by Box::into_raw in matrix_rtc_machine_new.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

#[unsafe(no_mangle)]
/// Applies one sticky event to the machine.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `machine` must be a valid pointer returned by `matrix_rtc_machine_new`.
/// `event` must be non-null and point to a valid `FfiStickyEvent` whose string
/// pointers are either null (for optional fields) or valid NUL-terminated UTF-8.
pub unsafe extern "C" fn matrix_rtc_machine_on_sticky_event_received(
    machine: *mut RtcSessionManager,
    event: *const FfiStickyEvent,
) -> i32 {
    if machine.is_null() || event.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: pointers are checked for null above and expected to outlive this call.
    let machine = unsafe { &mut *machine };
    // SAFETY: pointers are checked for null above and expected to outlive this call.
    let event = unsafe { &*event };

    let parsed = match to_core_event(event) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    match machine.on_sticky_event_received(parsed) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Applies an initial sticky snapshot to the machine.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `machine` must be a valid pointer returned by `matrix_rtc_machine_new`.
/// `room_id` must be a valid NUL-terminated UTF-8 C string.
/// If `events_len > 0`, `events` must be non-null and point to `events_len`
/// valid `FfiStickyEvent` entries with valid string pointers.
pub unsafe extern "C" fn matrix_rtc_machine_on_sticky_events_snapshot_received(
    machine: *mut RtcSessionManager,
    room_id: *const c_char,
    events: *const FfiStickyEvent,
    events_len: usize,
) -> i32 {
    if machine.is_null() || room_id.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: machine is checked for null above and expected to outlive this call.
    let machine = unsafe { &mut *machine };
    let room_id = match c_string_required(room_id) {
        Ok(room_id) => room_id,
        Err(code) => return code,
    };

    let parsed = match to_core_events(events, events_len) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    match machine.initial_sticky_for_room(&room_id, parsed) {
        Ok(()) => RESULT_OK,
        Err(_) => RESULT_CONVERSION_ERROR,
    }
}

#[unsafe(no_mangle)]
/// Applies one sticky diff batch to the machine.
///
/// Returns an integer status code (`0` means success).
///
/// # Safety
///
/// `machine` must be a valid pointer returned by `matrix_rtc_machine_new`.
/// `room_id` must be a valid NUL-terminated UTF-8 C string.
/// For each array, if its length is greater than zero, the pointer must be
/// non-null and point to a contiguous region of valid entries.
pub unsafe extern "C" fn matrix_rtc_machine_on_sticky_events_update_received(
    machine: *mut RtcSessionManager,
    room_id: *const c_char,
    added: *const FfiStickyEvent,
    added_len: usize,
    updated: *const FfiStickyEventUpdate,
    updated_len: usize,
    removed: *const FfiStickyEvent,
    removed_len: usize,
) -> i32 {
    if machine.is_null() || room_id.is_null() {
        return RESULT_INVALID_POINTER;
    }

    // SAFETY: machine is checked for null above and expected to outlive this call.
    let machine = unsafe { &mut *machine };
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

    match machine.sticky_update_for_room(
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
    use std::ffi::CString;

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

        let machine = matrix_rtc_machine_new();

        // SAFETY: pointers are valid for the duration of the call.
        let result = unsafe { matrix_rtc_machine_on_sticky_event_received(machine, &event) };
        assert_eq!(result, RESULT_OK);

        // SAFETY: machine was created by matrix_rtc_machine_new.
        unsafe {
            matrix_rtc_machine_free(machine);
        }
    }
}
