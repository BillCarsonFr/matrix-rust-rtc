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

//! WASM binding implementation of the command sender interface.
//!
//! This module provides the `JsCommandSender` that implements `RtcCommandSender`
//! by delegating to a JavaScript object that provides the actual Matrix SDK integration.

use async_trait::async_trait;
use js_sys::{Array, Function, Reflect};
use matrix_rtc_core::{
    CommandError, RtcCommandSender, ToDeviceDelivery, ToDeviceRecipient, wire_event_type,
};
use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// WASM implementation of the RtcCommandSender trait.
///
/// This sender delegates to a JavaScript object that provides the actual Matrix SDK integration.
/// The client must implement methods: sendStickyEvent(roomId, type, content, durationMs),
/// sendDelayedEvent, restartDelayedEvent, cancelDelayedEvent.
#[wasm_bindgen]
pub struct JsCommandSender {
    /// The JavaScript Matrix client that handles the actual event sending
    #[wasm_bindgen(skip)]
    client: JsValue,
    /// Optional callback for logging/debugging
    #[wasm_bindgen(skip)]
    on_command: Option<Function>,
}

#[wasm_bindgen]
impl JsCommandSender {
    /// Creates a new JsCommandSender with the given Matrix client.
    ///
    /// The client must implement the following methods:
    /// - sendStickyEvent(roomId, eventType, content, durationMs, callback)
    /// - sendDelayedEvent(roomId, eventType, content, delayMs, callback)
    /// - sendToDeviceMessage(recipients, type, content) -> Promise, resolving to
    ///   `[{userId, deviceId, error?}, ...]` (or nothing, meaning all delivered)
    /// - restartDelayedEvent(roomId, delayId, callback)
    /// - cancelDelayedEvent(roomId, delayId, callback)
    /// - sendToDeviceMessage(userId, deviceId, messageType, content, callback)
    #[wasm_bindgen(constructor)]
    pub fn new(client: JsValue) -> Self {
        Self {
            client,
            on_command: None,
        }
    }

    /// Sets a debug callback for logging commands.
    pub fn set_debug_callback(&mut self, callback: Function) {
        self.on_command = Some(callback);
    }
}

impl JsCommandSender {
    fn log_command(&self, description: &str) {
        log::debug!("command sending: {description}");

        if let Some(callback) = &self.on_command {
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(description));
        }
    }

    fn convert_js_error(error: JsValue) -> CommandError {
        let converted = Self::classify_js_error(error);
        log::warn!("command failed: {converted}");
        converted
    }

    fn classify_js_error(error: JsValue) -> CommandError {
        if error.is_undefined() || error.is_null() {
            CommandError::SendError("unknown error".to_string())
        } else if let Ok(error_obj) = error.clone().dyn_into::<js_sys::Error>() {
            CommandError::SendError(error_obj.message().into())
        } else if let Some(msg) = error.as_string() {
            CommandError::SendError(msg)
        } else {
            CommandError::SendError(format!("{:?}", error))
        }
    }

    /// Call a method on the client object by name that returns a Promise.
    ///
    /// This is used for async operations where the JS method returns a Promise
    /// that will be converted to a Rust Future.
    fn call_js_promise_method(
        &self,
        method_name: &str,
        args: Vec<JsValue>,
    ) -> Result<js_sys::Promise, JsValue> {
        let method = Reflect::get(&self.client, &JsValue::from_str(method_name))?;
        if method.is_undefined() {
            return Err(JsValue::from_str(&format!(
                "client missing method: {}",
                method_name
            )));
        }

        // Convert args to js_sys::Array
        let js_args = Array::new();
        for (i, arg) in args.iter().enumerate() {
            js_args.set(i as u32, arg.clone());
        }

        // Call the method and expect a Promise to be returned
        let result = Reflect::apply(&method.dyn_into::<Function>()?, &self.client, &js_args)?;

        // Verify it's a Promise
        if result.is_instance_of::<js_sys::Promise>() {
            Ok(result.dyn_into::<js_sys::Promise>().unwrap())
        } else {
            // If it's not a Promise, wrap it in a resolved Promise
            Ok(js_sys::Promise::resolve(&result))
        }
    }
}

// SAFE: In WASM, there's no actual thread sharing happening.
// The Send+Sync bounds are required by the trait but are safe in this context.
unsafe impl Send for JsCommandSender {}
unsafe impl Sync for JsCommandSender {}

#[async_trait(?Send)]
impl RtcCommandSender for JsCommandSender {
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<(), CommandError> {
        // The JS host puts this string on the wire verbatim, so translate the
        // core's stable id to the one peers actually match on.
        let event_type = wire_event_type(&event_type);
        self.log_command(&format!(
            "send_sticky_event: room={}, type={}, duration={}ms",
            room_id, event_type, duration_ms
        ));

        // Convert Rust Value to JsValue
        let js_content = serde_wasm_bindgen::to_value(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        // Create a Promise that will be resolved by the JS callback
        let promise = self
            .call_js_promise_method(
                "sendStickyEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(event_type),
                    js_content,
                    // The core refreshes the entry against this lifetime; the
                    // host must pass it through, not choose its own.
                    JsValue::from_f64(duration_ms as f64),
                ],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
        wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        Ok(())
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<(), CommandError> {
        let event_type = wire_event_type(&event_type);
        self.log_command(&format!(
            "send_state_event: room={}, type={}, state_key={}",
            room_id, event_type, state_key
        ));

        let js_content = serde_wasm_bindgen::to_value(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let promise = self
            .call_js_promise_method(
                "sendStateEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(event_type),
                    JsValue::from_str(&state_key),
                    js_content,
                ],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        Ok(())
    }

    async fn send_delayed_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let event_type = wire_event_type(&event_type);
        self.log_command(&format!(
            "send_delayed_event: room={}, type={}, delay={}ms",
            room_id, event_type, delay_ms
        ));

        // Convert Rust Value to JsValue
        let js_content = serde_wasm_bindgen::to_value(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        // Create a Promise that will be resolved by the JS callback
        let promise = self
            .call_js_promise_method(
                "sendDelayedEvent",
                vec![
                    JsValue::from_str(&room_id),
                    JsValue::from_str(event_type),
                    js_content,
                    JsValue::from_f64(delay_ms as f64),
                ],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
        // The Promise should resolve to the MSC4140 delay id
        let js_result = wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        // Extract the delay id from the result
        let delay_id = js_result.as_string().ok_or_else(|| {
            CommandError::SendError("sendDelayedEvent did not return a string delay id".to_string())
        })?;

        Ok(delay_id)
    }

    async fn restart_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "restart_delayed_event: room={}, delay_id={}",
            room_id, delay_id
        ));

        // MSC4140's `restart` action, not cancel-then-reschedule: one request,
        // and never a moment with no delayed leave armed.
        let promise = self
            .call_js_promise_method(
                "restartDelayedEvent",
                vec![JsValue::from_str(&room_id), JsValue::from_str(&delay_id)],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "cancel_delayed_event: room={}, delay_id={}",
            room_id, delay_id
        ));

        // Create a Promise that will be resolved by the JS callback
        let promise = self
            .call_js_promise_method(
                "cancelDelayedEvent",
                vec![JsValue::from_str(&room_id), JsValue::from_str(&delay_id)],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
        wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        let message_type = wire_event_type(&message_type);
        self.log_command(&format!(
            "send_to_device_message: {} recipient(s), type={}",
            recipients.len(),
            message_type
        ));

        let js_content = serde_wasm_bindgen::to_value(&content)
            .map_err(|e| CommandError::SerializationError(e.to_string()))?;
        // `[{userId, deviceId}, ...]`, mirroring matrix-js-sdk's own to-device
        // shape so a host can pass it straight through.
        let js_recipients = serde_wasm_bindgen::to_value(
            &recipients
                .iter()
                .map(|recipient| {
                    serde_json::json!({
                        "userId": recipient.user_id,
                        "deviceId": recipient.device_id,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|e| CommandError::SerializationError(e.to_string()))?;

        let promise = self
            .call_js_promise_method(
                "sendToDeviceMessage",
                vec![js_recipients, JsValue::from_str(message_type), js_content],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        let result = wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        // A host that resolves with nothing is taken to have served everyone —
        // the shape the callback had before it could report per recipient. One
        // that resolves with `[{userId, deviceId, error?}, ...]` is believed.
        if result.is_undefined() || result.is_null() {
            return Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect());
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct JsDelivery {
            user_id: String,
            device_id: String,
            #[serde(default)]
            error: Option<String>,
        }

        let reported: Vec<JsDelivery> = serde_wasm_bindgen::from_value(result).map_err(|e| {
            CommandError::SerializationError(format!(
                "sendToDeviceMessage resolved with something that is not a delivery list: {e}"
            ))
        })?;

        Ok(reported
            .into_iter()
            .map(|delivery| {
                let recipient = ToDeviceRecipient::new(delivery.user_id, delivery.device_id);
                match delivery.error {
                    Some(error) => ToDeviceDelivery::failed(recipient, error),
                    None => ToDeviceDelivery::sent(recipient),
                }
            })
            .collect())
    }
}

impl Default for JsCommandSender {
    fn default() -> Self {
        panic!("JsCommandSender requires a client object. Use new(client) instead.");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_type_structure() {
        // Verify the type can be referenced
        // Actual functionality tested in JavaScript tests
    }
}
