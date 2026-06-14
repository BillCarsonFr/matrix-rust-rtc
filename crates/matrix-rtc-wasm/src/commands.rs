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
use matrix_rtc_core::{CommandError, RtcCommandSender};
use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// WASM implementation of the RtcCommandSender trait.
///
/// This sender delegates to a JavaScript object that provides the actual Matrix SDK integration.
/// The client must implement methods: sendStickyEvent, sendDelayedEvent, cancelDelayedEvent.
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
    /// - sendStickyEvent(roomId, eventType, content, callback)
    /// - sendDelayedEvent(roomId, eventType, content, delayMs, callback)
    /// - cancelDelayedEvent(roomId, eventId, callback)
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
        if let Some(callback) = &self.on_command {
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(description));
        }
    }

    fn convert_js_error(error: JsValue) -> CommandError {
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
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "send_sticky_event: room={}, type={}",
            room_id, event_type
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
                    JsValue::from_str(&event_type),
                    js_content,
                ],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
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
                    JsValue::from_str(&event_type),
                    js_content,
                    JsValue::from_f64(delay_ms as f64),
                ],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
        // The Promise should resolve to the event_id string
        let js_result = wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        // Extract the event_id from the result
        let event_id = js_result.as_string().ok_or_else(|| {
            CommandError::SendError("sendDelayedEvent did not return a string event_id".to_string())
        })?;

        Ok(event_id)
    }

    async fn cancel_delayed_event(
        &self,
        room_id: String,
        event_id: String,
    ) -> Result<(), CommandError> {
        self.log_command(&format!(
            "cancel_delayed_event: room={}, event_id={}",
            room_id, event_id
        ));

        // Create a Promise that will be resolved by the JS callback
        let promise = self
            .call_js_promise_method(
                "cancelDelayedEvent",
                vec![JsValue::from_str(&room_id), JsValue::from_str(&event_id)],
            )
            .map_err(JsCommandSender::convert_js_error)?;

        // Convert the Promise to a Rust Future and await it
        wasm_bindgen_futures::JsFuture::from(promise)
            .await
            .map_err(JsCommandSender::convert_js_error)?;

        Ok(())
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
