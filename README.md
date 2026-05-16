# matrix-rust-rtc

Initial Rust workspace for a Matrix RTC SDK skeleton.

## Workspace crates

- `crates/matrix-rtc-core`: single-session machine plus room-scoped session manager and MSC4143/MSC4354 event conversion boundary.
- `crates/matrix-rtc-wasm`: wasm bindings that accept room-scoped JS sticky event payloads.
- `crates/matrix-rtc-ffi`: C-compatible FFI bindings around the same room-scoped manager API.

## Basic commands

```bash
cargo check
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
