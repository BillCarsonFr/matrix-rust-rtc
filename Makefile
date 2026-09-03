#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# This file is part of matrix-rust-rtc.
#
# matrix-rust-rtc is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# matrix-rust-rtc is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

# Makefile for common development tasks

.PHONY: help setup build-check fmt fmt-check clippy test build-ffi build-mobile build-android build-ios clean backend-up backend-down backend-logs test-e2e interop-up interop-down interop-logs interop-trust test-interop

help:
	@echo "Matrix RTC Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup              Install development dependencies"
	@echo ""
	@echo "Quality Checks:"
	@echo "  make fmt                Format code"
	@echo "  make fmt-check          Check code formatting without changes"
	@echo "  make clippy             Run clippy linter"
	@echo "  make test               Run all tests"
	@echo "  make build-check        Check builds for all crates"
	@echo ""
	@echo "E2E / local backend (demo/backend):"
	@echo "  make backend-up         Start the MatrixRTC backend stack (docker compose)"
	@echo "  make backend-down       Tear the backend stack down"
	@echo "  make backend-logs       Follow the backend stack logs"
	@echo "  make test-e2e           Run the e2e call test against the backend stack (upstream SDK: pre-sticky mode only)"
	@echo ""
	@echo "MSC4354 sticky events (fork SDK, see .cargo/experimental-sticky.toml):"
	@echo "  make check-sticky       cargo check with the fork SDK and experimental-sticky"
	@echo "  make clippy-sticky      Clippy with the fork SDK and experimental-sticky"
	@echo "  make test-e2e-sticky    Run the full e2e suite (sticky + pre-sticky) with the fork SDK"
	@echo ""
	@echo "Element Call interop (demo/backend/INTEROP.md):"
	@echo "  make interop-up         Start the backend stack + TLS, Element Web/Call"
	@echo "  make interop-down       Tear the interop stack down"
	@echo "  make interop-logs       Follow the interop stack logs"
	@echo "  make interop-trust      Print how to trust the stack's dev CA"
	@echo "  make test-interop       Run the Element Call interop test (Playwright)"
	@echo ""
	@echo "Build Mobile:"
	@echo "  make build-mobile       Build both Android AAR and iOS XCFramework (interactive)"
	@echo "  make build-android      Build Android AAR (slim, signalling only)"
	@echo "  make build-ios          Build iOS XCFramework (slim, signalling only)"
	@echo "  make build-android-media Build Android AAR with media (frame streams; libwebrtc)"
	@echo "  make build-ios-media    Build iOS XCFramework with media (frame streams; libwebrtc)"
	@echo "  make build-ffi          Build FFI crate only"
	@echo "  make test-ffi-media     Run the media FFI smoke tests (needs libwebrtc build)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean              Clean build artifacts"
	@echo ""

setup:
	cargo install cargo-ndk
	rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
	rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
	@echo "✅ Setup complete!"

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

# Explicit features rather than `--all-features`: that would also turn on
# `experimental-sticky`, whose code needs the fork SDK and does not compile
# against the upstream one the workspace depends on. This set covers every
# target the upstream build can have, including the SDK-backed half of the
# livekit crate (`call`, the bridge's `sdk`) and the FFI's media surface.
UPSTREAM_FEATURES = matrix-rtc-livekit/matrix-sdk,matrix-rtc-livekit/testing,matrix-rtc-ffi/media

clippy:
	cargo clippy --workspace --all-targets --features $(UPSTREAM_FEATURES) -- -D warnings

# ---- MSC4354 sticky events (fork SDK) -------------------------------------
# Everything below runs through scripts/cargo-sticky.sh, which applies the
# `.cargo/experimental-sticky.toml` overlay (redirecting matrix-rust-sdk to the
# fork that has sticky events), builds in target/sticky, and keeps the sticky
# lockfile in Cargo.sticky.lock so the committed Cargo.lock stays the upstream
# one. The SDK's own `unstable-msc4354` is passed here because our
# `experimental-sticky` feature cannot forward to a feature upstream lacks.
STICKY_FEATURES = matrix-sdk,testing,experimental-sticky,matrix-sdk/unstable-msc4354,matrix-sdk-ui/unstable-msc4354

.PHONY: check-sticky clippy-sticky test-e2e-sticky
check-sticky:
	./scripts/cargo-sticky.sh check -p matrix-rtc-livekit --all-targets --features $(STICKY_FEATURES)

clippy-sticky:
	./scripts/cargo-sticky.sh clippy -p matrix-rtc-livekit --all-targets --features $(STICKY_FEATURES) -- -D warnings

# The full e2e suite: the three sticky scenarios plus the pre-sticky one.
test-e2e-sticky: backend-up
	./scripts/cargo-sticky.sh test -p matrix-rtc-livekit --features $(STICKY_FEATURES) --test e2e_call -- --ignored --nocapture --test-threads=1

test:
	cargo test --all

build-check:
	cargo check --all

build-ffi:
	cargo build -p matrix-rtc-ffi --release

build-mobile:
	./scripts/build-mobile.sh

build-android:
	./scripts/build-android-aar.sh

build-ios:
	./scripts/build-ios-xcframework.sh

# Media variants: matrix-rtc-ffi with the `media` feature (frame streams,
# publishing, constraints). Pulls libwebrtc — needs a C++ toolchain / the NDK
# and grows the binaries; see mobile/PACKAGING.md.
.PHONY: build-android-media build-ios-media test-ffi-media
build-android-media:
	MEDIA=1 ./scripts/build-android-aar.sh

build-ios-media:
	MEDIA=1 ./scripts/build-ios-xcframework.sh

# In-process smoke tests of the media FFI surface (no SFU or homeserver
# needed, but compiles libwebrtc).
test-ffi-media:
	cargo test -p matrix-rtc-ffi --features media

clean:
	cargo clean
	rm -rf mobile/ios/build
	rm -rf mobile/ios/generated
	rm -rf mobile/android/matrixrtc/src/main/jniLibs
	rm -rf mobile/android/matrixrtc/build

backend-up:
	docker compose -f demo/backend/docker-compose.yml up -d --wait
	./demo/backend/wait-ready.sh

backend-down:
	docker compose -f demo/backend/docker-compose.yml down -v

# Full reset: also wipe the persisted homeserver state (./data is a bind
# mount, so `down -v` alone keeps it). Use when synapse starts returning 500s
# (e.g. a wedged sqlite writer / stale WAL after a killed run) — everything in
# the stack is throwaway dev state and regenerates on the next backend-up.
backend-reset: backend-down
	rm -rf demo/backend/data/synapse demo/backend/data/tls

backend-logs:
	docker compose -f demo/backend/docker-compose.yml logs -f

# Against the upstream SDK only the pre-sticky scenario exists; the sticky
# scenarios are compiled out without `experimental-sticky` (see
# test-e2e-sticky). --test-threads=1: the scenarios share the compose stack;
# running them in parallel would double the load and interleave logs.
test-e2e: backend-up
	cargo test -p matrix-rtc-livekit --features matrix-sdk,testing --test e2e_call -- --ignored --nocapture --test-threads=1

# ---- Element Call interop ------------------------------------------------
# The same base stack plus TLS (nginx + a dev CA) and Element Web, which ships
# Element Call embedded. A browser needs a secure context for WebRTC and the
# widget API, which http://localhost cannot give the iframe. See INTEROP.md.

.PHONY: interop-up interop-down interop-logs interop-trust test-interop
INTEROP_COMPOSE = docker compose -f demo/backend/docker-compose.yml -f demo/backend/docker-compose.interop.yml

interop-up:
	$(INTEROP_COMPOSE) up -d --wait
	./demo/backend/wait-ready.sh --interop
	@$(MAKE) --no-print-directory interop-trust

interop-down:
	$(INTEROP_COMPOSE) down -v

interop-logs:
	$(INTEROP_COMPOSE) logs -f

# The test wires the dev CA into the Rust peer (SSL_CERT_FILE) and Node
# (NODE_EXTRA_CA_CERTS) itself, so the only host-side prerequisite is name
# resolution. This just says so, rather than installing anything: mutating the
# machine's trust store is not something a `make` target should do behind your
# back. See INTEROP.md.
interop-trust:
	@echo ""
	@echo "Interop stack is up. One host-side prerequisite (once per machine):"
	@echo ""
	@echo "  /etc/hosts:"
	@echo "    127.0.0.1 synapse.m.localhost matrix-rtc.m.localhost app.m.localhost"
	@echo ""
	@echo "  Element Web: https://app.m.localhost"
	@echo "  (Your browser will warn on the dev cert; Playwright ignores it.)"
	@echo ""
	@echo "  Running the peer or the examples by hand, outside the test:"
	@echo "    export SSL_CERT_FILE=\"$$PWD/demo/backend/data/tls/local-ca.crt\""
	@echo ""

test-interop: interop-up
	cargo build -p matrix-rtc-livekit --features matrix-sdk,testing --example interop_peer
	# The web peer: wasm bindings into web/pkg, then its page deps. Playwright's
	# webServer builds and serves the page itself.
	./web/scripts/build-bindings.sh
	cd web/demo && { [ -f package-lock.json ] && npm ci || npm install; }
	cd interop && { [ -f package-lock.json ] && npm ci || npm install; } && npx playwright test

.PHONY: quality-check
quality-check: fmt-check clippy test build-check
	@echo "✅ All quality checks passed!"

