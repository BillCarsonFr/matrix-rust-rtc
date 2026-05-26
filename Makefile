#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

# Makefile for common development tasks

.PHONY: help setup build-check fmt fmt-check clippy test build-ffi build-mobile build-android build-ios clean

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
	@echo "Build Mobile:"
	@echo "  make build-mobile       Build both Android AAR and iOS XCFramework (interactive)"
	@echo "  make build-android      Build Android AAR"
	@echo "  make build-ios          Build iOS XCFramework"
	@echo "  make build-ffi          Build FFI crate only"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean              Clean build artifacts"
	@echo ""

setup:
	cargo install uniffi_bindgen cargo-ndk
	rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
	rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
	@echo "✅ Setup complete!"

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

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

clean:
	cargo clean
	rm -rf mobile/ios/build
	rm -rf mobile/ios/generated
	rm -rf mobile/android/matrixrtc/src/main/jniLibs
	rm -rf mobile/android/matrixrtc/build

.PHONY: quality-check
quality-check: fmt-check clippy test build-check
	@echo "✅ All quality checks passed!"

