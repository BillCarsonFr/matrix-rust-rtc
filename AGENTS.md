# Agent Context: Matrix RTC Rust

## Project Overview

This project is a Rust implementation of a Matrix RTC (Real-Time Communication) client. 
It is designed to facilitate real-time communication features such as voice and video calls within the Matrix ecosystem.

The idea is to provide a core RTC SDK in Rust that can be used across multiple platforms, with bindings
for web (via WebAssembly) and native mobile platforms (via FFI).
This allows us to maintain a single codebase for the core RTC functionality while enabling broad platform support.

At the higher level, the rtc-sdk is fed events from the Matrix client (e.g., incoming call, call state changes)
and provides an API for managing RTC sessions, like membership management, call control.
The rtc-sdk will itself send commands back to the Matrix client to perform actions like accepting/declining a call,
updating call state, sending reactions, raising hand, handling key distribution for E2EE calls, etc.

The project provides clean interfaces for the Matrix client to interact with the RTC functionality, while abstracting away platform-specific details.
It can then be used in web in conjunction with the matrix-js-sdk and in mobile with the matrix-rust-sdk bindings.

// TODO flesh out the architecture and design principles more in the ARCHITECTURE.md, but the high-level idea is:
- The core rtc-sdk is implemented in Rust, providing the main logic and state management for RTC sessions.
- For web, we compile the Rust code to WebAssembly and provide JavaScript bindings for easy integration with the matrix-js-sdk.
- For native mobile platforms, we provide FFI bindings that can be used in the matrix-rust-sdk to integrate RTC functionality into iOS and Android apps.
All organized in a rust workspace with clear boundaries between the core logic and platform-specific bindings.
There will also be a crate to manage the livekit transport integration (MSC4195), using the rust livekit client library, that
can be used to record calls via a headless bot


## Audience & Scope
This document is a lightweight guide for contributors and automated agents. It focuses on stable concepts and boundaries, not implementation details.

## Development Phase

This project is in active development.

- Source and API/ABI breaking changes are acceptable for now.
- Prioritize clarity and fast iteration over backward compatibility.
- Prefer direct renames/removals instead of compatibility shims unless explicitly requested.

## Tech Stack (High-Level)
- Rust for core rtc sdk
- Wasm for web bindings
- FFI bindings for native integration (mobile)

## Repository Layout (Intent-Oriented)

## Core Principles

The `skills/<skill-name>` folder contains reusable skill for agents.
Level 1: On startup reads only the name and description from every `SKILL.md`.
Level 2: When a skill is relevant to a task, the agent loads the full markdown and executes according to its instructions.
Level 3: Some skills folder have a `references/` subfolder for static files (docs, templates, checklists), Agent should only read them when relevant (load on demand).


Always read the karpathy-guidelines skill before coding (`skills/karpathy-guidelines/SKILL.md`).

## Useful Commands

- `cargo check`
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- Web bindings: `cd web && npm run build && npm test`
- Android bindings: `./scripts/build-android-aar.sh`
- iOS bindings (macOS): `./scripts/build-ios-xcframework.sh`

## AI Working Folder (`agent-workspace/`)

A git-ignored sandbox for all transient AI-generated files. **Never use `/tmp/` or paths outside the workspace.**

- Use for planning notes, PR script outputs, commit messages, and PR descriptions
- **Write files** using file tools — never shell workarounds (`echo >>`, heredoc, etc.)
- **Overwriting**: always replace entire files; never partial `replace_string_in_file` edits on files like `pr-body.md`
- **Commit messages**: write to `agent-workspace/commit-msg.txt` and use `git commit -F agent-workspace/commit-msg.txt`
- **PR descriptions**: write to `agent-workspace/pr-body.md` and use `gh pr create --body-file agent-workspace/pr-body.md`
- **Never store source code** or files meant to be reviewed here

Do **not** commit on the first iteration. Write the code, show the user what changed, and wait for feedback. Only commit once the user confirms the direction is correct — or explicitly asks you to commit.

## First-Pass Handoff (for Agents)

After implementing a requested change, stop and hand the result to the user before doing the full code-quality pass.

- Default workflow: implement the change, do only the narrowest sanity check needed to avoid an obviously broken handoff, then ask whether the user is happy with the result.
- Do **not** automatically run the full build, coverage, benchmark, or broad test suite immediately after implementation.
- Do **not** add follow-up work like wider refactors, extra tests or documentation polish until the user confirms the implementation direction or explicitly asks for the quality pass.
- If a validation step is needed before user feedback, keep it targeted to the touched code and state why that check is necessary.
- Once the user confirms the direction, complete the remaining quality work needed for the requested end state.

## Pre-Commit Checklist (for Agents)

This checklist is for commit/PR readiness, not for the initial implementation handoff.

Before committing **any** code change (new feature, bug fix, PR comment fix, refactor, etc.), always run the following commands and resolve all errors before proceeding:

`cargo check`
`cargo fmt`
`cargo clippy --all-targets --all-features -- -D warnings`
`cargo test`

Then run binding tasks for any touched binding surface:

- If changes touch `crates/matrix-rtc-wasm/**` or `web/**`:
  - `cd web && npm run build`
  - `cd web && npm test`
- If changes touch `crates/matrix-rtc-ffi/**`, `mobile/**`, or `scripts/build-*.sh`:
  - `./scripts/build-android-aar.sh`
  - `./scripts/build-ios-xcframework.sh` (on macOS)

If a required platform/toolchain is not available locally, document the skip reason in the PR description and ensure the corresponding CI job passes before merge.

Do not commit if any of these steps fail. Fix the issue first, then re-run the full checklist before committing.

**Manual review** — before committing, scan the diff against each pillar in [Code Quality Standards](#code-quality-standards) and verify all apply.

## Creating PRs (for Agents)

When asked to commit and create a PR, follow `skills/create-pr/SKILL.md`. Core steps:
1. Pass the Pre-Commit Checklist above before each commit.
2. Analyze `git diff` and group changes into logical commits.
3. Create a branch (`feat/`, `fix/`, etc.) and commit each group with a conventional commit message: `type(scope): description`.
4. Write PR description to `agent-workspace/pr-body.md`.
5. **Self-review** — load `skills/self-review/SKILL.md` (`/self-review`) and resolve all findings before pushing.


## PR Review Workflow (for Agents)
When prompted to **"review-pr"**, follow `skills/review-pr/SKILL.md`. This is an offline review flow and does not require GitHub/GitLab.

Expected state before running this workflow:
- Feature/fix work is already committed on a branch (typically via `skills/create-pr/SKILL.md`)
- PR description exists at `agent-workspace/pr-body.md`

Core steps:
1. Read `agent-workspace/pr-body.md` to understand intent/scope.
2. Compute diff from current branch to `master`:
   - `git diff master`
   - `git diff --stat master`
3. Conduct an independent review of changed files against:
   - PR description accuracy and completeness
   - Repository guidance in this file and `ARCHITECTURE.md`
   - Consistency with existing codebase patterns
   - Refactoring opportunities that improve clarity/maintainability
4. Write findings to `agent-workspace/review-pr-findings.md` with this structure:
   - Quick summary
   - One section per file, with comments referencing line numbers
5. Reviewer is read-only: never edit code, stage files, commit, or push.

Output contract:
- If findings exist: provide actionable, respectful comments with file + line references.
- If no findings: write exactly `Review Completed - No findings`.


## Performance Regression Detection

// TODO

## Contribution Guidelines
- Always pass the full [Pre-Commit Checklist](#pre-commit-checklist-for-agents), including binding build/test tasks for touched binding surfaces, before committing.

## Comments and Module Documentation

- Add a short module-level rustdoc comment (`//!`) in each new Rust module and in modified modules when missing.
- Module comments should explain intent and boundaries, not line-by-line behavior.
- For boundary/transport modules, explicitly mention DTO rationale: DTOs are used to decouple core logic from platform-specific SDK or FFI types.
- Keep comments concise (2-6 lines is usually enough), factual, and maintenance-friendly.

