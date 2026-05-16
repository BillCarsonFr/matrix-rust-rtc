---
name: matrix-msc
description: Use Matrix MSC documents as authoritative implementation references. Resolve MSCs by number, load the corresponding reference file, and implement strictly according to spec language.
argument-hint: 'Mention an MSC by number (e.g. MSC4143) to load its reference and implement accordingly'
license: MIT
---

# Matrix MSC Skill

This skill governs how to use Matrix Spec Change (MSC) documents during analysis, design, and implementation.

## Scope

- The authoritative MSC references are stored in `skills/msc/references/`.
- Each MSC file follows the naming convention: `msc<NUMBER>.md` (lowercase), e.g.:
    - `skills/msc/references/msc4143.md`
    - `skills/msc/references/msc4195.md`
    - `skills/msc/references/msc4354.md`

## MSC Resolution Rules

When a request mentions an MSC, resolve it as follows:

1. Detect MSC identifiers in any of these forms:
    - `MSC4143`
    - `msc4143`
    - `MSC 4143`
    - `4143` (only if clearly referring to an MSC in context)

2. Normalize to numeric ID `<NUMBER>` and map to:
    - `skills/msc/references/msc<NUMBER>.md`

3. Load the corresponding file(s) before proposing implementation details.

4. If the MSC reference file is missing:
    - State that the local reference is unavailable.
    - Ask for the missing MSC text or permission to proceed with limited assumptions.
    - Do not invent normative requirements.

## Spec-First Implementation Policy

Treat MSC text as normative:

- `MUST` / `MUST NOT`: hard requirements, no deviation.
- `SHOULD` / `SHOULD NOT`: follow unless a justified reason is documented.
- `MAY`: optional behavior.

Implementation guidance:

- Do not contradict normative MSC language.
- Do not replace explicit spec behavior with “best effort” alternatives.
- If code and spec disagree, flag the mismatch and propose spec-compliant changes.
- If multiple MSCs apply, satisfy all constraints and call out conflicts explicitly.

## Working Procedure

For any Matrix RTC task touching protocol behavior:

1. Identify referenced MSC(s).
2. Load and read matching file(s) in `skills/msc/references/`.
3. Extract normative requirements relevant to the task.
4. Translate requirements into concrete code-level checks, structures, and flows.
5. Validate proposed behavior against the MSC text before finalizing.

## Output Expectations

When giving implementation guidance or review feedback:

- Cite the exact MSC number(s), e.g. `MSC4143`, `MSC4195`, `MSC4354`.
- Reference the local file path used, e.g. `skills/msc/references/msc4195.md`.
- Distinguish clearly between:
    - Normative requirement (from MSC)
    - Implementation decision (project-specific)
- If uncertain, ask for clarification rather than guessing.

## MatrixRTC Notes

For this project, common dependencies include:

- `MSC4143` for MatrixRTC core semantics
- `MSC4195` for LiveKit transport details
- `MSC4354` for sticky event semantics used by RTC membership/state

Always verify cross-MSC assumptions before implementation.
