---
name: review-pr
description: 'Run an independent offline PR review from current branch changes and pr-body.md. Use when asked to review-pr, review the branch, or perform a pre-merge review without GitHub/GitLab.'
argument-hint: 'Optional focus area (e.g., correctness, architecture, performance, readability)'
---

# Review PR (Offline)

Run this skill when a separate agent must review committed branch changes without modifying code.

## When to Use
- User says "review-pr", "review this branch", "do an independent PR review", or similar
- A branch already contains commits and `agent-workspace/pr-body.md` exists

## Inputs
- `agent-workspace/pr-body.md`
- Git diff from current branch to `master`

## Procedure

### 1. Read the PR intent
Read `agent-workspace/pr-body.md` first and capture:
- Claimed scope
- Claimed behavior changes
- Claimed testing/validation

### 2. Collect the review diff (offline)
Use local `master` directly:

```sh
git diff master
git diff --stat master
```

If `master` is unavailable locally, use `main` and explicitly note the fallback in the report.

### 3. Review independently
Evaluate changed files for:
- Correctness and behavioral risk
- Alignment with `AGENTS.md` and `ARCHITECTURE.md`
- Consistency with repository patterns and style
- Missing edge cases/tests where relevant
- Practical refactoring opportunities (avoid speculative redesign)
- PR description vs actual diff mismatches

Review tone must be respectful, specific, and actionable.

### 4. Produce findings report
Write `agent-workspace/review-pr-findings.md` with this format:

```markdown
## Review Summary
<2-5 lines: scope reviewed, overall quality, major risks>

## <path/to/file1>
- L<line>: <finding or suggestion>
- L<line>: <finding or suggestion>

## <path/to/file2>
- L<line>: <finding or suggestion>
```

Rules:
- Group comments by file
- Include line numbers for each comment
- Focus on impactful items first
- Do not include comments for unchanged files

### 5. No-findings output
If there are no findings, write exactly:

`Review Completed - No findings`

## Hard Guardrails
- Reviewer is read-only: do not change source files
- Do not stage, commit, rebase, or push
- Do not rewrite `agent-workspace/pr-body.md`
- Only write/update `agent-workspace/review-pr-findings.md`

