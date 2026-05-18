---
name: create-pr
description: 'Create a pull request from current changes. Use when asked to commit changes, create a PR, open a pull request, submit changes, push to a branch, or make a PR. Produces separate conventional commits per logical concern.'
argument-hint: 'Brief description of the feature or fix (used for branch name and PR title)'
---

# Create PR

## When to Use
- User says "create a PR", "commit and push", "open a pull request", "submit these changes", "make a PR"
- After the user confirms the implementation direction — **never on the first iteration**

## Prerequisites

### Step 0: Self-Review (required — run after Step 4, before Step 5)

After writing `agent-workspace/pr-body.md` (Step 4) and before pushing (Step 5), load `skills/self-review/SKILL.md` and run the full self-review procedure against the diff. Resolve every finding before continuing. Only declare the self-review clean and proceed once no pattern matches remain.

This step catches the categories of issues that generate the most Copilot review comments (comment/implementation mismatches, CSS magic numbers, test fixture gaps, floating promises, docs coherence, etc.). See [PATTERNS.md](../self-review/PATTERNS.md) for the full catalogue.

### Step 1: Pre-Commit Checklist

Before each commit (Step 3), pass the build/lint/test checklist:

```sh
cargo check
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Do not proceed if any step fails. Fix the issue first.

## Procedure

### 1. Analyze the diff

Run `git diff --stat HEAD` and `git status` to understand all changed files.

Group changes into logical concerns — each concern becomes one commit. Examples:
- New utility function + its tests → one commit
- New component + its CSS module → one commit
- Type definition changes → bundle with the feature unless substantial
- Demo log extension → separate `chore(demo)` commit
- Documentation/AGENTS.MD changes → separate `docs` commit

### 2. Create a branch

```sh
git checkout -b <type>/<short-description>
```

Use the same type prefix as the primary commit. Lowercase, hyphen-separated.
Examples: `feat/search-filter`, `fix/parser-crash`, `chore/update-deps`

Branch policy (mandatory):
- First check current branch with `git rev-parse --abbrev-ref HEAD`.
- If on `main` or `master`, always create a new branch before committing.
- If already on a typed branch (`feat/*`, `fix/*`, `docs/*`, `refactor/*`, `test/*`, `chore/*`, `perf/*`, `style/*`):
  - Reuse it only when the current diff is scoped to the same task.
  - Otherwise create a new typed branch for the current task.
- Never commit PR work directly on `main`/`master`.

### 3. Commit each logical group

Use the file creation/edit tool to write each commit message to `agent-workspace/commit-msg.txt`. **Never use shell (`echo`, heredoc, `cat >`) — terminal encoding corrupts the content.** Then:

```sh
git add <files for this group>
git commit -F agent-workspace/commit-msg.txt
```

#### Conventional Commit Format

```
<type>(<scope>): <short description>

<optional body: what changed and why, not how>
```

**Types**

| Type | Use for |
|---|---|
| `feat` | New user-visible feature |
| `fix` | Bug fix |
| `refactor` | Code restructure without behavior change |
| `test` | Adding or updating tests only |
| `chore` | Build, tooling, config, demo log, deps |
| `docs` | Documentation only (AGENTS.MD, README, JSDoc) |
| `perf` | Performance improvement |
| `style` | CSS/visual changes with no logic change |

**Scopes** (optional but recommended):
`parser`, `store`, `views`, `components`, `extension`, `utils`, `types`, `deps`, `ci`, `demo`

**Rules**
- Subject line: imperative mood, lowercase, no trailing period, ≤72 chars
    - ✅ `feat(store): add time range filter`
    - ❌ `Added time range filter to store.`
- Body: explain *why*, not *what* — the diff shows what
- Breaking changes: add `!` after type/scope and a `BREAKING CHANGE:` footer

### 4. Write the PR description

Use the file creation/edit tool to write `agent-workspace/pr-body.md`. **Never use shell (`echo`, heredoc, `cat >`) — terminal encoding corrupts the content.**

Template:

```markdown
## Summary
<1-3 sentences describing what the PR does and why>

## Changes
- `<type>(scope): description` — what this commit does
- `<type>(scope): description` — what this commit does

## Testing
<Which commands to run and what to verify in the UI>
```

### 4.5 Self-review (mandatory)

Run the self-review step from `skills/self-review/SKILL.md` now, before any push/PR action.
This is mandatory even when no remote is configured.

If `origin/main` is unavailable, run self-review against the best local base branch
(typically `main` or `master`) and state which base was used.

- If findings exist: fix them, then re-run the relevant checks.
- If no findings: record `Self-review clean — no pattern matches found.` and continue.

### 5. Push and create the PR

```sh
git push -u origin <branch-name>
gh pr create --title "<type>(<scope>): <description>" --body-file agent-workspace/pr-body.md
```

The PR title should match the primary commit's subject line.

If the repository has no configured remote (or no pushable upstream), skip only this step (push/create PR)
and stop after:
1. Writing `agent-workspace/pr-body.md`
2. Completing Step 4.5 Self-review
3. Committing the changes

In that case, report the branch name and PR body content to the user so they
can push/create the PR later once a remote is available.

