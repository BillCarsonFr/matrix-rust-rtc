# Agent Workflow (Shareable Template)

> **This file is a human-facing starter template.**
> Its content should be copy-pasted (and adapted) directly into a repo's `AGENTS.md`.
> Agents should never be pointed to this file — inline everything into `AGENTS.md` so agents read it in one pass.


## AI Working Folder (`agent-workspace/`)

A git-ignored sandbox for all transient AI-generated files. **Never use `/tmp/` or paths outside the workspace.**

### Rules

- **Write files** using file tools — never shell workarounds (`echo >>`, heredoc, etc.)
- **Overwriting**: always replace entire files; never partial edits on files like `pr-body.md`
- **Never store source code** or files meant to be reviewed here
- **Commit messages**: write to `agent-workspace/<feature-slug>/commit-msg.txt` and use `git commit -F agent-workspace/<feature-slug>/commit-msg.txt`
- **PR descriptions**: write to `agent-workspace/<feature-slug>/pr-body.md` and use `gh pr create --body-file agent-workspace/<feature-slug>/pr-body.md`

### Per-Feature Subfolder Convention

Every agent task that spans more than a single trivial edit **must** create a dedicated subfolder:

```
agent-workspace/<feature-slug>/
```

`feature-slug` is a short kebab-case label matching the branch name (e.g., `membership-routing`, `e2ee-key-dist`).

**Required files** (create as the work progresses, omit only if genuinely not applicable):

| File | Purpose |
|---|---|
| `plan.md` | Agreed approach before coding starts. Written after the user confirms direction. |
| `implementation-summary.md` | What was built, key decisions made, trade-offs. Written before handoff. |
| `commit-msg.txt` | Conventional commit message(s), one file per logical commit if batched. |
| `pr-body.md` | Full PR description. |

**Optional files:**

| File | Purpose |
|---|---|
| `NN-prompt.md` | Raw prompt transcript (numbered, e.g. `01-initial.md`, `02-debug.md`). Keep as many as useful for knowledge sharing. |
| `review-pr-findings.md` | Output of the PR self-review step. |
| `decisions.md` | Architectural/design decisions and the reasoning behind them. |

### Prompt Transcripts

Capturing prompts in `NN-prompt.md` files enables knowledge sharing with teammates. Each file is simply the raw prompt text as sent to the agent — no specific structure required.

---

## First-Pass Handoff (for Agents)

After implementing a requested change, stop and hand the result to the user before doing the full code-quality pass.

- Default workflow: implement the change, do only the narrowest sanity check needed to avoid an obviously broken handoff, then ask whether the user is happy with the result.
- Do **not** automatically run the full build, coverage, benchmark, or broad test suite immediately after implementation.
- Do **not** add follow-up work like wider refactors, extra tests or documentation polish until the user confirms the implementation direction or explicitly asks for the quality pass.
- If a validation step is needed before user feedback, keep it targeted to the touched code and state why that check is necessary.
- Once the user confirms the direction, complete the remaining quality work needed for the requested end state.

Do **not** commit on the first iteration. Write the code, show the user what changed, and wait for feedback. Only commit once the user confirms the direction is correct — or explicitly asks you to commit.

---

## Pre-Commit Checklist (for Agents)

This checklist is for commit/PR readiness, **not** for the initial implementation handoff.

Before committing any code change, run the project's standard validation suite (defined in `AGENTS.md`) and resolve all errors. Do not commit if any step fails.

**Manual review** — before committing, scan the diff against the project's code-quality standards (defined in `AGENTS.md`) and verify they all apply.
