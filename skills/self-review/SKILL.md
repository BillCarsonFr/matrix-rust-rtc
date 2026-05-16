---
name: self-review
description: 'Pre-PR self-review checklist to catch recurring review comment patterns before pushing. Use when asked to self-review, run pre-PR review, or before creating a PR to reduce review back-and-forth.'
argument-hint: 'Reviews the current branch against origin/main using the merge base'
---

# Self-Review

Run this skill before creating any PR.

## When to Use
- As required by the `create-pr` skill's Step 0 gate
- When asked to "self-review", "run pre-PR review", or "check for review issues"

## Procedure

### 1. Get the diff

```sh
git diff $(git merge-base HEAD origin/main) HEAD -- . ':(exclude)agent-workspace' ':(exclude)*.png' ':(exclude)*.jpg' ':(exclude)*.jpeg' ':(exclude)*.gif' ':(exclude)*.webp' ':(exclude)*.svg' ':(exclude)*.ico'
git diff --stat $(git merge-base HEAD origin/main) HEAD -- . ':(exclude)agent-workspace' ':(exclude)*.png' ':(exclude)*.jpg' ':(exclude)*.jpeg' ':(exclude)*.gif' ':(exclude)*.webp' ':(exclude)*.svg' ':(exclude)*.ico'
```

Read the stat output to understand which file types changed.


### 2. Review

Code reviewer focus on ensuring code quality, security, performance, and maintainability using cutting-edge analysis tools and techniques.
Combines deep technical expertise with modern AI-assisted review processes, static analysis tools, and production
reliability practices to deliver comprehensive code assessments that prevent bugs, security vulnerabilities,
and production incidents.

#### Code Quality & Maintainability
- Clean Code principles and SOLID pattern adherence
- Design pattern implementation and architectural consistency
- Code duplication detection and refactoring opportunities
- Naming convention and code style compliance
- Technical debt identification and remediation planning
- Code complexity reduction and simplification techniques
- Maintainability metrics and long-term sustainability assessment

### 3. Declare result

After walking all changed files against all applicable patterns:

- If no findings: state "Self-review clean — no pattern matches found." and proceed to the create-pr skill.
- If findings exist: list each one (file + line + pattern ID + description). Fix all of them before proceeding. Re-run the relevant tests/clippy as a spot-check after fixing.
