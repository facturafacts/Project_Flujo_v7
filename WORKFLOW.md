# WORKFLOW.md — Mercado Pago Pipeline

## Purpose

This document defines the operating conventions for this project. It exists so that any person (or AI) touching this repo knows exactly how to work with it safely.

---

## The Core Rule

> **Commit before you change. Push after every commit.**
> This repo is a backup + checkpoint system. Every session that modifies files must leave them pushed before closing.

---

## Commit Triggers

Commit immediately **before** making any of these changes:

- Adding or modifying any script (`.py`)
- Editing `db_manager.py` or schema logic
- Changing `categories.csv` (chart of accounts)
- Editing `README.md` or this `WORKFLOW.md`
- Running a new sync or merge that updates the ledger
- Importing or exporting Excel labeling files
- Modifying `.env` or config files

**Rule of thumb:** If you modified a file and it worked — commit it.

---

## Commit Message Format

```
<type>: <what changed>

<optional context or why>
```

Types: `feat`, `fix`, `refactor`, `data`, `docs`, `chore`

Examples:
```
feat: add POS cache sync script
fix: correct sign handling on bank withdrawals
data: reload ledger — Jan 2026 movements added
docs: update README with new labeling workflow
```

---

## Never Commit These

| File/Folder | Why |
|---|---|
| `.env` | Contains live API token |
| `data/ledger.db` | Live transaction data |
| `data/reports/*.csv` | Raw bank CSVs from Mercado Pago |
| `.venv/` | Python environment |
| `__pycache__/` | Python cache |
| `output/*.xlsx` | Large binary outputs (optional — can track versions) |

---

## Git Workflow Summary

```
# 1. Make your changes
python3 scripts/live_sync.py

# 2. Commit before continuing
git add -A
git commit -m "data: sync — last 7 days of payments"

# 3. Push immediately (this is a backup repo)
git push
```

---

## Branching (Optional)

- `master` is the stable, pushed branch
- For experimental scripts or major refactors: create a branch
- Merge back to `master` only after testing locally
- Never push experimental branches to origin unless intentional

---

## If Something Breaks

1. `git log` → find the last good commit
2. `git diff <bad> <good>` → review what changed
3. Fix in a new commit, don't amend published history
4. Push the fix

---

## Questions or Edge Cases

If this workflow doesn't cover a situation — update this document as part of resolving it. The doc earns its place by being kept current.
