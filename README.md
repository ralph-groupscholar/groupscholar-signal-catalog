# Group Scholar Signal Catalog

A lightweight CLI to log, track, and summarize program signals (risks, opportunities, partner notes, scholar feedback) so weekly briefings stay grounded in real inputs.

## Quick start

```bash
python app.py init
python app.py add --title "Partner onboarding delay" --category "partner" --severity "high" --owner "Leah" --due "2026-02-21" --notes "Two partners missing data exports" --source "weekly call" --tags "onboarding,ops"
python app.py list
python app.py summary
```

## Commands

- `init`: create the local SQLite database
- `add`: add a signal record
- `list`: list signals with filters
- `close`: close a signal by id
- `reopen`: reopen a signal by id
- `summary`: rollups by status, category, severity, owner
- `export`: export filtered signals to CSV
- `digest`: generate a markdown digest of overdue, due soon, and recent signals
- `triage`: rank open signals by urgency and ownership gaps

## Examples

```bash
python app.py list --status open --category partner
python app.py close 3 --note "Partner sent exports"
python app.py export --status open --out data/open-signals.csv
python app.py digest --days 14 --out data/weekly-digest.md
python app.py triage --days 10 --limit 12
```

## Data

The database lives at `data/signals.db` by default. Use `--db` to point elsewhere.
