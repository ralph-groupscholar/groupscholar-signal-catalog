# Group Scholar Signal Catalog

A lightweight CLI to log, track, and summarize program signals (risks, opportunities, partner notes, scholar feedback) so weekly briefings stay grounded in real inputs.

## Quick start

```bash
python3 app.py init
python3 app.py add --title "Partner onboarding delay" --category "partner" --severity "high" --owner "Leah" --due "2026-02-21" --notes "Two partners missing data exports" --source "weekly call" --tags "onboarding,ops"
python3 app.py list
python3 app.py summary
```

## Postgres backend

Use the Postgres backend for hosted deployments. Set `SIGNAL_CATALOG_DATABASE_URL` and either pass `--backend postgres` or set `SIGNAL_CATALOG_BACKEND=postgres`.

```bash
pip install -r requirements.txt
export SIGNAL_CATALOG_DATABASE_URL="postgres://user:pass@host:port/db"
python3 app.py --backend postgres init
python3 app.py --backend postgres list --status open
```

To seed the production database with realistic sample data:

```bash
export SIGNAL_CATALOG_DATABASE_URL="postgres://user:pass@host:port/db"
python3 app.py --backend postgres seed
```

## Commands

- `init`: create the local SQLite database
- `seed`: insert sample signals if none exist
- `add`: add a signal record
- `list`: list signals with filters
- `close`: close a signal by id
- `reopen`: reopen a signal by id
- `update`: update signal fields or append notes
- `summary`: rollups by status, category, severity, owner
- `export`: export filtered signals to CSV
- `digest`: generate a markdown digest of overdue, due soon, and recent signals
- `triage`: rank open signals by urgency and ownership gaps
- `workload`: summarize open-signal workload by owner with due buckets (table or markdown)
- `calendar`: show upcoming due dates grouped by week (table or markdown)
- `audit`: flag open signals missing owners, due dates, categories, severity, tags, or sources
- `metrics`: show operational metrics like open age, overdue counts, stale items, and close cycle time
- `stale`: list open signals that have not been updated recently
- `activity`: report new, closed, and updated signals in a recent window

## Examples

```bash
python3 app.py list --status open --category partner
python3 app.py close 3 --note "Partner sent exports"
python3 app.py update 3 --owner "Diego" --due "2026-02-26" --append-note "Aligned on new delivery date"
python3 app.py update 5 --status closed --notes "Resolved after mentor onboarding"
python3 app.py export --status open --out data/open-signals.csv
python3 app.py digest --days 14 --out data/weekly-digest.md
python3 app.py triage --days 10 --limit 12
python3 app.py workload --days 14
python3 app.py workload --days 14 --format markdown --out data/workload.md
python3 app.py calendar --days 30 --limit 6
python3 app.py calendar --days 45 --format markdown --out data/signal-calendar.md
python3 app.py audit --stale-days 21 --limit 10
python3 app.py metrics --due-days 14 --stale-days 14 --limit 5
python3 app.py stale --days 14 --limit 8
python3 app.py activity --days 7 --limit 6
python3 app.py activity --days 14 --format markdown --out data/signal-activity.md
```

## Data

The database lives at `data/signals.db` by default. Use `--db` to point elsewhere. Postgres uses the `groupscholar_signal_catalog.signals` table.
