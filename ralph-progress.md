# Ralph Progress Log

## Iteration 1
- Built the Group Scholar Signal Catalog CLI with SQLite storage, add/list/close/reopen/export commands, and summary rollups.
- Documented usage and quick-start examples in the README.

## Iteration 2
- Added a markdown digest generator to summarize overdue, due-soon, and recent signals for weekly briefs.
- Expanded the README with digest usage examples.

## Iteration 3
- Added a triage command that ranks open signals by urgency, aging, and ownership gaps with a snapshot summary.
- Updated README with triage usage.

## Iteration 4
- Added optional Postgres backend support with environment-based configuration while preserving SQLite defaults.
- Added a Postgres seed script with realistic sample signals and documented backend setup in the README.
- Added a requirements.txt entry for psycopg and seeded the production database table.

## Iteration 5
- Added an update command to edit signal fields, append notes, or clear values while handling status transitions.
- Documented update usage in the README examples.

## Iteration 6
- Added a metrics command to report open/closed counts, overdue/due-soon totals, age medians, and close-cycle timing.
- Included severity rollups and oldest-open signal visibility plus README usage details.

## Iteration 83
- Added an audit command to flag open signals missing owners, due dates, categories, severity, tags, or sources, plus aging/overdue alerts.
- Documented audit usage in the README.

## Iteration 84
- Expanded the workload report to support markdown or table output with optional file export.
- Updated the README with workload format and output examples.

## Iteration 113
- Added a calendar command to group upcoming due dates by week with overdue, no-due, and beyond-horizon sections.
- Documented calendar usage examples in the README.

## Iteration 109
- Added updated_at tracking for signals with automatic backfill and update on edits, close, and reopen.
- Added a stale command plus metrics stale counts to surface open signals without recent updates.
- Expanded exports to include updated_at and documented the new command in the README.

## Iteration 70
- Expanded the activity report with snapshot metrics, owner/category/severity rollups, and due-soon/overdue context.
- Added a unit test covering the activity snapshot counts.

## Iteration 71
- Added an activity report that summarizes newly created, closed, and updated signals within a recent window.
- Added markdown output and file export options for the activity report.
- Documented the new activity command in the README examples.

## Iteration 72
- Added a trend report that shows weekly created/closed volume, net change, and average close cycle time with optional markdown output.
- Introduced an --as-of option for deterministic trend windows and created a unit test for trend counts.
- Documented the new trend command in the README examples.
