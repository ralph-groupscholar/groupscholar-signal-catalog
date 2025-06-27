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
