#!/usr/bin/env python3
import argparse
import csv
import os
import sqlite3
import sys
import statistics
from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime, timedelta

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # Optional dependency for Postgres
    psycopg = None
    dict_row = None

DEFAULT_SEVERITY = "medium"
DEFAULT_DIGEST_DAYS = 7
DEFAULT_DIGEST_LIMIT = 8
DEFAULT_TRIAGE_DAYS = 14
DEFAULT_TRIAGE_LIMIT = 10
DEFAULT_METRICS_DUE_DAYS = 14
DEFAULT_AUDIT_LIMIT = 8
DEFAULT_AUDIT_STALE_DAYS = 21
DEFAULT_WORKLOAD_DAYS = 14
DEFAULT_STALE_DAYS = 14
DEFAULT_CALENDAR_DAYS = 30
DEFAULT_CALENDAR_LIMIT = 6
POSTGRES_SCHEMA = "groupscholar_signal_catalog"
POSTGRES_TABLE = f"{POSTGRES_SCHEMA}.signals"


@dataclass
class DBConfig:
    backend: str
    db_path: Optional[str]
    dsn: Optional[str]
    table: str


def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_days_ago(days):
    return (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def default_db_path():
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "data", "signals.db")


def ensure_data_dir(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)


def resolve_db_config(args):
    backend = args.backend or os.getenv("SIGNAL_CATALOG_BACKEND", "sqlite")
    backend = backend.lower()
    if backend not in {"sqlite", "postgres"}:
        print(f"Unsupported backend '{backend}'. Use sqlite or postgres.")
        sys.exit(1)
    if backend == "postgres":
        dsn = args.database_url or os.getenv("SIGNAL_CATALOG_DATABASE_URL") or os.getenv("DATABASE_URL")
        if not dsn:
            print("SIGNAL_CATALOG_DATABASE_URL (or DATABASE_URL) is required for the postgres backend.")
            sys.exit(1)
        return DBConfig(backend="postgres", db_path=None, dsn=dsn, table=POSTGRES_TABLE)
    return DBConfig(backend="sqlite", db_path=args.db, dsn=None, table="signals")


def connect(db):
    if db.backend == "sqlite":
        if not db.db_path:
            print("SQLite database path is missing.")
            sys.exit(1)
        ensure_data_dir(db.db_path)
        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    if psycopg is None:
        print("psycopg is required for the postgres backend. Install with pip.")
        sys.exit(1)
    conn = psycopg.connect(db.dsn, row_factory=dict_row)
    return conn


def placeholder(db):
    return "?" if db.backend == "sqlite" else "%s"


def init_db(db):
    conn = connect(db)
    cur = conn.cursor()
    if db.backend == "sqlite":
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT,
                severity TEXT,
                owner TEXT,
                due_date TEXT,
                status TEXT NOT NULL,
                notes TEXT,
                source TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                closed_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute("PRAGMA table_info(signals)")
        columns = [row[1] for row in cur.fetchall()]
        if "updated_at" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN updated_at TEXT")
        cur.execute("UPDATE signals SET updated_at = created_at WHERE updated_at IS NULL")
    else:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db.table} (
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                category TEXT,
                severity TEXT,
                owner TEXT,
                due_date DATE,
                status TEXT NOT NULL,
                notes TEXT,
                source TEXT,
                tags TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                closed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(f"ALTER TABLE {db.table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")
        cur.execute(f"UPDATE {db.table} SET updated_at = created_at WHERE updated_at IS NULL")
    conn.commit()
    conn.close()


def add_signal(db, args):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    created_at = utc_now()
    values = (
        args.title,
        args.category,
        args.severity or DEFAULT_SEVERITY,
        args.owner,
        args.due,
        "open",
        args.notes,
        args.source,
        args.tags,
        created_at,
        created_at,
    )
    if db.backend == "sqlite":
        cur.execute(
            f"""
            INSERT INTO {db.table}
            (title, category, severity, owner, due_date, status, notes, source, tags, created_at, updated_at)
            VALUES ({", ".join([p] * 11)})
            """,
            values,
        )
        conn.commit()
        new_id = cur.lastrowid
    else:
        cur.execute(
            f"""
            INSERT INTO {db.table}
            (title, category, severity, owner, due_date, status, notes, source, tags, created_at, updated_at)
            VALUES ({", ".join([p] * 11)})
            RETURNING id
            """,
            values,
        )
        conn.commit()
        row = cur.fetchone()
        new_id = row["id"] if row else None
    conn.close()
    print(f"Added signal {new_id}.")


def seed_signals(db):
    conn = connect(db)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) AS total FROM {db.table}")
    row = cur.fetchone()
    existing = row[0] if not isinstance(row, dict) else row.get("total", 0)
    if existing and int(existing) > 0:
        conn.close()
        print("Seed skipped; signals already exist.")
        return

    p = placeholder(db)
    seed_rows = [
        {
            "title": "FAFSA verification delays impacting award timing",
            "category": "financial_aid",
            "severity": "high",
            "owner": "Janelle",
            "due_date": "2026-02-20",
            "status": "open",
            "notes": "Multiple scholars flagged for verification; track impact on March disbursements.",
            "source": "financial aid sync",
            "tags": "fa,disbursement",
            "created_at": utc_days_ago(10),
            "closed_at": None,
        },
        {
            "title": "Partner onboarding data feed missing graduation fields",
            "category": "partner",
            "severity": "medium",
            "owner": "Leah",
            "due_date": "2026-02-18",
            "status": "open",
            "notes": "Need schema update request before March intake imports.",
            "source": "partner ops",
            "tags": "onboarding,ops",
            "created_at": utc_days_ago(6),
            "closed_at": None,
        },
        {
            "title": "Scholar sentiment dip for virtual advising sessions",
            "category": "community",
            "severity": "medium",
            "owner": "Mina",
            "due_date": "2026-02-25",
            "status": "open",
            "notes": "Survey comments cite scheduling friction; recommend pilot with office hours.",
            "source": "pulse survey",
            "tags": "sentiment,advising",
            "created_at": utc_days_ago(4),
            "closed_at": None,
        },
        {
            "title": "Donor Q2 reporting needs updated outcomes highlights",
            "category": "fundraising",
            "severity": "high",
            "owner": "Rico",
            "due_date": "2026-02-22",
            "status": "open",
            "notes": "Prepare 3 new scholar vignettes and updated retention numbers.",
            "source": "fundraising sync",
            "tags": "donor,briefing",
            "created_at": utc_days_ago(8),
            "closed_at": None,
        },
        {
            "title": "STEM cohort mentoring slots overbooked",
            "category": "mentorship",
            "severity": "high",
            "owner": "Noah",
            "due_date": "2026-02-15",
            "status": "open",
            "notes": "Waitlist building; consider adding two volunteer mentors.",
            "source": "mentor map",
            "tags": "mentors,capacity",
            "created_at": utc_days_ago(12),
            "closed_at": None,
        },
        {
            "title": "Scholar outreach cadence missed for January cohort",
            "category": "operations",
            "severity": "critical",
            "owner": "Avery",
            "due_date": "2026-02-12",
            "status": "closed",
            "notes": "Backfilled touchpoints and updated cadence playbook.",
            "source": "ops retro",
            "tags": "cadence,retention",
            "created_at": utc_days_ago(20),
            "closed_at": utc_days_ago(5),
        },
        {
            "title": "Scholarship award acceptance rate trending down",
            "category": "awards",
            "severity": "medium",
            "owner": "Priya",
            "due_date": "2026-02-28",
            "status": "open",
            "notes": "Analyze yield vs offer package; coordinate with comms.",
            "source": "award allocator",
            "tags": "yield,analysis",
            "created_at": utc_days_ago(7),
            "closed_at": None,
        },
        {
            "title": "Partner renewal risk flagged for Horizon Foundation",
            "category": "partner",
            "severity": "high",
            "owner": "Diego",
            "due_date": "2026-02-19",
            "status": "open",
            "notes": "Need updated impact snapshot ahead of renewal call.",
            "source": "renewal tracker",
            "tags": "renewal,impact",
            "created_at": utc_days_ago(9),
            "closed_at": None,
        },
    ]

    cur.executemany(
        f"""
        INSERT INTO {db.table}
        (title, category, severity, owner, due_date, status, notes, source, tags, created_at, closed_at, updated_at)
        VALUES ({", ".join([p] * 12)})
        """,
        [
            (
                row["title"],
                row["category"],
                row["severity"],
                row["owner"],
                row["due_date"],
                row["status"],
                row["notes"],
                row["source"],
                row["tags"],
                row["created_at"],
                row["closed_at"],
                row["created_at"],
            )
            for row in seed_rows
        ],
    )
    conn.commit()
    conn.close()
    print(f"Seeded {len(seed_rows)} signals.")


def build_filters(args, param):
    filters = []
    values = []

    if args.status:
        filters.append(f"status = {param}")
        values.append(args.status)
    if args.category:
        filters.append(f"category = {param}")
        values.append(args.category)
    if args.owner:
        filters.append(f"owner = {param}")
        values.append(args.owner)
    if args.severity:
        filters.append(f"severity = {param}")
        values.append(args.severity)
    if args.search:
        filters.append(f"(title LIKE {param} OR notes LIKE {param} OR source LIKE {param})")
        pattern = f"%{args.search}%"
        values.extend([pattern, pattern, pattern])

    clause = ""
    if filters:
        clause = "WHERE " + " AND ".join(filters)
    return clause, values


def list_signals(db, args):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    clause, values = build_filters(args, p)
    limit_clause = ""
    if args.limit:
        limit_clause = f"LIMIT {p}"
        values.append(args.limit)

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, tags, created_at
        FROM {db.table}
        {clause}
        ORDER BY created_at DESC
        {limit_clause}
        """,
        values,
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No signals found.")
        return

    columns = [
        ("ID", "id"),
        ("Title", "title"),
        ("Category", "category"),
        ("Severity", "severity"),
        ("Owner", "owner"),
        ("Due", "due_date"),
        ("Status", "status"),
        ("Tags", "tags"),
    ]

    widths = []
    for label, key in columns:
        width = len(label)
        for row in rows:
            value = "" if row[key] is None else str(row[key])
            width = max(width, min(len(value), 40))
        widths.append(width)

    header = " | ".join(label.ljust(widths[i]) for i, (label, _) in enumerate(columns))
    divider = "-+-".join("-" * widths[i] for i in range(len(columns)))
    print(header)
    print(divider)

    for row in rows:
        line = []
        for i, (_, key) in enumerate(columns):
            value = "" if row[key] is None else str(row[key])
            if len(value) > 40:
                value = value[:37] + "..."
            line.append(value.ljust(widths[i]))
        print(" | ".join(line))


def close_signal(db, signal_id, note):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    cur.execute(f"SELECT id FROM {db.table} WHERE id = {p}", (signal_id,))
    if cur.fetchone() is None:
        conn.close()
        print(f"Signal {signal_id} not found.")
        return

    cur.execute(
        f"""
        UPDATE {db.table}
        SET status = {p}, closed_at = {p}, updated_at = {p}, notes = COALESCE(notes, '') || {p}
        WHERE id = {p}
        """,
        (
            "closed",
            utc_now(),
            utc_now(),
            f"\n[Closed] {note}" if note else "",
            signal_id,
        ),
    )
    conn.commit()
    conn.close()
    print(f"Closed signal {signal_id}.")


def reopen_signal(db, signal_id, note):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    cur.execute(f"SELECT id FROM {db.table} WHERE id = {p}", (signal_id,))
    if cur.fetchone() is None:
        conn.close()
        print(f"Signal {signal_id} not found.")
        return

    cur.execute(
        f"""
        UPDATE {db.table}
        SET status = {p}, closed_at = NULL, updated_at = {p}, notes = COALESCE(notes, '') || {p}
        WHERE id = {p}
        """,
        (
            "open",
            utc_now(),
            f"\n[Reopened] {note}" if note else "",
            signal_id,
        ),
    )
    conn.commit()
    conn.close()
    print(f"Reopened signal {signal_id}.")


def update_signal(db, args):
    if args.owner and args.clear_owner:
        print("Choose either --owner or --clear-owner, not both.")
        return
    if args.category and args.clear_category:
        print("Choose either --category or --clear-category, not both.")
        return
    if args.due and args.clear_due:
        print("Choose either --due or --clear-due, not both.")
        return
    if args.source and args.clear_source:
        print("Choose either --source or --clear-source, not both.")
        return
    if args.tags and args.clear_tags:
        print("Choose either --tags or --clear-tags, not both.")
        return
    if args.notes and args.clear_notes:
        print("Choose either --notes or --clear-notes, not both.")
        return

    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    cur.execute(f"SELECT id FROM {db.table} WHERE id = {p}", (args.signal_id,))
    if cur.fetchone() is None:
        conn.close()
        print(f"Signal {args.signal_id} not found.")
        return

    updates = []
    values = []

    def set_value(field, value):
        updates.append(f"{field} = {p}")
        values.append(value)

    if args.title is not None:
        set_value("title", args.title)
    if args.category is not None:
        set_value("category", args.category)
    if args.clear_category:
        updates.append("category = NULL")
    if args.severity is not None:
        set_value("severity", args.severity)
    if args.owner is not None:
        set_value("owner", args.owner)
    if args.clear_owner:
        updates.append("owner = NULL")
    if args.due is not None:
        set_value("due_date", args.due)
    if args.clear_due:
        updates.append("due_date = NULL")
    if args.source is not None:
        set_value("source", args.source)
    if args.clear_source:
        updates.append("source = NULL")
    if args.tags is not None:
        set_value("tags", args.tags)
    if args.clear_tags:
        updates.append("tags = NULL")

    if args.status is not None:
        set_value("status", args.status)
        if args.status == "closed":
            set_value("closed_at", utc_now())
        else:
            updates.append("closed_at = NULL")

    if args.clear_notes:
        updates.append("notes = NULL")

    if args.notes is not None:
        note_value = args.notes
        if args.append_note:
            note_value = f"{note_value}\n[Update {date.today().isoformat()}] {args.append_note}"
        set_value("notes", note_value)
    elif args.append_note:
        updates.append(f"notes = COALESCE(notes, '') || {p}")
        values.append(f"\n[Update {date.today().isoformat()}] {args.append_note}")

    if not updates:
        conn.close()
        print("No updates provided.")
        return

    updates.append(f"updated_at = {p}")
    values.append(utc_now())

    cur.execute(
        f"""
        UPDATE {db.table}
        SET {", ".join(updates)}
        WHERE id = {p}
        """,
        (*values, args.signal_id),
    )
    conn.commit()
    conn.close()
    print(f"Updated signal {args.signal_id}.")


def summary(db):
    conn = connect(db)
    cur = conn.cursor()

    def print_section(label, query):
        cur.execute(query)
        rows = cur.fetchall()
        print(f"\n{label}")
        print("-" * len(label))
        if not rows:
            print("(none)")
            return
        for row in rows:
            print(f"{row[0] or 'Unspecified'}: {row[1]}")

    print_section(
        "By status",
        f"SELECT status, COUNT(*) FROM {db.table} GROUP BY status ORDER BY COUNT(*) DESC",
    )
    print_section(
        "By category",
        f"SELECT category, COUNT(*) FROM {db.table} GROUP BY category ORDER BY COUNT(*) DESC",
    )
    print_section(
        "By severity",
        f"SELECT severity, COUNT(*) FROM {db.table} GROUP BY severity ORDER BY COUNT(*) DESC",
    )
    print_section(
        "By owner",
        f"SELECT owner, COUNT(*) FROM {db.table} GROUP BY owner ORDER BY COUNT(*) DESC",
    )

    conn.close()


def export_csv(db, args):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
    clause, values = build_filters(args, p)

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, notes, source, tags, created_at, closed_at, updated_at
        FROM {db.table}
        {clause}
        ORDER BY created_at DESC
        """,
        values,
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No signals found to export.")
        return

    headers = rows[0].keys()
    if args.out:
        out_path = args.out
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([row[h] for h in headers])
        print(f"Exported {len(rows)} signals to {out_path}.")
        return

    writer = csv.writer(os.sys.stdout)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row[h] for h in headers])


def digest(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow().date()

    cur.execute(
        """
        SELECT id, title, category, severity, owner, due_date, status, notes, source, tags, created_at
        FROM {table}
        ORDER BY created_at DESC
        """.format(table=db.table)
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No signals found.")
        return

    open_rows = [row for row in rows if row["status"] == "open"]
    closed_rows = [row for row in rows if row["status"] == "closed"]

    overdue = []
    due_soon = []
    for row in open_rows:
        due = parse_date(row["due_date"])
        if due is None:
            continue
        if due < today:
            overdue.append(row)
        elif (due - today).days <= args.days:
            due_soon.append(row)

    recent_cutoff = today.toordinal() - args.days
    recent = []
    for row in rows:
        created = row["created_at"]
        created_date = parse_date(created)
        if created_date and created_date.toordinal() >= recent_cutoff:
            recent.append(row)

    def section_title(label):
        return f"## {label}"

    def line_for(row):
        due = row["due_date"] or "No due date"
        owner = row["owner"] or "Unassigned"
        category = row["category"] or "Unspecified"
        return f"- [{row['id']}] {row['title']} ({category}, {row['severity'] or 'medium'}) — {owner} — due {due}"

    digest_lines = []
    digest_lines.append("# Signal Digest")
    digest_lines.append("")
    digest_lines.append("## Snapshot")
    digest_lines.append(f"- Total signals: {len(rows)}")
    digest_lines.append(f"- Open: {len(open_rows)}")
    digest_lines.append(f"- Closed: {len(closed_rows)}")
    digest_lines.append(f"- Overdue (open): {len(overdue)}")
    digest_lines.append(f"- Due soon (next {args.days} days): {len(due_soon)}")

    digest_lines.append("")
    digest_lines.append(section_title("Overdue Signals"))
    if overdue:
        for row in overdue[: args.limit]:
            digest_lines.append(line_for(row))
    else:
        digest_lines.append("- None")

    digest_lines.append("")
    digest_lines.append(section_title("Due Soon"))
    if due_soon:
        for row in due_soon[: args.limit]:
            digest_lines.append(line_for(row))
    else:
        digest_lines.append("- None")

    digest_lines.append("")
    digest_lines.append(section_title(f"Recent Signals (last {args.days} days)"))
    if recent:
        for row in recent[: args.limit]:
            digest_lines.append(line_for(row))
    else:
        digest_lines.append("- None")

    output = "\n".join(digest_lines)
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(output + "\n")
        print(f"Wrote digest to {args.out}.")
        return

    print(output)


def triage(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow().date()

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, notes, source, tags, created_at
        FROM {db.table}
        WHERE status = 'open'
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No open signals found.")
        return

    severity_weights = {"low": 1, "medium": 2, "high": 3, "critical": 4}
    scored = []

    for row in rows:
        due = parse_date(row["due_date"])
        created_date = parse_date(row["created_at"])
        age_days = (today - created_date).days if created_date else 0

        score = severity_weights.get(row["severity"] or DEFAULT_SEVERITY, 2) * 10
        reasons = []

        if due:
            if due < today:
                score += 15
                reasons.append("overdue")
            elif (due - today).days <= args.days:
                score += 8
                reasons.append("due soon")
        else:
            score += 2
            reasons.append("no due date")

        if not row["owner"]:
            score += 3
            reasons.append("unassigned")

        if age_days >= 14:
            score += min(12, age_days // 7)
            reasons.append("aging")

        scored.append(
            {
                "id": row["id"],
                "title": row["title"],
                "severity": row["severity"] or DEFAULT_SEVERITY,
                "owner": row["owner"] or "Unassigned",
                "due": row["due_date"] or "No due date",
                "age_days": age_days,
                "score": score,
                "reason": ", ".join(reasons) if reasons else "recent",
            }
        )

    scored.sort(key=lambda item: (item["score"], item["age_days"]), reverse=True)

    overdue_count = sum(1 for item in scored if "overdue" in item["reason"])
    due_soon_count = sum(1 for item in scored if "due soon" in item["reason"])
    unassigned_count = sum(1 for item in scored if "unassigned" in item["reason"])
    no_due_count = sum(1 for item in scored if "no due date" in item["reason"])

    print("Triage Snapshot")
    print("----------------")
    print(f"Open signals: {len(scored)}")
    print(f"Overdue: {overdue_count}")
    print(f"Due soon (next {args.days} days): {due_soon_count}")
    print(f"Unassigned: {unassigned_count}")
    print(f"No due date: {no_due_count}")

    columns = [
        ("ID", "id"),
        ("Title", "title"),
        ("Severity", "severity"),
        ("Owner", "owner"),
        ("Due", "due"),
        ("Age(d)", "age_days"),
        ("Score", "score"),
        ("Reason", "reason"),
    ]

    widths = []
    for label, key in columns:
        width = len(label)
        for row in scored[: args.limit]:
            value = "" if row[key] is None else str(row[key])
            width = max(width, min(len(value), 40))
        widths.append(width)

    header = " | ".join(label.ljust(widths[i]) for i, (label, _) in enumerate(columns))
    divider = "-+-".join("-" * widths[i] for i in range(len(columns)))
    print("")
    print(header)
    print(divider)

    for row in scored[: args.limit]:
        line = []
        for i, (_, key) in enumerate(columns):
            value = "" if row[key] is None else str(row[key])
            if len(value) > 40:
                value = value[:37] + "..."
            line.append(value.ljust(widths[i]))
        print(" | ".join(line))


def workload(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow().date()

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, created_at
        FROM {db.table}
        WHERE status = 'open'
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No open signals found.")
        return

    buckets = {}
    for row in rows:
        owner = row["owner"] or "Unassigned"
        due = parse_date(row["due_date"])
        created = parse_date(row["created_at"])
        age_days = (today - created).days if created else 0

        bucket = buckets.setdefault(
            owner,
            {
                "open": 0,
                "overdue": 0,
                "due_soon": 0,
                "due_later": 0,
                "no_due": 0,
                "age_total": 0,
                "age_count": 0,
                "high_critical": 0,
            },
        )

        bucket["open"] += 1
        bucket["age_total"] += age_days
        bucket["age_count"] += 1

        if (row["severity"] or DEFAULT_SEVERITY) in {"high", "critical"}:
            bucket["high_critical"] += 1

        if due is None:
            bucket["no_due"] += 1
        elif due < today:
            bucket["overdue"] += 1
        elif (due - today).days <= args.days:
            bucket["due_soon"] += 1
        else:
            bucket["due_later"] += 1

    rows_out = []
    for owner, stats in buckets.items():
        avg_age = round(stats["age_total"] / stats["age_count"], 1) if stats["age_count"] else 0
        rows_out.append(
            {
                "owner": owner,
                "open": stats["open"],
                "overdue": stats["overdue"],
                "due_soon": stats["due_soon"],
                "due_later": stats["due_later"],
                "no_due": stats["no_due"],
                "avg_age": avg_age,
                "high_critical": stats["high_critical"],
            }
        )

    rows_out.sort(key=lambda item: (item["overdue"], item["open"], item["high_critical"]), reverse=True)

    columns = [
        ("Owner", "owner"),
        ("Open", "open"),
        ("Overdue", "overdue"),
        ("Due soon", "due_soon"),
        ("Due later", "due_later"),
        ("No due", "no_due"),
        ("Avg age", "avg_age"),
        ("High/Crit", "high_critical"),
    ]

    def build_table_lines():
        widths = []
        for label, key in columns:
            width = len(label)
            for row in rows_out:
                value = "" if row[key] is None else str(row[key])
                width = max(width, min(len(value), 40))
            widths.append(width)

        header = " | ".join(label.ljust(widths[i]) for i, (label, _) in enumerate(columns))
        divider = "-+-".join("-" * widths[i] for i in range(len(columns)))
        lines = [
            "Workload Snapshot",
            "-----------------",
            f"Open signals: {len(rows)}",
            f"Due soon window: {args.days} days",
            "",
            header,
            divider,
        ]
        for row in rows_out:
            line = []
            for i, (_, key) in enumerate(columns):
                value = "" if row[key] is None else str(row[key])
                if len(value) > 40:
                    value = value[:37] + "..."
                line.append(value.ljust(widths[i]))
            lines.append(" | ".join(line))
        return lines

    def build_markdown_lines():
        header_cells = [label for label, _ in columns]
        lines = [
            "# Signal Workload",
            "",
            f"- Open signals: {len(rows)}",
            f"- Due soon window: {args.days} days",
            "",
            "| " + " | ".join(header_cells) + " |",
            "| " + " | ".join(["---"] * len(header_cells)) + " |",
        ]
        for row in rows_out:
            lines.append(
                "| "
                + " | ".join(str(row[key]) if row[key] is not None else "" for _, key in columns)
                + " |"
            )
        return lines

    if args.format == "markdown":
        output = "\n".join(build_markdown_lines()) + "\n"
    else:
        output = "\n".join(build_table_lines()) + "\n"

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(output)
        print(f"Wrote workload to {args.out}.")
        return

    print(output, end="")


def calendar(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow().date()
    horizon = today + timedelta(days=args.days)

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, created_at
        FROM {db.table}
        WHERE status = 'open'
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No open signals found.")
        return

    overdue = []
    due_today = []
    no_due = []
    beyond = []
    upcoming_by_week = {}

    for row in rows:
        due = parse_date(row["due_date"])
        if due is None:
            no_due.append(row)
            continue
        if due < today:
            overdue.append(row)
            continue
        if due == today:
            due_today.append(row)
            continue
        if due <= horizon:
            week_start = due - timedelta(days=due.weekday())
            upcoming_by_week.setdefault(week_start, []).append(row)
        else:
            beyond.append(row)

    def normalize_row(row):
        due = parse_date(row["due_date"])
        return {
            "due": due.isoformat() if due else "No due date",
            "id": row["id"],
            "title": row["title"],
            "owner": row["owner"] or "Unassigned",
            "severity": row["severity"] or DEFAULT_SEVERITY,
            "category": row["category"] or "Unspecified",
        }

    columns = [
        ("Due", "due"),
        ("ID", "id"),
        ("Title", "title"),
        ("Owner", "owner"),
        ("Severity", "severity"),
        ("Category", "category"),
    ]

    def build_table_section(label, section_rows):
        if not section_rows:
            return [label, "-" * len(label), "(none)"]

        normalized = [normalize_row(row) for row in section_rows[: args.limit]]
        widths = []
        for col_label, key in columns:
            width = len(col_label)
            for item in normalized:
                value = "" if item[key] is None else str(item[key])
                width = max(width, min(len(value), 40))
            widths.append(width)

        header = " | ".join(col_label.ljust(widths[i]) for i, (col_label, _) in enumerate(columns))
        divider = "-+-".join("-" * widths[i] for i in range(len(columns)))
        lines = [label, "-" * len(label), header, divider]
        for item in normalized:
            row_line = []
            for i, (_, key) in enumerate(columns):
                value = "" if item[key] is None else str(item[key])
                if len(value) > 40:
                    value = value[:37] + "..."
                row_line.append(value.ljust(widths[i]))
            lines.append(" | ".join(row_line))
        return lines

    def build_markdown_section(label, section_rows):
        lines = [f"## {label}", ""]
        if not section_rows:
            lines.append("- (none)")
            lines.append("")
            return lines
        for row in section_rows[: args.limit]:
            item = normalize_row(row)
            lines.append(
                f"- [{item['id']}] {item['title']} — {item['owner']} — due {item['due']} — "
                f"{item['severity']} — {item['category']}"
            )
        lines.append("")
        return lines

    sections = []
    if overdue:
        sections.append(("Overdue Signals", sorted(overdue, key=lambda r: parse_date(r["due_date"]) or today)))
    if due_today:
        sections.append(("Due Today", sorted(due_today, key=lambda r: r["id"])))

    for week_start in sorted(upcoming_by_week.keys()):
        label = f"Week of {week_start.isoformat()}"
        week_rows = sorted(upcoming_by_week[week_start], key=lambda r: parse_date(r["due_date"]) or horizon)
        sections.append((label, week_rows))

    if beyond:
        sections.append((f"Beyond {horizon.isoformat()}", sorted(beyond, key=lambda r: parse_date(r["due_date"]) or horizon)))
    if no_due:
        sections.append(("No Due Date", sorted(no_due, key=lambda r: r["id"])))

    if args.format == "markdown":
        lines = [
            "# Signal Calendar",
            "",
            f"- Open signals: {len(rows)}",
            f"- Horizon: {args.days} days (through {horizon.isoformat()})",
            "",
        ]
        for label, section_rows in sections:
            lines.extend(build_markdown_section(label, section_rows))
        output = "\n".join(lines).rstrip() + "\n"
    else:
        lines = [
            "Signal Calendar Snapshot",
            "------------------------",
            f"Open signals: {len(rows)}",
            f"Horizon: {args.days} days (through {horizon.isoformat()})",
            "",
        ]
        for label, section_rows in sections:
            lines.extend(build_table_section(label, section_rows))
            lines.append("")
        output = "\n".join(lines).rstrip() + "\n"

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(output)
        print(f"Wrote calendar to {args.out}.")
        return

    print(output, end="")


def audit(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow().date()

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, notes, source, tags, created_at
        FROM {db.table}
        WHERE status = 'open'
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No open signals found.")
        return

    missing_owner = []
    missing_due = []
    missing_category = []
    missing_severity = []
    missing_tags = []
    missing_source = []
    aging = []
    overdue = []

    for row in rows:
        due = parse_date(row["due_date"])
        created_date = parse_date(row["created_at"])
        age_days = (today - created_date).days if created_date else 0

        if not row["owner"]:
            missing_owner.append(row)
        if not row["due_date"]:
            missing_due.append(row)
        if not row["category"]:
            missing_category.append(row)
        if not row["severity"]:
            missing_severity.append(row)
        if not row["tags"]:
            missing_tags.append(row)
        if not row["source"]:
            missing_source.append(row)
        if age_days >= args.stale_days:
            aging.append((row, age_days))
        if due and due < today:
            overdue.append(row)

    def line_for(row, extra=""):
        owner = row["owner"] or "Unassigned"
        due = row["due_date"] or "No due date"
        category = row["category"] or "Unspecified"
        suffix = f" — {extra}" if extra else ""
        return f"- [{row['id']}] {row['title']} ({category}) — {owner} — due {due}{suffix}"

    print("Audit Snapshot")
    print("---------------")
    print(f"Open signals: {len(rows)}")
    print(f"Missing owner: {len(missing_owner)}")
    print(f"Missing due date: {len(missing_due)}")
    print(f"Missing category: {len(missing_category)}")
    print(f"Missing severity: {len(missing_severity)}")
    print(f"Missing tags: {len(missing_tags)}")
    print(f"Missing source: {len(missing_source)}")
    print(f"Aging (>= {args.stale_days} days): {len(aging)}")
    print(f"Overdue: {len(overdue)}")

    sections = [
        ("Missing Owner", missing_owner, ""),
        ("Missing Due Date", missing_due, ""),
        ("Missing Category", missing_category, ""),
        ("Missing Severity", missing_severity, ""),
        ("Missing Tags", missing_tags, ""),
        ("Missing Source", missing_source, ""),
        (f"Aging Open Signals (>= {args.stale_days} days)", [item[0] for item in aging], "age"),
        ("Overdue Open Signals", overdue, ""),
    ]

    for label, items, extra in sections:
        print(f"\n{label}")
        print("-" * len(label))
        if not items:
            print("(none)")
            continue
        for item in items[: args.limit]:
            if extra == "age":
                age_days = next(
                    (age for row, age in aging if row["id"] == item["id"]),
                    None,
                )
                age_text = f"{age_days} days old" if age_days is not None else ""
                print(line_for(item, age_text))
            else:
                print(line_for(item))


def metrics(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow()

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, created_at, closed_at, updated_at
        FROM {db.table}
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No signals found.")
        return

    open_rows = [row for row in rows if row["status"] == "open"]
    closed_rows = [row for row in rows if row["status"] == "closed"]

    open_ages = []
    open_overdue = 0
    open_due_soon = 0
    open_unassigned = 0
    open_by_severity = {"low": 0, "medium": 0, "high": 0, "critical": 0, "unspecified": 0}
    open_stale = 0

    for row in open_rows:
        created_dt = parse_datetime(row["created_at"])
        if created_dt:
            open_ages.append((today - created_dt).days)
        due = parse_date(row["due_date"])
        if due and due < today.date():
            open_overdue += 1
        elif due and (due - today.date()).days <= args.due_days:
            open_due_soon += 1
        if not row["owner"]:
            open_unassigned += 1
        severity = row["severity"] or "unspecified"
        open_by_severity[severity] = open_by_severity.get(severity, 0) + 1
        updated_dt = parse_datetime(row["updated_at"]) or created_dt
        if updated_dt and (today - updated_dt).days >= args.stale_days:
            open_stale += 1

    def avg(values):
        return round(sum(values) / len(values), 1) if values else 0

    def median(values):
        return round(statistics.median(values), 1) if values else 0

    closed_cycle = []
    for row in closed_rows:
        created_dt = parse_datetime(row["created_at"])
        closed_dt = parse_datetime(row["closed_at"])
        if created_dt and closed_dt:
            closed_cycle.append((closed_dt - created_dt).days)

    oldest_open = []
    for row in open_rows:
        created_dt = parse_datetime(row["created_at"])
        if not created_dt:
            continue
        age_days = (today - created_dt).days
        oldest_open.append((age_days, row))
    oldest_open.sort(key=lambda item: item[0], reverse=True)

    print("Signal Metrics")
    print("---------------")
    print(f"Total signals: {len(rows)}")
    print(f"Open signals: {len(open_rows)}")
    print(f"Closed signals: {len(closed_rows)}")
    print(f"Open overdue: {open_overdue}")
    print(f"Open due soon (next {args.due_days} days): {open_due_soon}")
    print(f"Open unassigned: {open_unassigned}")
    print(f"Open stale (>= {args.stale_days} days since update): {open_stale}")
    print(f"Avg open age (days): {avg(open_ages)}")
    print(f"Median open age (days): {median(open_ages)}")
    print(f"Avg close cycle (days): {avg(closed_cycle)}")
    print(f"Median close cycle (days): {median(closed_cycle)}")

    print("\nOpen signals by severity")
    print("------------------------")
    for severity in ("critical", "high", "medium", "low", "unspecified"):
        print(f"{severity.title()}: {open_by_severity.get(severity, 0)}")

    print("\nOldest open signals")
    print("-------------------")
    if not oldest_open:
        print("(none)")
        return
    for age_days, row in oldest_open[: args.limit]:
        owner = row["owner"] or "Unassigned"
        due = row["due_date"] or "No due date"
        category = row["category"] or "Unspecified"
        print(f"- [{row['id']}] {row['title']} ({category}) — {owner} — due {due} — {age_days} days open")


def stale(db, args):
    conn = connect(db)
    cur = conn.cursor()
    today = datetime.utcnow()

    cur.execute(
        f"""
        SELECT id, title, category, severity, owner, due_date, status, created_at, updated_at
        FROM {db.table}
        WHERE status = 'open'
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No open signals found.")
        return

    stale_rows = []
    for row in rows:
        updated_dt = parse_datetime(row["updated_at"]) or parse_datetime(row["created_at"])
        if not updated_dt:
            continue
        age_days = (today - updated_dt).days
        if age_days >= args.days:
            stale_rows.append(
                {
                    "id": row["id"],
                    "title": row["title"],
                    "severity": row["severity"] or DEFAULT_SEVERITY,
                    "owner": row["owner"] or "Unassigned",
                    "due": row["due_date"] or "No due date",
                    "last_update": updated_dt.date().isoformat(),
                    "age_days": age_days,
                }
            )

    stale_rows.sort(key=lambda item: item["age_days"], reverse=True)

    print("Stale Signal Snapshot")
    print("----------------------")
    print(f"Open signals: {len(rows)}")
    print(f"Stale (>= {args.days} days since update): {len(stale_rows)}")

    if not stale_rows:
        print("\nNo stale signals found.")
        return

    columns = [
        ("ID", "id"),
        ("Title", "title"),
        ("Severity", "severity"),
        ("Owner", "owner"),
        ("Due", "due"),
        ("Last update", "last_update"),
        ("Age(d)", "age_days"),
    ]

    widths = []
    for label, key in columns:
        width = len(label)
        for row in stale_rows[: args.limit]:
            value = "" if row[key] is None else str(row[key])
            width = max(width, min(len(value), 40))
        widths.append(width)

    header = " | ".join(label.ljust(widths[i]) for i, (label, _) in enumerate(columns))
    divider = "-+-".join("-" * widths[i] for i in range(len(columns)))
    print("")
    print(header)
    print(divider)

    for row in stale_rows[: args.limit]:
        line = []
        for i, (_, key) in enumerate(columns):
            value = "" if row[key] is None else str(row[key])
            if len(value) > 40:
                value = value[:37] + "..."
            line.append(value.ljust(widths[i]))
        print(" | ".join(line))


def build_parser():
    parser = argparse.ArgumentParser(description="Group Scholar Signal Catalog")
    parser.add_argument("--db", default=default_db_path(), help="Path to the SQLite database")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "postgres"],
        help="Storage backend (defaults to sqlite unless SIGNAL_CATALOG_BACKEND is set)",
    )
    parser.add_argument(
        "--database-url",
        help="Postgres connection string (or set SIGNAL_CATALOG_DATABASE_URL)",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init", help="Initialize the database")
    subparsers.add_parser("seed", help="Insert sample signals if none exist")

    add_parser = subparsers.add_parser("add", help="Add a signal")
    add_parser.add_argument("--title", required=True)
    add_parser.add_argument("--category")
    add_parser.add_argument("--severity", choices=["low", "medium", "high", "critical"])
    add_parser.add_argument("--owner")
    add_parser.add_argument("--due", help="Due date (YYYY-MM-DD)")
    add_parser.add_argument("--notes")
    add_parser.add_argument("--source")
    add_parser.add_argument("--tags", help="Comma-separated tags")

    list_parser = subparsers.add_parser("list", help="List signals")
    list_parser.add_argument("--status", default="open")
    list_parser.add_argument("--category")
    list_parser.add_argument("--owner")
    list_parser.add_argument("--severity", choices=["low", "medium", "high", "critical"])
    list_parser.add_argument("--search")
    list_parser.add_argument("--limit", type=int)

    close_parser = subparsers.add_parser("close", help="Close a signal")
    close_parser.add_argument("signal_id", type=int)
    close_parser.add_argument("--note")

    reopen_parser = subparsers.add_parser("reopen", help="Reopen a signal")
    reopen_parser.add_argument("signal_id", type=int)
    reopen_parser.add_argument("--note")

    update_parser = subparsers.add_parser("update", help="Update fields on a signal")
    update_parser.add_argument("signal_id", type=int)
    update_parser.add_argument("--title")
    update_parser.add_argument("--category")
    update_parser.add_argument("--severity", choices=["low", "medium", "high", "critical"])
    update_parser.add_argument("--owner")
    update_parser.add_argument("--due", help="Due date (YYYY-MM-DD)")
    update_parser.add_argument("--status", choices=["open", "closed"])
    update_parser.add_argument("--notes")
    update_parser.add_argument("--append-note")
    update_parser.add_argument("--source")
    update_parser.add_argument("--tags")
    update_parser.add_argument("--clear-owner", action="store_true")
    update_parser.add_argument("--clear-category", action="store_true")
    update_parser.add_argument("--clear-due", action="store_true")
    update_parser.add_argument("--clear-source", action="store_true")
    update_parser.add_argument("--clear-tags", action="store_true")
    update_parser.add_argument("--clear-notes", action="store_true")

    subparsers.add_parser("summary", help="Show summary rollups")

    export_parser = subparsers.add_parser("export", help="Export signals to CSV")
    export_parser.add_argument("--status")
    export_parser.add_argument("--category")
    export_parser.add_argument("--owner")
    export_parser.add_argument("--severity", choices=["low", "medium", "high", "critical"])
    export_parser.add_argument("--search")
    export_parser.add_argument("--out", help="Output CSV file path")

    digest_parser = subparsers.add_parser("digest", help="Generate a markdown digest")
    digest_parser.add_argument("--days", type=int, default=DEFAULT_DIGEST_DAYS)
    digest_parser.add_argument("--limit", type=int, default=DEFAULT_DIGEST_LIMIT)
    digest_parser.add_argument("--out", help="Output markdown file path")

    triage_parser = subparsers.add_parser("triage", help="Rank open signals by urgency")
    triage_parser.add_argument("--days", type=int, default=DEFAULT_TRIAGE_DAYS)
    triage_parser.add_argument("--limit", type=int, default=DEFAULT_TRIAGE_LIMIT)

    workload_parser = subparsers.add_parser("workload", help="Summarize open-signal workload by owner")
    workload_parser.add_argument("--days", type=int, default=DEFAULT_WORKLOAD_DAYS)
    workload_parser.add_argument("--format", choices=["table", "markdown"], default="table")
    workload_parser.add_argument("--out", help="Output file path for workload report")

    calendar_parser = subparsers.add_parser("calendar", help="Show upcoming due dates by week")
    calendar_parser.add_argument("--days", type=int, default=DEFAULT_CALENDAR_DAYS)
    calendar_parser.add_argument("--limit", type=int, default=DEFAULT_CALENDAR_LIMIT)
    calendar_parser.add_argument("--format", choices=["table", "markdown"], default="table")
    calendar_parser.add_argument("--out", help="Output file path for calendar report")

    audit_parser = subparsers.add_parser("audit", help="Audit open signals for missing fields")
    audit_parser.add_argument("--limit", type=int, default=DEFAULT_AUDIT_LIMIT)
    audit_parser.add_argument("--stale-days", type=int, default=DEFAULT_AUDIT_STALE_DAYS)

    metrics_parser = subparsers.add_parser("metrics", help="Show operational metrics for signals")
    metrics_parser.add_argument("--due-days", type=int, default=DEFAULT_METRICS_DUE_DAYS)
    metrics_parser.add_argument("--limit", type=int, default=8)
    metrics_parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS)

    stale_parser = subparsers.add_parser("stale", help="List open signals without recent updates")
    stale_parser.add_argument("--days", type=int, default=DEFAULT_STALE_DAYS)
    stale_parser.add_argument("--limit", type=int, default=8)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    db = resolve_db_config(args)

    if args.command == "init":
        init_db(db)
        location = db.db_path if db.backend == "sqlite" else db.table
        print(f"Initialized database at {location}.")
        return

    if args.command is None:
        parser.print_help()
        return

    init_db(db)

    if args.command == "add":
        add_signal(db, args)
    elif args.command == "seed":
        seed_signals(db)
    elif args.command == "list":
        list_signals(db, args)
    elif args.command == "close":
        close_signal(db, args.signal_id, args.note)
    elif args.command == "reopen":
        reopen_signal(db, args.signal_id, args.note)
    elif args.command == "update":
        update_signal(db, args)
    elif args.command == "summary":
        summary(db)
    elif args.command == "export":
        export_csv(db, args)
    elif args.command == "digest":
        digest(db, args)
    elif args.command == "triage":
        triage(db, args)
    elif args.command == "workload":
        workload(db, args)
    elif args.command == "calendar":
        calendar(db, args)
    elif args.command == "audit":
        audit(db, args)
    elif args.command == "metrics":
        metrics(db, args)
    elif args.command == "stale":
        stale(db, args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
