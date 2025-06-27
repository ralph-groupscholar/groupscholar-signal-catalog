#!/usr/bin/env python3
import argparse
import csv
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime

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
POSTGRES_TABLE = "gsc_signals"


@dataclass
class DBConfig:
    backend: str
    db_path: Optional[str]
    dsn: Optional[str]
    table: str


def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


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
        dsn = os.getenv("SIGNAL_CATALOG_DATABASE_URL")
        if not dsn:
            print("SIGNAL_CATALOG_DATABASE_URL is required for the postgres backend.")
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
                closed_at TEXT
            )
            """
        )
    else:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db.table} (
                id SERIAL PRIMARY KEY,
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
                closed_at TEXT
            )
            """
        )
    conn.commit()
    conn.close()


def add_signal(db, args):
    conn = connect(db)
    cur = conn.cursor()
    p = placeholder(db)
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
        utc_now(),
    )
    if db.backend == "sqlite":
        cur.execute(
            f"""
            INSERT INTO {db.table}
            (title, category, severity, owner, due_date, status, notes, source, tags, created_at)
            VALUES ({", ".join([p] * 10)})
            """,
            values,
        )
        conn.commit()
        new_id = cur.lastrowid
    else:
        cur.execute(
            f"""
            INSERT INTO {db.table}
            (title, category, severity, owner, due_date, status, notes, source, tags, created_at)
            VALUES ({", ".join([p] * 10)})
            RETURNING id
            """,
            values,
        )
        conn.commit()
        row = cur.fetchone()
        new_id = row["id"] if row else None
    conn.close()
    print(f"Added signal {new_id}.")


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
        SET status = {p}, closed_at = {p}, notes = COALESCE(notes, '') || {p}
        WHERE id = {p}
        """,
        (
            "closed",
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
        SET status = {p}, closed_at = NULL, notes = COALESCE(notes, '') || {p}
        WHERE id = {p}
        """,
        (
            "open",
            f"\n[Reopened] {note}" if note else "",
            signal_id,
        ),
    )
    conn.commit()
    conn.close()
    print(f"Reopened signal {signal_id}.")


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
        SELECT id, title, category, severity, owner, due_date, status, notes, source, tags, created_at, closed_at
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
        created_date = parse_date(created[:10]) if created else None
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
        created_date = parse_date(row["created_at"][:10]) if row["created_at"] else None
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


def build_parser():
    parser = argparse.ArgumentParser(description="Group Scholar Signal Catalog")
    parser.add_argument("--db", default=default_db_path(), help="Path to the SQLite database")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "postgres"],
        help="Storage backend (defaults to sqlite unless SIGNAL_CATALOG_BACKEND is set)",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init", help="Initialize the database")

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
    elif args.command == "list":
        list_signals(db, args)
    elif args.command == "close":
        close_signal(db, args.signal_id, args.note)
    elif args.command == "reopen":
        reopen_signal(db, args.signal_id, args.note)
    elif args.command == "summary":
        summary(db)
    elif args.command == "export":
        export_csv(db, args)
    elif args.command == "digest":
        digest(db, args)
    elif args.command == "triage":
        triage(db, args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
