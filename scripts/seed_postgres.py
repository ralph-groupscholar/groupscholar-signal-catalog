#!/usr/bin/env python3
import os
import sys
from datetime import date

try:
    import psycopg
except ImportError:
    print("psycopg is required. Install with: pip install 'psycopg[binary]'", file=sys.stderr)
    sys.exit(1)

TABLE = "gsc_signals"

SEED_SIGNALS = [
    {
        "title": "FAFSA verification backlog spike",
        "category": "operations",
        "severity": "high",
        "owner": "Ariana",
        "due_date": "2026-03-05",
        "status": "open",
        "notes": "Verification queue doubled week-over-week; need staffing check.",
        "source": "ops dashboard",
        "tags": "fafsa,backlog",
    },
    {
        "title": "Partner onboarding doc refresh needed",
        "category": "partner",
        "severity": "medium",
        "owner": "Leah",
        "due_date": "2026-03-12",
        "status": "open",
        "notes": "New compliance section missing from latest deck.",
        "source": "partner call",
        "tags": "onboarding,docs",
    },
    {
        "title": "Scholar retention dip in cohort 7",
        "category": "scholars",
        "severity": "critical",
        "owner": "Mateo",
        "due_date": "2026-02-25",
        "status": "open",
        "notes": "Drop-off at week 5; schedule listening sessions.",
        "source": "retention report",
        "tags": "retention,cohort-7",
    },
    {
        "title": "Grant reporting deadline approaching",
        "category": "funding",
        "severity": "high",
        "owner": "Priya",
        "due_date": "2026-02-18",
        "status": "open",
        "notes": "Need outcome stats + beneficiary stories.",
        "source": "funding calendar",
        "tags": "grant,reporting",
    },
    {
        "title": "Mentor match satisfaction trend positive",
        "category": "program",
        "severity": "low",
        "owner": "Jules",
        "due_date": None,
        "status": "open",
        "notes": "NPS up 12 points after new matching rubric.",
        "source": "survey insights",
        "tags": "mentors,nps",
    },
    {
        "title": "Data sharing agreement needs legal review",
        "category": "compliance",
        "severity": "high",
        "owner": "Rita",
        "due_date": "2026-03-20",
        "status": "open",
        "notes": "Draft from partner includes new data fields.",
        "source": "legal inbox",
        "tags": "compliance,legal",
    },
    {
        "title": "Alumni spotlight series filming",
        "category": "marketing",
        "severity": "medium",
        "owner": "Noah",
        "due_date": "2026-02-28",
        "status": "open",
        "notes": "Finalize interview schedule with 3 alumni.",
        "source": "content calendar",
        "tags": "alumni,storytelling",
    },
    {
        "title": "Scholar support tickets cleared",
        "category": "support",
        "severity": "low",
        "owner": "Kai",
        "due_date": None,
        "status": "closed",
        "notes": "Queue back to baseline after weekend push.",
        "source": "support queue",
        "tags": "support,ops",
        "closed_at": "2026-02-06T19:45:00Z",
    },
    {
        "title": "Employer partnership pipeline warming",
        "category": "partnerships",
        "severity": "medium",
        "owner": "Sasha",
        "due_date": "2026-03-01",
        "status": "open",
        "notes": "Two employers requested cohort impact stats.",
        "source": "pipeline review",
        "tags": "employers,pipeline",
    },
    {
        "title": "Budget variance flagged for Q1",
        "category": "finance",
        "severity": "high",
        "owner": "Iris",
        "due_date": "2026-02-22",
        "status": "open",
        "notes": "Travel costs trending 18% above plan.",
        "source": "finance report",
        "tags": "budget,variance",
    },
]


def main():
    dsn = os.getenv("SIGNAL_CATALOG_DATABASE_URL")
    if not dsn:
        print("SIGNAL_CATALOG_DATABASE_URL is required to seed Postgres.", file=sys.stderr)
        sys.exit(1)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
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
            cur.execute(f"TRUNCATE TABLE {TABLE} RESTART IDENTITY")

            for signal in SEED_SIGNALS:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE}
                    (title, category, severity, owner, due_date, status, notes, source, tags, created_at, closed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        signal["title"],
                        signal["category"],
                        signal["severity"],
                        signal["owner"],
                        signal["due_date"],
                        signal["status"],
                        signal["notes"],
                        signal["source"],
                        signal["tags"],
                        signal.get("created_at", date.today().isoformat()),
                        signal.get("closed_at"),
                    ),
                )

        conn.commit()

    print(f"Seeded {len(SEED_SIGNALS)} signals into {TABLE}.")


if __name__ == "__main__":
    main()
