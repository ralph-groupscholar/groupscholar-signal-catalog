import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app import DBConfig, init_db, connect, activity


class ActivityReportTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "signals.db")
        self.db = DBConfig(backend="sqlite", db_path=self.db_path, dsn=None, table="signals")
        init_db(self.db)

    def tearDown(self):
        self.temp_dir.cleanup()

    def seed(self, rows):
        conn = connect(self.db)
        cur = conn.cursor()
        for row in rows:
            cur.execute(
                """
                INSERT INTO signals (
                    title, category, severity, owner, due_date, status, notes, source, tags,
                    created_at, closed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        conn.commit()
        conn.close()

    def test_activity_snapshot_counts(self):
        now = datetime.utcnow()
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        rows = [
            (
                "Partner delay",
                "partner",
                "high",
                "Leah",
                (now + timedelta(days=2)).date().isoformat(),
                "open",
                "",
                "weekly call",
                "",
                (now - timedelta(days=1)).strftime(fmt),
                None,
                (now - timedelta(days=1)).strftime(fmt),
            ),
            (
                "Cycle closed",
                "ops",
                "medium",
                "Diego",
                None,
                "closed",
                "",
                "ops sync",
                "",
                (now - timedelta(days=10)).strftime(fmt),
                (now - timedelta(days=2)).strftime(fmt),
                (now - timedelta(days=2)).strftime(fmt),
            ),
            (
                "Overdue outreach",
                "scholar",
                "critical",
                "",
                (now - timedelta(days=5)).date().isoformat(),
                "open",
                "",
                "",
                "",
                (now - timedelta(days=20)).strftime(fmt),
                None,
                (now - timedelta(days=20)).strftime(fmt),
            ),
            (
                "Fresh update",
                "ops",
                "low",
                "Avery",
                None,
                "open",
                "",
                "",
                "",
                (now - timedelta(days=3)).strftime(fmt),
                None,
                (now - timedelta(days=3)).strftime(fmt),
            ),
        ]
        self.seed(rows)

        args = SimpleNamespace(days=7, limit=5, format="table", out=None)
        buf = io.StringIO()
        with redirect_stdout(buf):
            activity(self.db, args)
        output = buf.getvalue()

        self.assertIn("Signals created: 2", output)
        self.assertIn("Signals updated/closed: 3", output)
        self.assertIn("Signals closed: 1", output)
        self.assertIn("Open overdue: 1", output)
        self.assertIn("Open due soon (next 7 days): 1", output)


if __name__ == "__main__":
    unittest.main()
