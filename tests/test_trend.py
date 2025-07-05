import io
import tempfile
import unittest
from contextlib import redirect_stdout

import app


class TrendReportTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.temp_dir.name}/signals.db"
        self.db = app.DBConfig(backend="sqlite", db_path=self.db_path, dsn=None, table="signals")
        app.init_db(self.db)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _insert_signal(self, created_at, closed_at=None):
        conn = app.connect(self.db)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signals
            (title, category, severity, owner, due_date, status, notes, source, tags, created_at, closed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Test signal",
                "ops",
                "high",
                "Leah",
                None,
                "closed" if closed_at else "open",
                "",
                "",
                "",
                created_at,
                closed_at,
                created_at,
            ),
        )
        conn.commit()
        conn.close()

    def test_trend_report_counts(self):
        self._insert_signal("2026-02-02T10:00:00Z", "2026-02-05T12:00:00Z")
        self._insert_signal("2026-01-27T09:00:00Z", "2026-01-30T12:00:00Z")
        self._insert_signal("2026-01-20T09:00:00Z")

        args = type(
            "Args",
            (),
            {"weeks": 3, "as_of": "2026-02-08", "format": "table", "out": None},
        )

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            app.trend(self.db, args)

        output = buffer.getvalue()
        lines = output.splitlines()

        def find_row(week):
            for line in lines:
                if line.strip().startswith(week):
                    return [part.strip() for part in line.split("|")]
            return None

        row_current = find_row("2026-02-02")
        row_prev = find_row("2026-01-26")
        row_old = find_row("2026-01-19")

        self.assertIsNotNone(row_current)
        self.assertIsNotNone(row_prev)
        self.assertIsNotNone(row_old)

        self.assertEqual(row_current[1], "1")
        self.assertEqual(row_current[2], "1")
        self.assertEqual(row_current[3], "0")

        self.assertEqual(row_prev[1], "1")
        self.assertEqual(row_prev[2], "1")
        self.assertEqual(row_prev[3], "0")

        self.assertEqual(row_old[1], "1")
        self.assertEqual(row_old[2], "0")
        self.assertEqual(row_old[3], "1")


if __name__ == "__main__":
    unittest.main()
