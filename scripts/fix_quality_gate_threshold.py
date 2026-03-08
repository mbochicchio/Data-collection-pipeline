"""
One-off script to update the quality_gates table:
changes passed = (score > 5) to passed = (score >= 5).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from common.db import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        # Show current distribution before update
        cur.execute("SELECT score, passed, COUNT(*) FROM quality_gates GROUP BY score, passed ORDER BY score")
        rows = cur.fetchall()
        print("Before update:")
        print(f"  {'score':>6}  {'passed':>6}  {'count':>6}")
        print("  " + "-" * 24)
        for score, passed, count in rows:
            print(f"  {score:>6}  {str(passed):>6}  {count:>6}")

        # Update: score >= 5 now passes
        cur.execute("""
            UPDATE quality_gates
            SET passed = (score >= 5)
            WHERE passed != (score >= 5)
        """)
        updated = cur.rowcount
        print(f"\nUpdated {updated} row(s).")

        # Show distribution after update
        cur.execute("SELECT score, passed, COUNT(*) FROM quality_gates GROUP BY score, passed ORDER BY score")
        rows = cur.fetchall()
        print("\nAfter update:")
        print(f"  {'score':>6}  {'passed':>6}  {'count':>6}")
        print("  " + "-" * 24)
        for score, passed, count in rows:
            print(f"  {score:>6}  {str(passed):>6}  {count:>6}")

        cur.execute("SELECT COUNT(*) FILTER (WHERE passed) FROM quality_gates")
        total_passed = cur.fetchone()[0]
        print(f"\nTotal passed: {total_passed}")
