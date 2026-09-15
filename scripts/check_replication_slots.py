"""Check (and optionally drop) Postgres replication slots on the OLTP RDS.

Why this exists: with rds.logical_replication=1, any replication slot DMS
leaves behind pins WAL files forever -> the instance fills its disk and goes
Storage-full. Run this at every teardown, after deleting the DMS task.

    python scripts/check_replication_slots.py --host <rds-endpoint>
    python scripts/check_replication_slots.py --host <rds-endpoint> --drop <slot_name>

Password: MAPLE_DB_PASSWORD env var, or interactive prompt.

Rule of thumb printed by the script:
  active=true               -> a DMS task is consuming it; leave it alone.
  active=false + task gone  -> orphaned; drop it or your disk fills again.
"""

from __future__ import annotations

import argparse
import getpass
import os

import psycopg2


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", required=True)
    p.add_argument("--user", default="maple_admin")
    p.add_argument("--dbname", default="maple")
    p.add_argument("--drop", metavar="SLOT_NAME", help="drop this (inactive) slot")
    args = p.parse_args()

    password = os.environ.get("MAPLE_DB_PASSWORD") or getpass.getpass(
        f"Password for {args.user}@{args.host}: "
    )
    conn = psycopg2.connect(
        host=args.host, dbname=args.dbname, user=args.user, password=password,
        connect_timeout=10, sslmode="require",
    )
    conn.autocommit = True

    with conn, conn.cursor() as cur:
        if args.drop:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (args.drop,))
            print(f"Dropped slot '{args.drop}'. WAL will be freed within minutes.")

        cur.execute("""
            SELECT slot_name, active,
                   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
            FROM pg_replication_slots
        """)
        rows = cur.fetchall()

        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
        db_size = cur.fetchone()[0]
        print(f"\nDatabase size: {db_size}")

        if not rows:
            print("No replication slots. Disk cannot be pinned by WAL - safe state.")
            return

        print(f"\n{'slot_name':<40} {'active':<8} retained WAL")
        print("-" * 65)
        for name, active, retained in rows:
            print(f"{name:<40} {str(active):<8} {retained}")

        inactive = [r[0] for r in rows if not r[1]]
        if inactive:
            print(
                "\nWARNING: inactive slot(s) pin WAL and WILL fill the disk if the"
                "\nowning DMS task is gone. Drop each orphan with:"
            )
            for name in inactive:
                print(f"  python scripts/check_replication_slots.py --host {args.host} --drop {name}")
        else:
            print("\nAll slots active (a running DMS task is consuming them). OK.")


if __name__ == "__main__":
    main()
