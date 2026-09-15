"""Seed and exercise the Maple & Co. OLTP database — plain local Python.

Run from the repo root on your own machine (no Lambda, no packaging):

    pip install psycopg2-binary
    python src/ingestion/seed_oltp_local.py seed     --host <rds-endpoint>
    python src/ingestion/seed_oltp_local.py counts   --host <rds-endpoint>
    python src/ingestion/seed_oltp_local.py activity --host <rds-endpoint> --orders 50 --updates 100 --deletes 10

The password is asked for interactively (paste it from Secrets Manager), or
set the MAPLE_DB_PASSWORD environment variable to skip the prompt.
Never put the password in the command line or in this file.

  seed      drops/creates schema `maple` and bulk-COPYs the Olist CSVs
            from ./data/olist/ (fast: Postgres COPY protocol, ~1-2 min)
  counts    prints row counts per table (verification)
  activity  simulates live traffic — INSERTs new orders, UPDATEs order
            statuses, DELETEs a few voucher payments — so DMS CDC has real
            changes (I/U/D) to replicate
"""

from __future__ import annotations

import argparse
import getpass
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[0]
DATA_DIR = REPO_ROOT / "data" / "olist"

# table name -> (csv file, DDL). geolocation (1M rows) is intentionally
# skipped — it adds load time and nothing to the pipeline's teaching value.
TABLES: dict[str, tuple[str, str]] = {
    "customers": (
        "olist_customers_dataset.csv",
        """CREATE TABLE maple.customers (
            customer_id              text PRIMARY KEY,
            customer_unique_id       text NOT NULL,
            customer_zip_code_prefix text,
            customer_city            text,
            customer_state           text)""",
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        """CREATE TABLE maple.sellers (
            seller_id              text PRIMARY KEY,
            seller_zip_code_prefix text,
            seller_city            text,
            seller_state           text)""",
    ),
    "products": (
        "olist_products_dataset.csv",
        """CREATE TABLE maple.products (
            product_id                 text PRIMARY KEY,
            product_category_name      text,
            product_name_lenght        int,
            product_description_lenght int,
            product_photos_qty         int,
            product_weight_g           int,
            product_length_cm          int,
            product_height_cm          int,
            product_width_cm           int)""",
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        """CREATE TABLE maple.category_translation (
            product_category_name         text PRIMARY KEY,
            product_category_name_english text)""",
    ),
    "orders": (
        "olist_orders_dataset.csv",
        """CREATE TABLE maple.orders (
            order_id                      text PRIMARY KEY,
            customer_id                   text NOT NULL,
            order_status                  text NOT NULL,
            order_purchase_timestamp      timestamp,
            order_approved_at             timestamp,
            order_delivered_carrier_date  timestamp,
            order_delivered_customer_date timestamp,
            order_estimated_delivery_date timestamp)""",
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        """CREATE TABLE maple.order_items (
            order_id            text,
            order_item_id       int,
            product_id          text,
            seller_id           text,
            shipping_limit_date timestamp,
            price               numeric(10,2),
            freight_value       numeric(10,2),
            PRIMARY KEY (order_id, order_item_id))""",
    ),
    "order_payments": (
        "olist_order_payments_dataset.csv",
        """CREATE TABLE maple.order_payments (
            order_id             text,
            payment_sequential   int,
            payment_type         text,
            payment_installments int,
            payment_value        numeric(10,2),
            PRIMARY KEY (order_id, payment_sequential))""",
    ),
    "order_reviews": (
        "olist_order_reviews_dataset.csv",
        """CREATE TABLE maple.order_reviews (
            review_id               text,
            order_id                text,
            review_score            int,
            review_comment_title    text,
            review_comment_message  text,
            review_creation_date    timestamp,
            review_answer_timestamp timestamp,
            PRIMARY KEY (review_id, order_id))""",
    ),
}

ORDER_STATUS_FLOW = ["created", "approved", "processing", "shipped", "delivered"]


def connect(host: str, user: str, dbname: str):
    password = os.environ.get("MAPLE_DB_PASSWORD") or getpass.getpass(
        f"Password for {user}@{host} (from Secrets Manager): "
    )
    return psycopg2.connect(
        host=host, dbname=dbname, user=user, password=password,
        connect_timeout=10, sslmode="require",
    )


def cmd_seed(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS maple CASCADE")
        cur.execute("CREATE SCHEMA maple")
        for table, (csv_file, ddl) in TABLES.items():
            path = DATA_DIR / csv_file
            if not path.exists():
                raise SystemExit(f"Missing {path} — download the Olist dataset first (day-1 guide).")
            cur.execute(ddl)
            with open(path, encoding="utf-8") as f:
                # COPY handles quoted multiline fields (reviews) and is ~50x
                # faster than INSERTs for a 100k-order bulk load.
                cur.copy_expert(
                    f"COPY maple.{table} FROM STDIN WITH (FORMAT csv, HEADER true)", f
                )
            cur.execute(f"SELECT count(*) FROM maple.{table}")
            print(f"  {table:<22} {cur.fetchone()[0]:>10,} rows")
    conn.commit()
    print("Seed complete.")


def cmd_counts(conn) -> None:
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(f"SELECT count(*) FROM maple.{table}")
            print(f"  {table:<22} {cur.fetchone()[0]:>10,} rows")


def cmd_activity(conn, n_orders: int, n_updates: int, n_deletes: int) -> None:
    now = datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM maple.customers ORDER BY random() LIMIT %s", (n_orders,))
        customers = [r[0] for r in cur.fetchall()]
        cur.execute(
            "SELECT product_id, seller_id FROM maple.order_items ORDER BY random() LIMIT %s",
            (n_orders,),
        )
        prods = cur.fetchall()

        inserted = 0
        for i in range(min(n_orders, len(customers), len(prods))):
            oid = uuid.uuid4().hex
            cur.execute(
                "INSERT INTO maple.orders VALUES (%s, %s, 'created', %s, NULL, NULL, NULL, %s)",
                (oid, customers[i], now, now + timedelta(days=7)),
            )
            cur.execute(
                "INSERT INTO maple.order_items VALUES (%s, 1, %s, %s, %s, %s, %s)",
                (oid, prods[i][0], prods[i][1], now + timedelta(days=3),
                 round(random.uniform(10, 400), 2), round(random.uniform(5, 40), 2)),
            )
            cur.execute(
                "INSERT INTO maple.order_payments VALUES (%s, 1, %s, 1, %s)",
                (oid, random.choice(["credit_card", "debit_card", "voucher"]),
                 round(random.uniform(15, 440), 2)),
            )
            inserted += 1

        cur.execute(
            """SELECT order_id, order_status FROM maple.orders
               WHERE order_status <> 'delivered' ORDER BY random() LIMIT %s""",
            (n_updates,),
        )
        updated = 0
        for oid, status in cur.fetchall():
            nxt = ORDER_STATUS_FLOW[min(ORDER_STATUS_FLOW.index(status) + 1, 4)] \
                if status in ORDER_STATUS_FLOW else "approved"
            cur.execute(
                "UPDATE maple.orders SET order_status=%s, order_approved_at=COALESCE(order_approved_at,%s) WHERE order_id=%s",
                (nxt, now, oid),
            )
            updated += 1

        cur.execute(
            """DELETE FROM maple.order_payments
               WHERE (order_id, payment_sequential) IN (
                 SELECT order_id, payment_sequential FROM maple.order_payments
                 WHERE payment_type='voucher' ORDER BY random() LIMIT %s)""",
            (n_deletes,),
        )
        deleted = cur.rowcount
    conn.commit()
    print(f"Inserted {inserted} orders (+items+payments), updated {updated} "
          f"order statuses, deleted {deleted} voucher payments.")
    print("DMS CDC will pick these up within ~60s — watch s3://<raw>/dms/maple/orders/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["seed", "counts", "activity"])
    parser.add_argument("--host", required=True, help="RDS endpoint, e.g. maple-oltp.xxxx.ca-central-1.rds.amazonaws.com")
    parser.add_argument("--user", default="maple_admin")
    parser.add_argument("--dbname", default="maple")
    parser.add_argument("--orders", type=int, default=50)
    parser.add_argument("--updates", type=int, default=100)
    parser.add_argument("--deletes", type=int, default=10)
    args = parser.parse_args()

    with connect(args.host, args.user, args.dbname) as conn:
        if args.command == "seed":
            cmd_seed(conn)
        elif args.command == "counts":
            cmd_counts(conn)
        else:
            cmd_activity(conn, args.orders, args.updates, args.deletes)


if __name__ == "__main__":
    main()
