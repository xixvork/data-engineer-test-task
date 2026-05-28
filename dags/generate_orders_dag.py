import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pendulum
from airflow.decorators import dag, task
from common.constants import INSERT_PAGE_SIZE, ROWS_PER_BATCH, SUPPORTED_CURRENCIES
from common.db import get_source_connection
from common.schemas import create_orders_table
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def generate_random_order(batch_id, now):
    random_seconds = random.randint(0, 7 * 24 * 60 * 60)
    order_date = now - timedelta(seconds=random_seconds)

    amount_cents = random.randint(100, 1_000_000)
    amount = Decimal(amount_cents) / Decimal("100")

    customer_suffix = uuid.uuid4().hex[:12]
    customer_email = f"customer_{customer_suffix}@example.com"

    return (
        str(uuid.uuid4()),
        customer_email,
        order_date,
        amount,
        random.choice(SUPPORTED_CURRENCIES),
        str(batch_id),
    )


def generate_orders_batch():
    batch_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    rows = [
        generate_random_order(batch_id=batch_id, now=now)
        for _ in range(ROWS_PER_BATCH)
    ]

    if len(rows) != ROWS_PER_BATCH:
        raise ValueError(f"Expected {ROWS_PER_BATCH} rows, got {len(rows)}")

    min_order_date = min(row[2] for row in rows)
    max_order_date = max(row[2] for row in rows)
    used_currencies = sorted({row[4] for row in rows})

    logger.info(
        "Generated orders batch: batch_id=%s rows=%s min_order_date=%s "
        "max_order_date=%s currencies=%s",
        batch_id,
        len(rows),
        min_order_date,
        max_order_date,
        ",".join(used_currencies),
    )

    return batch_id, rows


def insert_orders(connection, batch_id, rows):
    insert_sql = """
        INSERT INTO orders (
            order_id,
            customer_email,
            order_date,
            amount,
            currency,
            batch_id
        )
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING;
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_sql, rows, page_size=INSERT_PAGE_SIZE)

        cursor.execute(
            "SELECT COUNT(*) FROM orders WHERE batch_id = %s;",
            (str(batch_id),),
        )
        inserted_rows = cursor.fetchone()[0]

    return inserted_rows


@dag(
    dag_id="generate_orders_dag",
    description="Generate 5000 random orders into postgres-1 every 10 minutes.",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["orders", "generator", "postgres"],
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
)
def generate_orders_dag():
    @task
    def generate_and_insert_orders():
        started_at = datetime.now(timezone.utc)

        logger.info(
            "Starting order generation: rows_per_batch=%s target=postgres-1.orders",
            ROWS_PER_BATCH,
        )

        batch_id, rows = generate_orders_batch()

        connection = get_source_connection()

        try:
            create_orders_table(connection)
            inserted_rows = insert_orders(connection, batch_id, rows)
            if inserted_rows != ROWS_PER_BATCH:
                raise ValueError(
                    f"Expected to insert {ROWS_PER_BATCH} rows, inserted {inserted_rows}"
                )
            connection.commit()
        except Exception:
            connection.rollback()
            logger.exception("Order generation failed: batch_id=%s", batch_id)
            raise
        finally:
            connection.close()

        duration_seconds = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds()

        logger.info(
            "Finished order generation: batch_id=%s expected_rows=%s "
            "inserted_rows=%s duration_seconds=%.2f",
            batch_id,
            ROWS_PER_BATCH,
            inserted_rows,
            duration_seconds,
        )

    generate_and_insert_orders()


generate_orders_dag()
