import logging
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.decorators import dag, task
from common.constants import INSERT_PAGE_SIZE, SYNC_CHUNK_SIZE, SYNC_STATE_NAME
from common.db import get_source_connection, get_target_connection
from common.exchange_rates import convert_amount_to_eur, fetch_latest_rates
from common.schemas import (
    create_orders_eur_table,
    create_orders_table,
    create_sync_state_table,
)
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_sync_state(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT last_created_at, last_order_id
            FROM sync_state
            WHERE name = %s;
            """,
            (SYNC_STATE_NAME,),
        )
        row = cursor.fetchone()

    if row is None:
        return None, None

    return row


def update_sync_state(connection, last_created_at, last_order_id):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sync_state (
                name,
                last_created_at,
                last_order_id
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                last_created_at = EXCLUDED.last_created_at,
                last_order_id = EXCLUDED.last_order_id,
                updated_at = now();
            """,
            (SYNC_STATE_NAME, last_created_at, last_order_id),
        )


def fetch_source_orders_chunk(connection, last_created_at, last_order_id, limit):
    if last_created_at is None and last_order_id is None:
        query = """
            SELECT
                order_id,
                customer_email,
                order_date,
                amount,
                currency,
                created_at
            FROM orders
            ORDER BY created_at ASC, order_id ASC
            LIMIT %s;
        """
        params = (limit,)
    else:
        query = """
            SELECT
                order_id,
                customer_email,
                order_date,
                amount,
                currency,
                created_at
            FROM orders
            WHERE (created_at, order_id) > (%s, %s)
            ORDER BY created_at ASC, order_id ASC
            LIMIT %s;
        """
        params = (last_created_at, last_order_id, limit)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


def build_converted_rows(source_orders, rates):
    converted_rows = []

    for order in source_orders:
        (
            order_id,
            customer_email,
            order_date,
            original_amount,
            original_currency,
            source_created_at,
        ) = order

        amount_eur, conversion_rate_to_eur = convert_amount_to_eur(
            amount=original_amount,
            currency=original_currency,
            rates=rates,
        )

        converted_rows.append(
            (
                order_id,
                customer_email,
                order_date,
                original_amount,
                original_currency,
                amount_eur,
                conversion_rate_to_eur,
                source_created_at,
            )
        )

    return converted_rows


def insert_converted_orders(connection, converted_rows):
    if not converted_rows:
        return 0

    insert_sql = """
        INSERT INTO orders_eur (
            order_id,
            customer_email,
            order_date,
            original_amount,
            original_currency,
            amount_eur,
            conversion_rate_to_eur,
            source_created_at
        )
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id;
    """

    with connection.cursor() as cursor:
        inserted_order_ids = execute_values(
            cursor,
            insert_sql,
            converted_rows,
            page_size=INSERT_PAGE_SIZE,
            fetch=True,
        )

    return len(inserted_order_ids)


@dag(
    dag_id="sync_orders_to_eur_dag",
    description="Sync orders from postgres-1 to postgres-2 and convert amounts to EUR.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["orders", "sync", "eur", "postgres"],
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
)
def sync_orders_to_eur_dag():
    @task
    def sync_orders_to_eur():
        started_at = datetime.now(timezone.utc)

        logger.info(
            "Starting orders sync: source=postgres-1.orders target=postgres-2.orders_eur"
        )

        source_connection = get_source_connection()
        target_connection = get_target_connection()

        try:
            create_orders_table(source_connection)
            source_connection.commit()

            create_orders_eur_table(target_connection)
            create_sync_state_table(target_connection)
            target_connection.commit()

            last_created_at, last_order_id = get_sync_state(target_connection)

            logger.info(
                "Current sync state: last_created_at=%s last_order_id=%s",
                last_created_at,
                last_order_id,
            )

            total_fetched_rows = 0
            total_converted_rows = 0
            total_inserted_rows = 0
            total_skipped_duplicates = 0
            chunk_count = 0
            rates_payload = None
            rates = None

            while True:
                source_orders = fetch_source_orders_chunk(
                    source_connection,
                    last_created_at,
                    last_order_id,
                    SYNC_CHUNK_SIZE,
                )

                if not source_orders:
                    break

                if rates is None:
                    rates_payload = fetch_latest_rates()
                    rates = rates_payload["rates"]

                    logger.info(
                        "Exchange rates loaded: base=%s timestamp=%s "
                        "rates_count=%s eur_rate=%s",
                        rates_payload["base"],
                        rates_payload["timestamp"],
                        len(rates),
                        rates["EUR"],
                    )

                converted_rows = build_converted_rows(source_orders, rates)
                inserted_rows = insert_converted_orders(
                    target_connection,
                    converted_rows,
                )

                last_order = source_orders[-1]
                last_created_at = last_order[5]
                last_order_id = last_order[0]
                update_sync_state(target_connection, last_created_at, last_order_id)
                target_connection.commit()

                skipped_duplicates = len(converted_rows) - inserted_rows
                chunk_count += 1
                total_fetched_rows += len(source_orders)
                total_converted_rows += len(converted_rows)
                total_inserted_rows += inserted_rows
                total_skipped_duplicates += skipped_duplicates

                logger.info(
                    "Processed orders sync chunk: chunk=%s fetched_rows=%s "
                    "converted_rows=%s inserted_rows=%s skipped_duplicates=%s "
                    "last_created_at=%s last_order_id=%s",
                    chunk_count,
                    len(source_orders),
                    len(converted_rows),
                    inserted_rows,
                    skipped_duplicates,
                    last_created_at,
                    last_order_id,
                )

            duration_seconds = (
                datetime.now(timezone.utc) - started_at
            ).total_seconds()

            logger.info(
                "Finished orders sync: chunks=%s fetched_rows=%s converted_rows=%s "
                "inserted_rows=%s skipped_duplicates=%s duration_seconds=%.2f",
                chunk_count,
                total_fetched_rows,
                total_converted_rows,
                total_inserted_rows,
                total_skipped_duplicates,
                duration_seconds,
            )

        except Exception:
            target_connection.rollback()
            source_connection.rollback()
            logger.exception("Orders sync failed.")
            raise
        finally:
            source_connection.close()
            target_connection.close()

    sync_orders_to_eur()


sync_orders_to_eur_dag()
