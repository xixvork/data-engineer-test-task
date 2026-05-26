import logging
import os
from datetime import datetime, timedelta, timezone

import pendulum
import psycopg2
from psycopg2.extras import execute_values

from airflow.decorators import dag, task

from common.exchange_rates import fetch_latest_rates, convert_amount_to_eur


logger = logging.getLogger(__name__)


def get_postgres_1_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_1_HOST"],
        port=int(os.environ["POSTGRES_1_PORT"]),
        dbname=os.environ["POSTGRES_1_DB"],
        user=os.environ["POSTGRES_1_USER"],
        password=os.environ["POSTGRES_1_PASSWORD"],
    )


def get_postgres_2_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_2_HOST"],
        port=int(os.environ["POSTGRES_2_PORT"]),
        dbname=os.environ["POSTGRES_2_DB"],
        user=os.environ["POSTGRES_2_USER"],
        password=os.environ["POSTGRES_2_PASSWORD"],
    )


def create_orders_eur_table(connection):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS orders_eur (
            order_id UUID PRIMARY KEY,
            customer_email TEXT NOT NULL,
            order_date TIMESTAMP NOT NULL,

            original_amount NUMERIC(12, 2) NOT NULL,
            original_currency TEXT NOT NULL,

            amount_eur NUMERIC(12, 2) NOT NULL,
            exchange_rate_to_eur NUMERIC(18, 8) NOT NULL,

            source_created_at TIMESTAMP NOT NULL,
            processed_at TIMESTAMP NOT NULL DEFAULT now()
        );
    """

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)


def get_last_processed_source_created_at(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(source_created_at) FROM orders_eur;")
        return cursor.fetchone()[0]


def fetch_source_orders(connection, last_processed_source_created_at):
    if last_processed_source_created_at is None:
        query = """
            SELECT
                order_id,
                customer_email,
                order_date,
                amount,
                currency,
                created_at
            FROM orders
            ORDER BY created_at ASC;
        """
        params = ()
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
            WHERE created_at >= %s
            ORDER BY created_at ASC;
        """
        params = (last_processed_source_created_at,)

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

        amount_eur, exchange_rate_to_eur = convert_amount_to_eur(
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
                exchange_rate_to_eur,
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
            exchange_rate_to_eur,
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
            page_size=1000,
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

        source_connection = get_postgres_1_connection()
        target_connection = get_postgres_2_connection()

        try:
            create_orders_eur_table(target_connection)
            target_connection.commit()

            last_processed_source_created_at = get_last_processed_source_created_at(
                target_connection
            )

            logger.info(
                "Last processed source_created_at=%s",
                last_processed_source_created_at,
            )

            source_orders = fetch_source_orders(
                source_connection,
                last_processed_source_created_at,
            )

            logger.info("Fetched source orders: rows=%s", len(source_orders))

            if not source_orders:
                logger.info("No source orders to process.")
                return

            source_currencies = sorted({row[4] for row in source_orders})

            logger.info(
                "Fetching exchange rates for source currencies: currencies=%s",
                ",".join(source_currencies),
            )

            rates_payload = fetch_latest_rates(required_currencies=source_currencies)
            rates = rates_payload["rates"]

            logger.info(
                "Exchange rates loaded: base=%s timestamp=%s rates_count=%s eur_rate=%s",
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

            target_connection.commit()

            skipped_duplicates = len(converted_rows) - inserted_rows
            duration_seconds = (
                datetime.now(timezone.utc) - started_at
            ).total_seconds()

            logger.info(
                "Finished orders sync: fetched_rows=%s converted_rows=%s "
                "inserted_rows=%s skipped_duplicates=%s duration_seconds=%.2f",
                len(source_orders),
                len(converted_rows),
                inserted_rows,
                skipped_duplicates,
                duration_seconds,
            )

        except Exception:
            target_connection.rollback()
            logger.exception("Orders sync failed.")
            raise
        finally:
            source_connection.close()
            target_connection.close()

    sync_orders_to_eur()


sync_orders_to_eur_dag()