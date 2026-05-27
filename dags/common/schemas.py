def create_orders_table(connection):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            customer_email TEXT NOT NULL,
            order_date TIMESTAMPTZ NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            currency TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            batch_id UUID NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_orders_created_at_order_id
            ON orders(created_at, order_id);

        CREATE INDEX IF NOT EXISTS idx_orders_batch_id
            ON orders(batch_id);
    """

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)


def create_orders_eur_table(connection):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS orders_eur (
            order_id UUID PRIMARY KEY,
            customer_email TEXT NOT NULL,
            order_date TIMESTAMPTZ NOT NULL,
            original_amount NUMERIC(12, 2) NOT NULL,
            original_currency TEXT NOT NULL,
            amount_eur NUMERIC(12, 2) NOT NULL,
            exchange_rate_to_eur NUMERIC(18, 8) NOT NULL,
            source_created_at TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_orders_eur_source_created_at
            ON orders_eur(source_created_at);
    """

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)


def create_sync_state_table(connection):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS sync_state (
            name TEXT PRIMARY KEY,
            last_created_at TIMESTAMPTZ,
            last_order_id UUID,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)
