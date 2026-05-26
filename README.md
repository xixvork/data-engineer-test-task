# Junior Data Engineer Test Task

Local Apache Airflow project that generates order data into one PostgreSQL database and syncs it into another PostgreSQL database with currency conversion to EUR.

## Stack

- Apache Airflow
- PostgreSQL
- Docker Compose
- Python
- OpenExchangeRates API

## Project Structure

```text
.
├── docker-compose.yaml
├── .env.example
├── README.md
└── dags/
    ├── generate_orders_dag.py
    ├── sync_orders_to_eur_dag.py
    └── common/
        ├── __init__.py
        └── exchange_rates.py
```

## DAGs

### `generate_orders_dag`

Runs every 10 minutes.

Generates 5000 new rows into:

```text
postgres-1.orders
```

Generated columns:

```text
order_id
customer_email
order_date
amount
currency
created_at
batch_id
```

### `sync_orders_to_eur_dag`

Runs every hour.

Reads orders from:

```text
postgres-1.orders
```

Converts amounts to EUR using OpenExchangeRates and writes results into:

```text
postgres-2.orders_eur
```

Target columns:

```text
order_id
customer_email
order_date
original_amount
original_currency
amount_eur
exchange_rate_to_eur
source_created_at
processed_at
```

The sync is idempotent: repeated runs do not duplicate rows because `order_id` is used as the primary key.

## Requirements

- Docker Desktop
- OpenExchangeRates API key

## Setup

Copy the example environment file:

```bash
cp .env.example .env
```

Open `.env` and set your OpenExchangeRates app id:

```env
OPENEXCHANGE_APP_ID=your_openexchange_app_id_here
```

Do not commit `.env` to Git.

## Run

Start all services:

```bash
docker compose up --build
```

Or run in detached mode:

```bash
docker compose up -d --build
```

Airflow UI:

```text
http://localhost:8080
```

Default credentials:

```text
username: airflow
password: airflow
```

## Services

```text
airflow-webserver   http://localhost:8080
airflow-scheduler
postgres-1          localhost:5433
postgres-2          localhost:5434
postgres-airflow    internal Airflow metadata database
```

## Trigger DAGs Manually

Trigger order generation:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger generate_orders_dag
```

Trigger sync to EUR:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger sync_orders_to_eur_dag
```

You can also trigger both DAGs from the Airflow UI.

## Check Source Database

Connect to `postgres-1`:

```bash
docker exec -it postgres-1 psql -U postgres -d orders_source
```

Useful SQL:

```sql
SELECT COUNT(*) FROM orders;

SELECT batch_id, COUNT(*)
FROM orders
GROUP BY batch_id
ORDER BY COUNT(*) DESC
LIMIT 10;

SELECT *
FROM orders
LIMIT 10;
```

One-line check:

```bash
docker exec -it postgres-1 psql -U postgres -d orders_source -c "SELECT COUNT(*) FROM orders;"
```

## Check Target Database

Connect to `postgres-2`:

```bash
docker exec -it postgres-2 psql -U postgres -d orders_target
```

Useful SQL:

```sql
SELECT COUNT(*) FROM orders_eur;

SELECT order_id, original_amount, original_currency, amount_eur, exchange_rate_to_eur
FROM orders_eur
LIMIT 10;
```

One-line check:

```bash
docker exec -it postgres-2 psql -U postgres -d orders_target -c "SELECT COUNT(*) FROM orders_eur;"
```

## Expected Flow

1. Start Docker Compose.
2. Open Airflow UI.
3. Trigger `generate_orders_dag`.
4. Check that `postgres-1.orders` contains 5000 rows.
5. Trigger `sync_orders_to_eur_dag`.
6. Check that `postgres-2.orders_eur` contains converted rows.
7. Trigger `sync_orders_to_eur_dag` again.
8. Check that row count in `orders_eur` does not double.

## Stop Services

Stop containers but keep data:

```bash
docker compose stop
```

Remove containers but keep volumes:

```bash
docker compose down
```

Remove containers and delete database volumes:

```bash
docker compose down -v
```

Use `down -v` only if you want to reset all local data.

## Notes

- `order_date` is generated within the last 7 days.
- `created_at` is used as the technical timestamp for sync logic.
- OpenExchangeRates is called once per sync run, not once per row.
- `orders_eur` keeps the original amount, original currency, converted EUR amount, and exchange rate for easier debugging.
- `.env` contains secrets and must not be committed.