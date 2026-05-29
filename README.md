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
├── .github/
│   └── workflows/
│       └── ci.yml
├── dags/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── db.py
│   │   ├── exchange_rates.py
│   │   └── schemas.py
│   ├── generate_orders_dag.py
│   └── sync_orders_to_eur_dag.py
├── tests/
│   └── test_exchange_rates.py
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── pyproject.toml
├── README.md
├── requirements-dev.txt
└── requirements.txt
```

`dags/` contains Airflow DAG definitions and shared runtime modules.
`tests/` contains unit tests for reusable business logic.
`.github/workflows/ci.yml` runs automated checks on push and pull requests.
`requirements-dev.txt` and `pyproject.toml` define local development and CI checks.

## DAGs

### `generate_orders_dag`

Runs every 10 minutes. It creates the source table if needed and generates 5000 new rows into:

```text
postgres-1 / orders_source / orders
```

Generated rows use `TIMESTAMPTZ` timestamps and include `batch_id` for debugging each insert batch.

Source columns:

| Column | Type | Purpose |
| --- | --- | --- |
| `order_id` | `UUID` | Source order identifier |
| `customer_email` | `TEXT` | Generated customer email |
| `order_date` | `TIMESTAMPTZ` | Generated order timestamp within the rolling 7-day window |
| `amount` | `NUMERIC(12, 2)` | Original order amount |
| `currency` | `TEXT` | Original order currency |
| `created_at` | `TIMESTAMPTZ` | Technical insertion timestamp used by sync |
| `batch_id` | `UUID` | Debug identifier for one generated batch |

### `sync_orders_to_eur_dag`

Runs every hour. It creates target tables if needed, reads source orders incrementally in chunks, and stores cursor progress in:

```text
postgres-2 / orders_target / sync_state
```

It converts amounts to EUR using OpenExchangeRates once per non-empty sync run and writes results into:

```text
postgres-2 / orders_target / orders_eur
```

Target columns:

| Column | Type | Purpose |
| --- | --- | --- |
| `order_id` | `UUID` | Source order identifier and target primary key |
| `customer_email` | `TEXT` | Customer email copied from source |
| `order_date` | `TIMESTAMPTZ` | Original order timestamp |
| `original_amount` | `NUMERIC(12, 2)` | Amount before conversion |
| `original_currency` | `TEXT` | Currency before conversion |
| `amount_eur` | `NUMERIC(12, 2)` | Converted amount in EUR |
| `conversion_rate_to_eur` | `NUMERIC(18, 8)` | Multiplier used for conversion to EUR |
| `source_created_at` | `TIMESTAMPTZ` | Source technical insertion timestamp |
| `processed_at` | `TIMESTAMPTZ` | Target insertion timestamp |

The sync is idempotent: repeated runs do not duplicate rows because `orders_eur.order_id` is the primary key and inserts use `ON CONFLICT DO NOTHING`.

## Design Notes

- `sync_state` stores the last processed `(created_at, order_id)` cursor so the hourly sync does not need to rescan the whole source table or rely on `MAX(source_created_at)`.
- `orders_eur` keeps original amount, original currency, converted EUR amount, and the conversion multiplier so converted values are auditable.
- `conversion_rate_to_eur` stores the multiplier used to convert one unit of the original currency into EUR.
- `TIMESTAMPTZ` is used for timestamps so generated and processed times keep timezone meaning across Airflow, PostgreSQL, and local machines.
- `Dockerfile` and `requirements.txt` install pinned Python dependencies at image build time instead of using `_PIP_ADDITIONAL_REQUIREMENTS` during container startup.

## Design Decisions and Trade-offs

### Why NUMERIC(12, 2) instead of float

The task describes `amount` as a float with 2 decimal places. The implementation stores money as `NUMERIC(12, 2)` in PostgreSQL and uses Python `Decimal` during conversion. This avoids binary floating-point rounding issues for money while still satisfying the requirement that generated amounts have 2 decimal places.

### Why `sync_state` instead of `MAX(source_created_at)`

Using `MAX(source_created_at)` can repeatedly re-read rows when many rows share the same maximum timestamp. That can happen naturally because one generated batch may contain many rows with the same `created_at`. The `sync_state` table stores the exact last processed cursor, which keeps the sync incremental and bounded.

### Why the cursor is `(created_at, order_id)`

`created_at` alone is not unique. Adding `order_id` makes the cursor deterministic when multiple rows have the same timestamp. The source index on `(created_at, order_id)` supports this access pattern.

### Why rates are fetched once per non-empty sync run

The sync converts all rows in one run using one rate snapshot from OpenExchangeRates. This avoids one API call per row or per chunk, is friendlier to free-plan rate limits, and keeps all rows processed in one run consistent. A production payment system would usually persist normalized FX rate snapshots separately and link converted rows to the snapshot used.

### Why `order_date` uses a rolling timestamp window

The assignment describes the range as `current_date - 7d` to `current_date`. This implementation uses a rolling 7-day timestamp window from the current time to produce a finer-grained datetime distribution.

### Why `ON CONFLICT DO NOTHING` is used in the target

Airflow tasks may be retried or manually triggered more than once. Since `orders_eur.order_id` is the primary key, duplicate sync attempts should not duplicate converted facts. In this implementation, `processed_at` means the first successful target insert time, not the last retry attempt time.

### Production extensions

- Store normalized FX rate snapshots and link converted orders to a rate snapshot id.
- Use schema migrations instead of lazy `CREATE TABLE IF NOT EXISTS` for long-lived environments.
- Add Airflow pools for shared external APIs if multiple DAGs call the same provider.
- Export operational metrics to StatsD, Prometheus, or Grafana.
- Add integration tests for the full Docker, Airflow, and PostgreSQL path.

## Requirements

For running the project:

- Docker Desktop
- OpenExchangeRates API key

For local development checks:

- Python 3.11+
- pip

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
airflow-scheduler   internal scheduler service
postgres-1          localhost:5433 -> orders_source
postgres-2          localhost:5434 -> orders_target
postgres-airflow    internal Airflow metadata database
```

## Trigger DAGs Manually

Trigger order generation:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger generate_orders_dag
```

Check the generator run state and wait until the latest run is successful:

```bash
docker compose exec -T airflow-scheduler airflow dags list-runs -d generate_orders_dag
```

Trigger sync to EUR:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger sync_orders_to_eur_dag
```

Optionally check the sync run state:

```bash
docker compose exec -T airflow-scheduler airflow dags list-runs -d sync_orders_to_eur_dag
```

You can also trigger both DAGs from the Airflow UI.

The Compose configuration asks Airflow to create DAGs unpaused. If you reuse existing Airflow metadata and the DAGs are still paused, unpause them manually after starting the stack:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause generate_orders_dag
docker compose exec -T airflow-scheduler airflow dags unpause sync_orders_to_eur_dag
```

Because DAGs are unpaused by default, a fresh Airflow metadata database may start a scheduled run shortly after startup. If you also trigger the generator manually at the same time, the source table may contain more than one 5000-row batch.

## Static Checks

Run lightweight pre-run validation before starting the full Docker stack:

```bash
docker compose config
python -m py_compile dags/generate_orders_dag.py dags/sync_orders_to_eur_dag.py dags/common/exchange_rates.py dags/common/constants.py dags/common/db.py dags/common/schemas.py
```

## Development Checks

Install development dependencies and run local checks:

```bash
python -m pip install -r requirements-dev.txt
ruff check .
pytest
```

GitHub Actions runs syntax checks, ruff, and pytest on push and pull requests.

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

Quick checks:

```bash
docker exec -i postgres-1 psql -U postgres -d orders_source -c "SELECT COUNT(*) FROM orders;"
```

After one generation run, there should be at least 5000 rows. If the scheduled DAG also ran, the count may be 10000 or more.

## Check Target Database

Connect to `postgres-2`:

```bash
docker exec -it postgres-2 psql -U postgres -d orders_target
```

Useful SQL:

```sql
SELECT COUNT(*) FROM orders_eur;

SELECT order_id, original_amount, original_currency, amount_eur, conversion_rate_to_eur
FROM orders_eur
LIMIT 10;
```

Quick checks:

```bash
docker exec -i postgres-2 psql -U postgres -d orders_target -c "SELECT COUNT(*) FROM orders_eur;"
docker exec -i postgres-2 psql -U postgres -d orders_target -c "SELECT * FROM sync_state;"
```

## Expected Flow

1. Start Docker Compose.
2. Open Airflow UI.
3. Trigger `generate_orders_dag`.
4. Wait until the generator run is successful.
5. Check that `postgres-1 / orders_source / orders` contains at least 5000 rows.
6. Trigger `sync_orders_to_eur_dag`.
7. Wait until the sync run is successful.
8. Check that `postgres-2 / orders_target / orders_eur` contains converted rows.
9. Trigger `sync_orders_to_eur_dag` again.
10. Check that row count in `orders_eur` does not double.

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

## Reset Local Data

If local Docker volumes contain an older schema or stale test data, reset them before a clean test run:

```bash
docker compose down -v
docker compose up -d --build
```

`docker compose down -v` deletes local database data.

## Notes

- `order_date` is generated within the last 7 days.
- `created_at` is used as the technical timestamp for sync logic.
- OpenExchangeRates is called once per non-empty sync run, not once per row.
- `orders_eur` keeps the original amount, original currency, converted EUR amount, and conversion multiplier for easier debugging.
- `.env` contains secrets and must not be committed.
