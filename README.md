# Orders Sync Pipeline — Airflow + PostgreSQL + EUR Conversion

![CI](https://github.com/xixvork/data-engineer-test-task/actions/workflows/ci.yml/badge.svg)

Airflow-orchestrated pipeline that generates synthetic orders into one PostgreSQL database and syncs them into another with currency conversion to EUR using OpenExchangeRates.

## Stack

- Apache Airflow 2.10.5
- PostgreSQL 15
- Python via the Airflow base image
- Docker Compose
- OpenExchangeRates API

## Architecture

```text
                 every 10 min
        ┌────────────────────────────┐
        │ generate_orders_dag        │
        │ 5000 synthetic orders      │
        │ rolling 7-day order_date   │
        └──────────────┬─────────────┘
                       │
                       ▼
        postgres-1 / orders_source / orders
                       │
                       │ cursor-paged read
                       ▼
        ┌────────────────────────────┐       fetch latest rates
        │ sync_orders_to_eur_dag     │ ◀──────────────────────────────▶ OpenExchangeRates
        │ convert to EUR             │
        │ ON CONFLICT DO NOTHING     │
        └──────────────┬─────────────┘
                       │
                       ▼
        postgres-2 / orders_target / orders_eur
        postgres-2 / orders_target / sync_state
```

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

- `dags/` contains Airflow DAG definitions and shared runtime modules.
- `tests/` contains unit tests for reusable conversion logic.
- `.github/workflows/ci.yml` runs syntax checks, ruff, and pytest on push and pull requests.

## DAGs

### `generate_orders_dag`

Runs every 10 minutes. It creates the source table if needed and writes exactly 5000 generated rows per successful run into:

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

Runs at the top of every hour. It creates target tables if needed, reads source orders incrementally in chunks, and stores cursor progress in:

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
| `conversion_rate_to_eur` | `NUMERIC(18, 8)` | Multiplier used to convert one unit of the original currency into EUR |
| `source_created_at` | `TIMESTAMPTZ` | Source technical insertion timestamp |
| `processed_at` | `TIMESTAMPTZ` | Target insertion timestamp |

The sync is idempotent: repeated runs do not duplicate rows because `orders_eur.order_id` is the primary key and inserts use `ON CONFLICT DO NOTHING`.

## Design Decisions and Trade-offs

### Money as `NUMERIC(12, 2)`

The task describes `amount` as a float with 2 decimal places. In payment-style data, money should use exact decimal arithmetic: binary floats can introduce representation errors, for example `0.1 + 0.2 != 0.3`. PostgreSQL `NUMERIC(12, 2)` and Python `Decimal` preserve cent-level precision during conversion and aggregation. The `(12, 2)` precision allows values up to `9,999,999,999.99`, which is far above the generated order range.

### Incremental sync cursor

`sync_state` stores the last processed `(created_at, order_id)` cursor. `created_at` alone is not unique because a generated batch can contain many rows with the same technical insertion timestamp; adding `order_id` makes the cursor deterministic and lets the sync continue without rescanning the full source table.

### Target idempotency

`orders_eur` stores original amount, original currency, converted EUR amount, and `conversion_rate_to_eur`, the multiplier used for conversion to EUR. `orders_eur.order_id` is the primary key, and target inserts use `ON CONFLICT DO NOTHING` so Airflow retries or manual reruns do not duplicate converted facts. In this implementation, `processed_at` is the first successful target insert time.

### Time and generated dates

Timestamps use `TIMESTAMPTZ` so generated and processed times keep timezone meaning across Airflow, PostgreSQL, and local machines. The assignment describes the generated order range as `current_date - 7d` to `current_date`; this implementation uses a rolling 7-day timestamp window relative to generation time for a finer-grained datetime distribution.

### Exchange-rate fetches

The sync fetches OpenExchangeRates once per non-empty sync run, after confirming there are pending source rows. This avoids one API call per row or per chunk, is friendlier to free-plan rate limits, and keeps all rows processed in one run consistent.

### Dependency pinning

Runtime dependencies are pinned in `requirements.txt` and installed at image build time by the `Dockerfile`. Development and CI dependencies are pinned separately in `requirements-dev.txt`.

### Production extensions

- Add an `exchange_rate_snapshots` table with `(snapshot_id, fetched_at, base_currency, rates_jsonb)` and reference it from `orders_eur`. This would make FX conversion auditable at snapshot level and reduce repeated per-row rate metadata.
- Use schema migrations instead of lazy `CREATE TABLE IF NOT EXISTS` for long-lived environments.
- Add integration tests for the Docker, Airflow, PostgreSQL, and OpenExchangeRates path with a mocked FX provider.
- Export operational metrics for generated rows, synced rows, skipped duplicates, and external API failures.

## Requirements

For running the project:

- Docker Desktop
- OpenExchangeRates app id

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

You can create a free app id at OpenExchangeRates: https://openexchangerates.org/signup/free

Do not commit `.env`; it contains local secrets.

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

These credentials are for local development only and must not be used in shared or production environments.

## Services

```text
airflow-webserver   http://localhost:8080
airflow-scheduler   schedules and runs DAG tasks
postgres-1          localhost:5433 -> orders_source
postgres-2          localhost:5434 -> orders_target
postgres-airflow    Airflow metadata DB
```

## Trigger DAGs Manually

Trigger order generation and check the latest run state:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger generate_orders_dag
docker compose exec -T airflow-scheduler airflow dags list-runs -d generate_orders_dag
```

Trigger sync to EUR after the generator run succeeds:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger sync_orders_to_eur_dag
docker compose exec -T airflow-scheduler airflow dags list-runs -d sync_orders_to_eur_dag
```

You can also trigger both DAGs from the Airflow UI.

The Compose configuration creates DAGs unpaused. With a fresh Airflow metadata database, scheduled runs may start shortly after startup. Each successful generator run creates exactly 5000 rows. The total source row count increases by 5000 per successful generator run; it may already be higher if scheduled runs have also executed.

If reused Airflow metadata still has the DAGs paused, unpause them manually:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause generate_orders_dag
docker compose exec -T airflow-scheduler airflow dags unpause sync_orders_to_eur_dag
```

## Database Connections

Connect to the source database:

```bash
docker exec -it postgres-1 psql -U postgres -d orders_source
```

Connect to the target database:

```bash
docker exec -it postgres-2 psql -U postgres -d orders_target
```

Quick row-count checks:

```bash
docker exec -i postgres-1 psql -U postgres -d orders_source -c "SELECT COUNT(*) FROM orders;"
docker exec -i postgres-2 psql -U postgres -d orders_target -c "SELECT COUNT(*) FROM orders_eur;"
docker exec -i postgres-2 psql -U postgres -d orders_target -c "SELECT * FROM sync_state;"
```

## How to verify correctness

Run source checks in `postgres-1 / orders_source`.

Generator creates exactly 5000 rows per batch:

```sql
SELECT batch_id, COUNT(*)
FROM orders
GROUP BY batch_id
ORDER BY batch_id;
```

Expected: each `batch_id` should have exactly 5000 rows.

Source order dates are within the rolling 7-day window relative to insert time:

```sql
SELECT COUNT(*) AS invalid_order_dates
FROM orders
WHERE order_date < created_at - interval '7 days'
   OR order_date > created_at;
```

Expected: `0`.

Run target checks in `postgres-2 / orders_target`.

Sync is idempotent:

```sql
SELECT COUNT(*) FROM orders_eur;
```

Expected: if no new source rows were generated, triggering `sync_orders_to_eur_dag` again should not change this count.

All target rows have converted amounts:

```sql
SELECT COUNT(*) AS null_converted_amounts
FROM orders_eur
WHERE amount_eur IS NULL;
```

Expected: `0`.

EUR orders preserve the original amount:

```sql
SELECT COUNT(*) AS changed_eur_rows
FROM orders_eur
WHERE original_currency = 'EUR'
  AND amount_eur != original_amount;
```

Expected: `0`.

Target currency distribution:

```sql
SELECT original_currency, COUNT(*)
FROM orders_eur
GROUP BY original_currency
ORDER BY original_currency;
```

Expected: the result should include the same generated currencies that appear in the source `orders` table.

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
