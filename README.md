# Test Task

Local Airflow + PostgreSQL project for generating orders and syncing them to another PostgreSQL database with EUR conversion.

## Services

- Airflow Webserver: http://localhost:8080
- postgres-1: source database with `orders`
- postgres-2: target database with `orders_eur`

## Requirements

- Docker Desktop
- OpenExchangeRates API key

## Setup

```bash
cp .env.example .env