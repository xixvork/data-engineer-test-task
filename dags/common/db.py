import os

import psycopg2


def get_postgres_connection(prefix: str):
    return psycopg2.connect(
        host=os.environ[f"{prefix}_HOST"],
        port=int(os.environ[f"{prefix}_PORT"]),
        dbname=os.environ[f"{prefix}_DB"],
        user=os.environ[f"{prefix}_USER"],
        password=os.environ[f"{prefix}_PASSWORD"],
    )


def get_source_connection():
    return get_postgres_connection("POSTGRES_1")


def get_target_connection():
    return get_postgres_connection("POSTGRES_2")
