from __future__ import annotations

import os
from collections.abc import Sequence

import psycopg
from common import ensure_raw_storage, replace_vintage, write_rows
from dotenv import load_dotenv
from psycopg import sql
from raw_manifest import RAW_TABLES, table_select_columns


def source_database_url() -> str:
  source_url = os.getenv('SOURCE_DATABASE_URL')
  target_url = os.getenv('DATABASE_URL')
  if source_url:
    return source_url
  if target_url:
    return target_url
  raise RuntimeError('DATABASE_URL is not set (set it in .env or environment.)')


def require_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f'{name} is not set (set it in .env or environment.)')
  return value


def validate_source_table(
  cur: psycopg.Cursor, schema_name: str, table_name: str, required_columns: Sequence[str]
) -> None:
  cur.execute(
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position
    """,
    (schema_name, table_name),
  )
  found = {row[0] for row in cur.fetchall()}
  if not found:
    raise RuntimeError(f'Missing source table {schema_name}.{table_name}')

  missing = [column for column in required_columns if column not in found]
  if missing:
    raise RuntimeError(f'Missing columns in {schema_name}.{table_name}: {", ".join(missing)}')


def load_table(
  source_conn: psycopg.Connection,
  target_conn: psycopg.Connection,
  schema_name: str,
  table_name: str,
  vintage: str,
) -> None:
  select_columns = table_select_columns(table_name)
  with source_conn.cursor() as meta_cur:
    validate_source_table(meta_cur, schema_name, table_name, select_columns)

  query = sql.SQL('SELECT {} FROM {}').format(
    sql.SQL(', ').join(sql.Identifier(column) for column in select_columns),
    sql.Identifier(schema_name, table_name),
  )

  with source_conn.cursor(name=f'{table_name}_stream') as cur:
    cur.execute(query)
    while rows := cur.fetchmany(5_000):
      write_rows(target_conn, table_name, [(vintage, *row) for row in rows])


def main() -> None:
  load_dotenv()

  vintage = require_env('VINTAGE')
  target_url = require_env('DATABASE_URL')
  source_url = source_database_url()

  with psycopg.connect(source_url) as source_conn, psycopg.connect(target_url) as target_conn:
    ensure_raw_storage(target_conn)
    replace_vintage(target_conn, vintage)

    for table_name in RAW_TABLES:
      print(f'Loading SQL source table {table_name}')
      load_table(source_conn, target_conn, vintage, table_name, vintage)


if __name__ == '__main__':
  main()
