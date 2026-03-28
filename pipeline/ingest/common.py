from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator, Sequence

import psycopg
from psycopg import sql
from raw_manifest import (
  RAW_TABLES,
  RawColumnType,
  table_column_defs,
  table_insert_columns,
)

COPY_CHUNK_SIZE = 10_000
COLUMN_TYPE_SQL: dict[RawColumnType, sql.Composable] = {
  'text': sql.SQL('text'),
  'integer': sql.SQL('integer'),
  'real': sql.SQL('real'),
}


def chunked(
  rows: Iterable[Sequence[object]], chunk_size: int = COPY_CHUNK_SIZE
) -> Iterator[list[Sequence[object]]]:
  chunk: list[Sequence[object]] = []
  for row in rows:
    chunk.append(row)
    if len(chunk) >= chunk_size:
      yield chunk
      chunk = []
  if chunk:
    yield chunk


def ensure_raw_storage(conn: psycopg.Connection) -> None:
  with conn.cursor() as cur:
    cur.execute('CREATE SCHEMA IF NOT EXISTS raw')

    for table_name in RAW_TABLES:
      column_defs: list[sql.Composable] = [
        sql.SQL('vintage text NOT NULL'),
        sql.SQL('geoid character varying NOT NULL'),
      ]
      for column_name, column_type in table_column_defs(table_name):
        column_defs.append(
          sql.SQL('{} {}').format(
            sql.Identifier(column_name),
            COLUMN_TYPE_SQL[column_type],
          )
        )
      column_defs.append(sql.SQL('PRIMARY KEY (vintage, geoid)'))

      cur.execute(
        sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(
          sql.Identifier('raw', table_name),
          sql.SQL(', ').join(column_defs),
        )
      )

    cur.execute(
      'CREATE INDEX IF NOT EXISTS raw_geoheader_vintage_sumlevel_idx '
      'ON raw.geoheader (vintage, sumlevel)'
    )

  conn.commit()


def replace_vintage(conn: psycopg.Connection, vintage: str) -> None:
  with conn.cursor() as cur:
    for table_name in RAW_TABLES:
      cur.execute(
        sql.SQL('DELETE FROM {} WHERE vintage = %s').format(sql.Identifier('raw', table_name)),
        (vintage,),
      )
  conn.commit()


def write_rows(
  conn: psycopg.Connection,
  table_name: str,
  rows: Sequence[Sequence[object]],
) -> None:
  if not rows:
    return

  columns = table_insert_columns(table_name)
  with conn.cursor() as cur:
    with cur.copy(
      sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(
        sql.Identifier('raw', table_name),
        sql.SQL(', ').join(sql.Identifier(column) for column in columns),
      )
    ) as copy:
      buffer = io.StringIO()
      writer = csv.writer(buffer)
      writer.writerows(rows)
      buffer.seek(0)
      copy.write(buffer.getvalue())

  conn.commit()
