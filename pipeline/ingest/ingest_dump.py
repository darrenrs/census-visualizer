from __future__ import annotations

import csv
import os
import re
import shutil
from pathlib import Path
from urllib.request import Request, urlopen

import psycopg
from common import COPY_CHUNK_SIZE, ensure_raw_storage, replace_vintage, write_rows
from dotenv import load_dotenv
from raw_manifest import (
  RAW_TABLES,
  SUPPORTED_SUMLEVELS,
  normalize_summary_geoid,
  summary_file_column_name,
  table_columns,
)

SUMMARY_FILE_ROOT = 'https://www2.census.gov/programs-surveys/acs/summary_file'
SUMMARY_FILE_START_YEAR = 2018
DOWNLOAD_TIMEOUT_SECONDS = 60
WORK_ROOT = Path(__file__).resolve().parent / 'work'
USER_AGENT = 'census-visualizer'


def require_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f'{name} is not set (set it in .env or environment.)')
  return value


def parse_vintage(vintage: str) -> int:
  match = re.fullmatch(r'acs(\d{4})_([15])yr', vintage.strip().lower())
  if not match:
    raise RuntimeError('VINTAGE must look like acs2024_5yr or acs2023_1yr.')

  year = int(match.group(1))
  period = match.group(2)
  if period != '5':
    raise RuntimeError('dump ingest currently supports only 5-year ACS vintages.')
  if year < SUMMARY_FILE_START_YEAR:
    raise RuntimeError(
      f'dump ingest currently supports only acs{SUMMARY_FILE_START_YEAR}_5yr and newer.'
    )
  return year


def data_base_url(year: int) -> str:
  if year >= 2021:
    return f'{SUMMARY_FILE_ROOT}/{year}/table-based-SF/data/5YRData'
  return f'{SUMMARY_FILE_ROOT}/{year}/prototype/5YRData'


def geography_url(year: int) -> str:
  if year >= 2021:
    return f'{SUMMARY_FILE_ROOT}/{year}/table-based-SF/documentation/Geos{year}5YR.txt'
  return f'{SUMMARY_FILE_ROOT}/{year}/prototype/Geos{year}5YR.csv'


def table_url(year: int, table_name: str) -> str:
  table_code = table_name.removesuffix('_moe')
  return f'{data_base_url(year)}/acsdt5y{year}-{table_code}.dat'


def download_file(url: str, path: Path) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  if path.exists() and path.stat().st_size > 0:
    print(f'Using cached file {path.name}')
    return

  print(f'Downloading {path.name}')
  request = Request(url, headers={'User-Agent': USER_AGENT})
  with urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response, path.open('wb') as out_file:
    shutil.copyfileobj(response, out_file)


def coerce_nullable(value: str | None) -> str | None:
  if value is None:
    return None
  trimmed = value.strip()
  if trimmed == '':
    return None
  return trimmed


def derive_state_code(sumlevel: int, value: str | None) -> str | None:
  trimmed = coerce_nullable(value)
  if trimmed is not None:
    return trimmed
  if sumlevel in (10, 310, 860):
    return 'US'
  return None


def extract_source_geoid(row: dict[str, str]) -> str | None:
  for key in ('GEO_ID', 'DADSID', 'GEOID'):
    value = coerce_nullable(row.get(key))
    if value:
      return value
  return None


def open_summary_file(path: Path):
  if path.suffix == '.csv':
    # Older prototype geography files can contain cp1252-style bytes in names.
    return path.open(newline='', encoding='cp1252')
  return path.open(newline='', encoding='utf-8-sig')


def load_geoheader(conn: psycopg.Connection, geography_path: Path, vintage: str) -> set[str]:
  delimiter = '|' if geography_path.suffix == '.txt' else ','
  supported_geoids: set[str] = set()
  buffer: list[tuple[object, ...]] = []
  row_count = 0

  with open_summary_file(geography_path) as file_obj:
    reader = csv.DictReader(file_obj, delimiter=delimiter)
    for row in reader:
      sumlevel_raw = coerce_nullable(row.get('SUMLEVEL'))
      geoid_raw = extract_source_geoid(row)
      name = coerce_nullable(row.get('NAME'))
      component = coerce_nullable(row.get('COMPONENT'))

      if not sumlevel_raw or not geoid_raw or not name or not component:
        continue

      sumlevel = int(sumlevel_raw)
      if sumlevel not in SUPPORTED_SUMLEVELS:
        continue

      geoid = normalize_summary_geoid(geoid_raw)
      supported_geoids.add(geoid)
      buffer.append(
        (
          vintage,
          geoid,
          name,
          sumlevel,
          component,
          derive_state_code(sumlevel, row.get('STUSAB')),
        )
      )

      if len(buffer) >= COPY_CHUNK_SIZE:
        write_rows(conn, 'geoheader', buffer)
        row_count += len(buffer)
        buffer.clear()

  if buffer:
    write_rows(conn, 'geoheader', buffer)
    row_count += len(buffer)

  print(f'Loaded geoheader rows: {row_count}')
  return supported_geoids


def load_data_table(
  conn: psycopg.Connection,
  data_path: Path,
  table_name: str,
  vintage: str,
  supported_geoids: set[str],
) -> None:
  raw_columns = table_columns(table_name)
  source_columns = [summary_file_column_name(raw_column) for raw_column in raw_columns]
  buffer: list[tuple[object, ...]] = []
  row_count = 0

  with open_summary_file(data_path) as file_obj:
    reader = csv.DictReader(file_obj, delimiter='|')
    fieldnames = set(reader.fieldnames or [])
    missing = [column for column in ['GEO_ID', *source_columns] if column not in fieldnames]
    if missing:
      raise RuntimeError(f'Missing columns in {data_path.name}: {", ".join(missing)}')

    for row in reader:
      geoid_raw = coerce_nullable(row.get('GEO_ID'))
      if not geoid_raw:
        continue

      geoid = normalize_summary_geoid(geoid_raw)
      if geoid not in supported_geoids:
        continue

      buffer.append(
        (
          vintage,
          geoid,
          *[coerce_nullable(row.get(column)) for column in source_columns],
        )
      )

      if len(buffer) >= COPY_CHUNK_SIZE:
        write_rows(conn, table_name, buffer)
        row_count += len(buffer)
        buffer.clear()

  if buffer:
    write_rows(conn, table_name, buffer)
    row_count += len(buffer)

  print(f'Loaded {table_name} rows: {row_count}')


def main() -> None:
  load_dotenv()

  vintage = require_env('VINTAGE')
  db_url = require_env('DATABASE_URL')
  year = parse_vintage(vintage)

  work_dir = WORK_ROOT / vintage
  geography_path = work_dir / Path(geography_url(year)).name
  table_paths = {
    table_name: work_dir / Path(table_url(year, table_name)).name
    for table_name in RAW_TABLES
    if table_name != 'geoheader'
  }

  download_file(geography_url(year), geography_path)
  for table_name, path in table_paths.items():
    download_file(table_url(year, table_name), path)

  with psycopg.connect(db_url) as conn:
    ensure_raw_storage(conn)
    replace_vintage(conn, vintage)
    supported_geoids = load_geoheader(conn, geography_path, vintage)

    for table_name in [name for name in RAW_TABLES if name != 'geoheader']:
      print(f'Loading Summary File table {table_name}')
      load_data_table(conn, table_paths[table_name], table_name, vintage, supported_geoids)


if __name__ == '__main__':
  main()
