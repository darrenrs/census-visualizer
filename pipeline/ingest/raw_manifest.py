from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

ROOT_DIR = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT_DIR / 'pipeline' / 'sql'

type RawColumnType = Literal['text', 'integer', 'real']

SUPPORTED_SUMLEVELS = [
  10,
  40,
  50,
  60,
  140,
  150,
  160,
  310,
  500,
  860,
]

RAW_TABLES = [
  'geoheader',
  'b01003_moe',
  'b11001_moe',
  'b25010_moe',
  'b19013_moe',
  'b19080_moe',
  'b19081_moe',
  'b19083_moe',
  'b15002_moe',
  'b03002_moe',
  'c24010_moe',
]

GEOHEADER_COLUMNS: list[tuple[str, RawColumnType]] = [
  ('name', 'text'),
  ('sumlevel', 'integer'),
  ('component', 'text'),
  ('stusab', 'text'),
]

_COLUMN_PATTERN = re.compile(r'\b(?:b\d{8}|c24010\d{3})(?:_moe)?\b')
_RAW_COLUMN_PATTERN = re.compile(r'([bc]\d{5})(\d{3})(?:(_moe))?$')


def table_columns(table_name: str) -> list[str]:
  if table_name == 'geoheader':
    return [name for name, _ in GEOHEADER_COLUMNS]

  table_code = table_name.removesuffix('_moe')
  found: set[str] = set()
  for path in SQL_DIR.glob('0[1-5]_*.sql'):
    found.update(
      col for col in _COLUMN_PATTERN.findall(path.read_text()) if col.startswith(table_code)
    )
  return sorted(found)


def table_column_defs(table_name: str) -> list[tuple[str, RawColumnType]]:
  if table_name == 'geoheader':
    return GEOHEADER_COLUMNS
  return [(column, 'real') for column in table_columns(table_name)]


def table_insert_columns(table_name: str) -> list[str]:
  return ['vintage', 'geoid', *[name for name, _ in table_column_defs(table_name)]]


def table_select_columns(table_name: str) -> list[str]:
  if table_name == 'geoheader':
    return ['geoid', 'name', 'sumlevel', 'component', 'stusab']
  return ['geoid', *table_columns(table_name)]


def summary_file_column_name(raw_column: str) -> str:
  match = _RAW_COLUMN_PATTERN.fullmatch(raw_column)
  if not match:
    raise RuntimeError(f'Unsupported raw column format: {raw_column}')

  table_code = match.group(1).upper()
  line = match.group(2)
  suffix = 'M' if match.group(3) else 'E'
  return f'{table_code}_{suffix}{line}'


def normalize_summary_geoid(source_geoid: str) -> str:
  trimmed = source_geoid.strip()
  if re.fullmatch(r'\d{3}.{4}US.*', trimmed):
    return trimmed[:3] + trimmed[5:]
  return trimmed
