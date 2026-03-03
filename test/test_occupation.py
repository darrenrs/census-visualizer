"""Pytest golden suite for viz.occupation_derived."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()

VINTAGE = 'acs2024_5yr'
TABLE = 'viz.occupation_derived'
REL_TOLERANCE = 1e-6
ABS_TOLERANCE = 1e-6

GEOIDS = [
  '01000US',
  '04000US49',
  '14000US72057270800',
  '16000US2455050',
  '16000US4948830',
]

NULL_CASE_GEOID = '16000US4948830'
NULL_CASE_FLAG_BIT = 8

GOLDEN_FILE = Path(__file__).parent / 'test_occupation.json'
GOLDEN_FIELDS = [
  'flags',
  'occ_occupation_index',
  'occ_occupation_index_lo90',
  'occ_occupation_index_hi90',
  'occ_occupation_index_ext',
  'occ_occupation_index_ext_lo90',
  'occ_occupation_index_ext_hi90',
  'occ_occupation_index_ratio',
  'occ_occupation_index_ratio_lo90',
  'occ_occupation_index_ratio_hi90',
]
NUMERIC_FIELDS = [f for f in GOLDEN_FIELDS if f != 'flags']


@pytest.fixture(scope='session')
def conn():
  url = os.getenv('DATABASE_URL')
  if not url:
    pytest.skip('DATABASE_URL not set')
  with psycopg.connect(url) as c:
    yield c


@pytest.fixture(scope='session')
def golden():
  if not GOLDEN_FILE.exists():
    pytest.skip(f'Golden file not found: {GOLDEN_FILE}')

  data = json.loads(GOLDEN_FILE.read_text())
  if data.get('vintage') != VINTAGE:
    pytest.skip(f'Golden file vintage mismatch: expected {VINTAGE}')

  rows = data.get('rows', {})
  if not isinstance(rows, dict):
    pytest.skip('Golden file rows must be an object')
  return rows


def fetch_row(conn, geoid):
  cols = ['vintage', 'sumlevel', 'geoid'] + GOLDEN_FIELDS
  with conn.cursor() as cur:
    cur.execute(
      f"""
      SELECT {', '.join(cols)}
      FROM {TABLE}
      WHERE vintage = %s
        AND geoid = %s
      """,
      (VINTAGE, geoid),
    )
    row = cur.fetchone()

  assert row is not None, f'{geoid} not found in {TABLE} for vintage={VINTAGE}'
  return dict(zip(cols, row, strict=False))


def assert_value_close(geoid: str, field: str, got, exp):
  if exp is None:
    assert got is None, f'[{geoid}] {field}: expected NULL, got {got!r}'
    return

  if field == 'flags':
    assert int(got) == int(exp), f'[{geoid}] flags mismatch: got={got}, expected={exp}'
    return

  assert got is not None, f'[{geoid}] {field}: expected {exp}, got NULL'
  rel_err = abs(float(got) - float(exp)) / max(abs(float(exp)), 1.0)
  abs_err = abs(float(got) - float(exp))
  assert rel_err <= REL_TOLERANCE or abs_err <= ABS_TOLERANCE, (
    f'[{geoid}] {field}: got={got}, expected={exp}, rel_err={rel_err:.3e}, abs_err={abs_err:.3e}'
  )


@pytest.mark.parametrize('geoid', GEOIDS)
def test_golden_values_for_selected_geoids(conn, golden, geoid):
  exp = golden.get(geoid)
  if exp is None:
    pytest.skip(f'Missing golden entry for {geoid}')

  row = fetch_row(conn, geoid)
  for field in GOLDEN_FIELDS:
    if field in exp:
      assert_value_close(geoid, field, row.get(field), exp.get(field))


def test_null_case_flag_and_null_outputs(conn):
  row = fetch_row(conn, NULL_CASE_GEOID)
  assert int(row['flags']) & NULL_CASE_FLAG_BIT, (
    f'[{NULL_CASE_GEOID}] expected flag bit {NULL_CASE_FLAG_BIT}, got flags={row["flags"]}'
  )
  for field in NUMERIC_FIELDS:
    assert row[field] is None, (
      f'[{NULL_CASE_GEOID}] expected {field} to be NULL, got {row[field]!r}'
    )


@pytest.mark.parametrize('geoid', GEOIDS)
def test_occupation_bounds_and_ratio_identity(conn, geoid):
  row = fetch_row(conn, geoid)

  base = row['occ_occupation_index']
  base_lo = row['occ_occupation_index_lo90']
  base_hi = row['occ_occupation_index_hi90']
  ext = row['occ_occupation_index_ext']
  ext_lo = row['occ_occupation_index_ext_lo90']
  ext_hi = row['occ_occupation_index_ext_hi90']
  ratio = row['occ_occupation_index_ratio']
  ratio_lo = row['occ_occupation_index_ratio_lo90']
  ratio_hi = row['occ_occupation_index_ratio_hi90']

  if base is None or ext is None or ratio is None:
    assert base_lo is None and base_hi is None
    assert ext_lo is None and ext_hi is None
    assert ratio_lo is None and ratio_hi is None
    return

  assert 1.0 <= float(base) <= 5.0, f'[{geoid}] base out of range: {base}'
  assert 1.0 <= float(ext) <= 25.0, f'[{geoid}] ext out of range: {ext}'
  assert 0.2 <= float(ratio) <= 25.0, f'[{geoid}] ratio out of range: {ratio}'

  assert base_lo is not None and base_hi is not None
  assert ext_lo is not None and ext_hi is not None
  assert ratio_lo is not None and ratio_hi is not None

  assert 1.0 <= float(base_lo) <= 5.0 and 1.0 <= float(base_hi) <= 5.0
  assert 1.0 <= float(ext_lo) <= 25.0 and 1.0 <= float(ext_hi) <= 25.0
  assert 0.2 <= float(ratio_lo) <= 25.0 and 0.2 <= float(ratio_hi) <= 25.0

  assert float(base_lo) <= float(base) <= float(base_hi)
  assert float(ext_lo) <= float(ext) <= float(ext_hi)
  assert float(ratio_lo) <= float(ratio) <= float(ratio_hi)

  derived_ratio = float(ext) / float(base)
  rel_err = abs(float(ratio) - derived_ratio) / max(abs(derived_ratio), 1.0)
  abs_err = abs(float(ratio) - derived_ratio)
  assert rel_err <= REL_TOLERANCE or abs_err <= ABS_TOLERANCE, (
    f'[{geoid}] ratio mismatch: ratio={ratio}, ext/base={derived_ratio}'
  )
