"""Pytest golden suite for viz.education_derived."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()

VINTAGE = 'acs2024_5yr'
TABLE = 'viz.education_derived'
REL_TOLERANCE = 1e-6
ABS_TOLERANCE = 1e-6

GEOIDS = [
  '01000US',
  '04000US49',
  '15000US060855115021',
  '16000US4948830',
]

NULL_CASE_GEOID = '16000US4948830'
NULL_CASE_FLAG_BIT = 64

GOLDEN_FILE = Path(__file__).parent / 'education_derived.json'
GOLDEN_FIELDS = [
  'flags',
  'edu_education_index',
  'edu_education_index_lo90',
  'edu_education_index_hi90',
  'edu_years_of_school',
  'edu_years_of_school_lo90',
  'edu_years_of_school_hi90',
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
  assert exp is not None, f'Missing golden entry for {geoid}'

  row = fetch_row(conn, geoid)
  for field in GOLDEN_FIELDS:
    if field in exp:
      assert_value_close(geoid, field, row.get(field), exp.get(field))


def test_null_case_flag8_and_null_outputs(conn):
  row = fetch_row(conn, NULL_CASE_GEOID)
  assert int(row['flags']) & NULL_CASE_FLAG_BIT, (
    f'[{NULL_CASE_GEOID}] expected flag bit {NULL_CASE_FLAG_BIT}, got flags={row["flags"]}'
  )
  for field in NUMERIC_FIELDS:
    assert row[field] is None, (
      f'[{NULL_CASE_GEOID}] expected {field} to be NULL, got {row[field]!r}'
    )
