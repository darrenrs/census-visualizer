"""Pytest suite for viz.income_derived.

Requires DATABASE_URL env var pointing to local Postgres with the income pipeline run for vintage.
Also place golden values in income_derived.json (see README for format).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()

VINTAGE = 'acs2024_5yr'
TABLE = 'viz.income_derived'
REL_TOLERANCE = 0.1

# Geoids expected to produce valid computed output
COMPUTED_GEOIDS = [
  '16000US4965110',  # P95 anchor, all means available
  '16000US4940360',  # P95 topcoded, P80 anchor
  '16000US4935190',  # P95+P80 topcoded, P60 anchor
  '16000US4969280',  # P95+P80+P60 topcoded, P40 anchor
  '16000US0603092',  # P95+P80+P60+P40 topcoded, P20 anchor
  '06000US7212757247',  # P20 bottom coded edge case
]

# (geoid, expected_flag_bit) — outputs should all be NULL
NULL_CASES = [
  ('15000US490351008001', 4),  # block group
  ('16000US4948830', 8),  # population too small
  ('16000US4946410', 16),  # missing thresholds
  ('16000US5142216', 32),  # all coded/topcoded
]

NUMERIC_FIELDS = [
  'hhi_sim_p90',
  'hhi_sim_p90_lo90',
  'hhi_sim_p90_hi90',
  'hhi_sim_p95',
  'hhi_sim_p95_lo90',
  'hhi_sim_p95_hi90',
  'hhi_sim_p99',
  'hhi_sim_p99_lo90',
  'hhi_sim_p99_hi90',
  'hhi_sim_p999',
  'hhi_sim_p999_lo90',
  'hhi_sim_p999_hi90',
]


# --- Fixtures ---


@pytest.fixture(scope='session')
def conn():
  url = os.getenv('DATABASE_URL')
  if not url:
    pytest.skip('DATABASE_URL not set')
  with psycopg.connect(url) as c:
    yield c


@pytest.fixture(scope='session')
def golden():
  p = Path(__file__).parent / 'test_income.json'
  if not p.exists():
    return {}
  data = json.loads(p.read_text())
  if data.get('vintage') != VINTAGE:
    return {}
  return data.get('rows', {})


# --- Helpers ---


def fetch_row(conn, geoid):
  cols = ['vintage', 'sumlevel', 'geoid', 'hhi_sim_anchor', 'hhi_sim_acc', 'flags'] + NUMERIC_FIELDS
  with conn.cursor() as cur:
    cur.execute(
      f'SELECT {", ".join(cols)} FROM {TABLE} WHERE vintage = %s AND geoid = %s',
      (VINTAGE, geoid),
    )
    row = cur.fetchone()
  assert row is not None, f'{geoid} not found in {TABLE} for vintage={VINTAGE}'
  return dict(zip(cols, row, strict=False))


def assert_close(geoid, field, got, exp):
  if exp is None:
    assert got is None, f'[{geoid}] {field}: expected NULL, got {got!r}'
    return
  assert got is not None, f'[{geoid}] {field}: expected {exp}, got NULL'
  rel_err = abs(float(got) - float(exp)) / max(abs(float(exp)), 1.0)
  assert rel_err <= REL_TOLERANCE, (
    f'[{geoid}] {field}: got {got}, expected {exp}, rel_err={rel_err:.4f}'
  )


# --- Tests ---


@pytest.mark.parametrize('geoid,flag_bit', NULL_CASES)
def test_null_cases(conn, geoid, flag_bit):
  row = fetch_row(conn, geoid)
  assert int(row['flags']) & flag_bit, (
    f'[{geoid}] expected flag bit {flag_bit}, got flags={row["flags"]}'
  )
  for field in NUMERIC_FIELDS:
    assert row[field] is None, f'[{geoid}] expected {field} to be NULL, got {row[field]!r}'


def test_monotone_tail(conn):
  for geoid in COMPUTED_GEOIDS:
    row = fetch_row(conn, geoid)
    p95, p99, p999 = row['hhi_sim_p95'], row['hhi_sim_p99'], row['hhi_sim_p999']
    if p95 is not None and p99 is not None:
      assert p99 >= p95, f'[{geoid}] p99={p99} < p95={p95}'
    if p99 is not None and p999 is not None:
      assert p999 >= p99, f'[{geoid}] p999={p999} < p99={p99}'


@pytest.mark.parametrize('geoid', COMPUTED_GEOIDS)
def test_golden(conn, golden, geoid):
  if not golden:
    pytest.skip('No golden file found')
  exp = golden.get(geoid)
  if exp is None:
    pytest.skip(f'No golden entry for {geoid}')

  row = fetch_row(conn, geoid)

  if 'sumlevel' in exp:
    assert int(row['sumlevel']) == int(exp['sumlevel']), f'[{geoid}] sumlevel mismatch'
  if exp.get('hhi_sim_anchor') is not None:
    assert int(row['hhi_sim_anchor']) == int(exp['hhi_sim_anchor']), f'[{geoid}] anchor mismatch'
  if exp.get('flags') is not None:
    assert int(row['flags']) == int(exp['flags']), f'[{geoid}] flags mismatch'

  for field in NUMERIC_FIELDS + ['hhi_sim_acc']:
    if field in exp:
      assert_close(geoid, field, row.get(field), exp.get(field))
