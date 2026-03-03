import csv
import io
import itertools
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
from dotenv import load_dotenv
from tqdm import tqdm

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS viz.diversity_derived (
  vintage                    text NOT NULL,
  sumlevel                   integer NOT NULL,
  geoid                      text NOT NULL,

  -- Derived/simulated values
  race_diversity_index       double precision,
  race_diversity_index_lo90  double precision,
  race_diversity_index_hi90  double precision,

  -- Used for data quality purposes
  flags                      integer NOT NULL default 0,

  PRIMARY KEY (vintage, sumlevel, geoid)
);
"""

QUERY_RESET_TABLE = 'DELETE FROM viz.diversity_derived WHERE vintage = %s;'

QUERY_POPULATE_TABLE_WITH_FLAGS = """
INSERT INTO viz.diversity_derived (
  vintage, sumlevel, geoid,
  race_diversity_index,
  flags
)
SELECT
  b.vintage,
  b.sumlevel,
  b.geoid,

  CASE WHEN (
    g.total_population < 250
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    1.0
    - (
        POWER(b.race_white_nh, 2)
      + POWER(b.race_black_nh, 2)
      + POWER(b.race_aian_nh, 2)
      + POWER(b.race_asian_nh + b.race_nhpi_nh, 2)
      + POWER(b.race_other_nh + b.race_multi_nh, 2)
      + POWER(b.race_hispanic, 2)
      )::double precision
      / NULLIF(POWER(g.total_population::double precision, 2), 0.0)
  END AS race_diversity_index,

  CASE WHEN (
    g.total_population < 250
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN %s ELSE 0 END AS flags

FROM viz.diversity_base b
JOIN viz.geoid_base g
  ON g.vintage  = b.vintage
 AND g.sumlevel = b.sumlevel
 AND g.geoid    = b.geoid
WHERE b.vintage = %s;
"""

QUERY_UPDATE_CREATE = """
CREATE TEMP TABLE tmp_diversity_derived (
  vintage                    text NOT NULL,
  sumlevel                   integer NOT NULL,
  geoid                      text NOT NULL,

  -- Derived/simulated values
  race_diversity_index_lo90  double precision,
  race_diversity_index_hi90  double precision
) ON COMMIT DROP;
"""

QUERY_UPDATE_FROM_CSV = (
  "COPY tmp_diversity_derived FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');"
)

QUERY_UPDATE_TO_DB = """
UPDATE viz.diversity_derived d
SET
  race_diversity_index_lo90 = t.race_diversity_index_lo90,
  race_diversity_index_hi90 = t.race_diversity_index_hi90
FROM tmp_diversity_derived t
WHERE d.vintage  = t.vintage
  AND d.sumlevel = t.sumlevel
  AND d.geoid    = t.geoid
  AND (d.flags & %s) = 0;
"""

VINTAGE = 'acs2024_5yr'
CHUNK_SIZE = 10_000
ACS_TABLE_CODE = 'B03002'

SLOT_ORDERS = [
  {3},
  {4},
  {5},
  {6, 7},
  {8, 9},
  {12},
]


VRE_SUMLVLS = [
  '010',  # Nation
  '040',  # State
  '050',  # County
  '060',  # County Subdivision
  '140',  # Tract
  '150',  # Block Group
  '160',  # Place
  '310',  # MSA
  '500',  # Congressional District
  '860',  # Zip Code
]

FLAG_POP_TOO_SMALL = 8  # population < 250, households < 100, household size >= 6


# GEOIDs have to be adjusted for some reason
def normalize_geoid(raw: str) -> str:
  first_three_chars = raw[:3]

  match first_three_chars:
    case '310':
      return raw.replace('310M700US', '31000US')
    case '500':
      return raw.replace('5001900US', '50000US')
    case '860':
      return raw.replace('860Z200US', '86000US')
    case _:
      return raw.replace('00000US', '000US', 1)


def compute_for_group(rows):
  slot_ests = np.zeros(len(SLOT_ORDERS), dtype=np.float64)
  slot_reps = np.zeros((len(SLOT_ORDERS), 80), dtype=np.float64)

  for row in rows:
    order = int(row['ORDER'])
    for i, slot in enumerate(SLOT_ORDERS):
      if order in slot:
        slot_ests[i] += float(row['ESTIMATE'])
        slot_reps[i] += np.array(
          [float(row[f'Var_Rep{j}']) for j in range(1, 81)], dtype=np.float64
        )
        break

  total = slot_ests.sum()
  if total <= 0:
    return None

  p = slot_ests / total
  simpson = 1.0 - np.sum(p**2)

  total_r = slot_reps.sum(axis=0)  # shape (80,)
  valid = total_r > 0
  if valid.any():
    p_r = slot_reps[:, valid] / total_r[valid]  # shape (6, n_valid)
    simpson_r = 1.0 - np.sum(p_r**2, axis=0)  # shape (n_valid,)
    factor = 4.0 / valid.sum()
    se = np.sqrt(factor * np.sum((simpson_r - simpson) ** 2))
    moe = 1.645 * se
    return (simpson - moe, simpson + moe)

  return (None, None)


def stream_records(path: Path):
  # load VRE file for given sumlevel
  with open(path, newline='', encoding='latin1') as f:
    r = csv.DictReader(f)

    # bypass the two label rows
    next(r)
    next(r)

    for geoid, group in itertools.groupby(r, key=lambda row: row['GEOID']):
      yield geoid, compute_for_group(group)


def write_vre_batch(write_conn, out_df: pd.DataFrame) -> None:
  with write_conn.cursor() as cur:
    cur.execute(QUERY_UPDATE_CREATE)

    buf = io.StringIO()
    out_df.to_csv(buf, index=False, na_rep='')  # blanks for NULL
    buf.seek(0)

    with cur.copy(QUERY_UPDATE_FROM_CSV) as cp:
      cp.write(buf.getvalue())

    # send temp rows to DB
    cur.execute(QUERY_UPDATE_TO_DB, (FLAG_POP_TOO_SMALL,))

  write_conn.commit()


def main() -> None:
  # Load .env from project root
  load_dotenv()

  db_url = os.getenv('DATABASE_URL')
  if not db_url:
    raise RuntimeError('DATABASE_URL not set.')

  # Write diversity index and error bars to sql
  with psycopg.connect(db_url) as write_conn:
    # Setup on write conn
    with write_conn.cursor() as cur:
      cur.execute(QUERY_CREATE_TABLE)
      cur.execute(QUERY_RESET_TABLE, (VINTAGE,))
      cur.execute(QUERY_POPULATE_TABLE_WITH_FLAGS, (FLAG_POP_TOO_SMALL, VINTAGE))

      rows_buf = []

      # load VRE tables
      vre_base_path = Path(__file__).parent.parent / 'vre' / ACS_TABLE_CODE

      for sumlevel in VRE_SUMLVLS:
        path = vre_base_path / f'{sumlevel}.csv'
        pbar = tqdm(desc=f'Processing VREs for sumlevel {sumlevel}')

        for geoid_raw, res in stream_records(path):
          pbar.update(1)

          if res is None:
            continue

          div_lo, div_hi = res
          geoid = normalize_geoid(geoid_raw)

          rows_buf.append((VINTAGE, sumlevel, geoid, div_lo, div_hi))

          if len(rows_buf) >= CHUNK_SIZE:
            out_df = pd.DataFrame(
              rows_buf,
              columns=[
                'vintage',
                'sumlevel',
                'geoid',
                'race_diversity_index_lo90',
                'race_diversity_index_hi90',
              ],
            )
            write_vre_batch(write_conn, out_df)
            rows_buf.clear()

      # final flush
      if rows_buf:
        out_df = pd.DataFrame(
          rows_buf,
          columns=[
            'vintage',
            'sumlevel',
            'geoid',
            'race_diversity_index_lo90',
            'race_diversity_index_hi90',
          ],
        )
        write_vre_batch(write_conn, out_df)

      write_conn.commit()


if __name__ == '__main__':
  main()
