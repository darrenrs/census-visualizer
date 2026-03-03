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
CREATE TABLE IF NOT EXISTS viz.education_derived (
  vintage                   text NOT NULL,
  sumlevel                  integer NOT NULL,
  geoid                     text NOT NULL,

  -- Derived/simulated values
  edu_education_index       double precision,
  edu_education_index_lo90  double precision,
  edu_education_index_hi90  double precision,
  edu_years_of_school       double precision,
  edu_years_of_school_lo90  double precision,
  edu_years_of_school_hi90  double precision,

  -- Used for data quality purposes
  flags                     integer NOT NULL default 0,

  PRIMARY KEY (vintage, sumlevel, geoid)
);
"""

QUERY_RESET_TABLE = 'DELETE FROM viz.education_derived WHERE vintage = %s;'

QUERY_POPULATE_TABLE_WITH_FLAGS = """
INSERT INTO viz.education_derived (
  vintage, sumlevel, geoid,
  edu_education_index,
  edu_years_of_school,
  flags
)
SELECT
  b.vintage,
  b.sumlevel,
  b.geoid,

  CASE WHEN (
    g.total_population < 250
    OR b.edu_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    (
      0.00 * (b.edu_no_schooling_m + b.edu_no_schooling_f) +
      0.10 * (b.edu_grade_0_4_m + b.edu_grade_0_4_f) +
      0.20 * (b.edu_grade_5_6_m + b.edu_grade_5_6_f) +
      0.30 * (b.edu_grade_7_8_m + b.edu_grade_7_8_f) +
      0.40 * (b.edu_grade_9_m + b.edu_grade_9_f) +
      0.50 * (b.edu_grade_10_m + b.edu_grade_10_f) +
      0.60 * (b.edu_grade_11_m + b.edu_grade_11_f) +
      0.80 * (b.edu_grade_12_m + b.edu_grade_12_f) +
      1.00 * (b.edu_high_school_m + b.edu_high_school_f) +
      1.25 * (b.edu_some_college_less_than_1_year_m + b.edu_some_college_less_than_1_year_f) +
      1.50 * (b.edu_some_college_no_degree_m + b.edu_some_college_no_degree_f) +
      1.75 * (b.edu_associate_degree_m + b.edu_associate_degree_f) +
      2.00 * (b.edu_bachelors_degree_m + b.edu_bachelors_degree_f) +
      2.50 * (b.edu_masters_degree_m + b.edu_masters_degree_f) +
      2.75 * (b.edu_professional_degree_m + b.edu_professional_degree_f) +
      3.00 * (b.edu_doctorate_degree_m + b.edu_doctorate_degree_f)
    )::double precision / NULLIF(b.edu_population::double precision, 0.0)
  END AS edu_education_index,

  CASE WHEN (
    g.total_population < 250
    OR b.edu_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    (
       0.0 * (b.edu_no_schooling_m + b.edu_no_schooling_f) +
       2.0 * (b.edu_grade_0_4_m + b.edu_grade_0_4_f) +
       5.5 * (b.edu_grade_5_6_m + b.edu_grade_5_6_f) +
       7.5 * (b.edu_grade_7_8_m + b.edu_grade_7_8_f) +
       9.0 * (b.edu_grade_9_m + b.edu_grade_9_f) +
      10.0 * (b.edu_grade_10_m + b.edu_grade_10_f) +
      11.0 * (b.edu_grade_11_m + b.edu_grade_11_f) +
      11.5 * (b.edu_grade_12_m + b.edu_grade_12_f) +
      12.0 * (b.edu_high_school_m + b.edu_high_school_f) +
      12.5 * (b.edu_some_college_less_than_1_year_m + b.edu_some_college_less_than_1_year_f) +
      13.5 * (b.edu_some_college_no_degree_m + b.edu_some_college_no_degree_f) +
      14.0 * (b.edu_associate_degree_m + b.edu_associate_degree_f) +
      16.0 * (b.edu_bachelors_degree_m + b.edu_bachelors_degree_f) +
      18.0 * (b.edu_masters_degree_m + b.edu_masters_degree_f) +
      20.0 * (b.edu_professional_degree_m + b.edu_professional_degree_f) +
      22.0 * (b.edu_doctorate_degree_m + b.edu_doctorate_degree_f)
    )::double precision / NULLIF(b.edu_population::double precision, 0.0)
  END AS edu_years_of_school,

  CASE WHEN (
    g.total_population < 250
    OR b.edu_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN %s ELSE 0 END AS flags

FROM viz.education_base b
JOIN viz.geoid_base g
  ON g.vintage  = b.vintage
 AND g.sumlevel = b.sumlevel
 AND g.geoid    = b.geoid
WHERE b.vintage = %s;
"""

QUERY_UPDATE_CREATE = """
CREATE TEMP TABLE tmp_education_derived (
  vintage                   text NOT NULL,
  sumlevel                  integer NOT NULL,
  geoid                     text NOT NULL,

  -- Derived/simulated values
  edu_education_index_lo90  double precision,
  edu_education_index_hi90  double precision,
  edu_years_of_school_lo90  double precision,
  edu_years_of_school_hi90  double precision
) ON COMMIT DROP;
"""

QUERY_UPDATE_FROM_CSV = (
  "COPY tmp_education_derived FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');"
)

QUERY_UPDATE_TO_DB = """
UPDATE viz.education_derived d
SET
  edu_education_index_lo90 = t.edu_education_index_lo90,
  edu_education_index_hi90 = t.edu_education_index_hi90,
  edu_years_of_school_lo90 = t.edu_years_of_school_lo90,
  edu_years_of_school_hi90 = t.edu_years_of_school_hi90
FROM tmp_education_derived t
WHERE d.vintage  = t.vintage
  AND d.sumlevel = t.sumlevel
  AND d.geoid    = t.geoid
  AND (d.flags & %s) = 0;
"""

VINTAGE = 'acs2024_5yr'
CHUNK_SIZE = 10_000
ACS_TABLE_CODE = 'B15002'

# fmt: off
WEIGHTS_EI = {
  3: 0.00, 4: 0.10, 5: 0.20, 6: 0.30, 7: 0.40, 8: 0.50, 9: 0.60, 10: 0.80, #
  11: 1.00, 12: 1.25, 13: 1.50, 14: 1.75, 15: 2.00, 16: 2.50, 17: 2.75, 18: 3.00, #
  20: 0.00, 21: 0.10, 22: 0.20, 23: 0.30, 24: 0.40, 25: 0.50, 26: 0.60, 27: 0.80, #
  28: 1.00, 29: 1.25, 30: 1.50, 31: 1.75, 32: 2.00, 33: 2.50, 34: 2.75, 35: 3.00, #
}
WEIGHTS_YOS = {
  3: 0.0, 4: 2.0, 5: 5.5, 6: 7.5, 7: 9.0, 8: 10.0, 9: 11.0, 10: 11.5,
  11: 12.0, 12: 12.5, 13: 13.5, 14: 14.0, 15: 16.0, 16: 18.0, 17: 20.0, 18: 22.0,
  20: 0.0, 21: 2.0, 22: 5.5, 23: 7.5, 24: 9.0, 25: 10.0, 26: 11.0, 27: 11.5,
  28: 12.0, 29: 12.5, 30: 13.5, 31: 14.0, 32: 16.0, 33: 18.0, 34: 20.0, 35: 22.0,
}
# fmt: on

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

FLAG_POP_TOO_SMALL = (
  8  # population < 250, population 25 or older < 200, households < 100, household size >= 6
)


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
  denom = 0.0
  num_ei = 0.0
  num_yos = 0.0

  denom_r = np.zeros(80, dtype=np.float64)
  num_ei_r = np.zeros(80, dtype=np.float64)
  num_yos_r = np.zeros(80, dtype=np.float64)

  for row in rows:
    order = int(row['ORDER'])
    if order not in WEIGHTS_EI:
      continue  # skip totals + Male/Female header rows

    est = float(row['ESTIMATE'])
    reps = np.array(
      [float(row[c]) for c in [f'Var_Rep{i}' for i in range(1, 81)]], dtype=np.float64
    )

    w_ei = WEIGHTS_EI[order]
    w_yos = WEIGHTS_YOS[order]

    denom += est
    num_ei += w_ei * est
    num_yos += w_yos * est

    denom_r += reps
    num_ei_r += w_ei * reps
    num_yos_r += w_yos * reps

  if denom <= 0:
    return None  # or emit NULLs + acc=0

  ei = num_ei / denom
  yos = num_yos / denom

  valid = denom_r > 0

  if valid.any():
    ei_r = num_ei_r[valid] / denom_r[valid]
    yos_r = num_yos_r[valid] / denom_r[valid]

    # use count-adjusted SDR factor if some reps invalid
    factor = 4.0 / valid.sum()

    se_ei = np.sqrt(factor * np.sum((ei_r - ei) ** 2))
    se_yos = np.sqrt(factor * np.sum((yos_r - yos) ** 2))

    moe_ei = 1.645 * se_ei
    moe_yos = 1.645 * se_yos

    return (ei - moe_ei, ei + moe_ei, yos - moe_yos, yos + moe_yos)

  return (None, None, None, None)


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

  # Write education index, years of schooling, and error bars to sql
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

          ei_lo, ei_hi, yos_lo, yos_hi = res
          geoid = normalize_geoid(geoid_raw)

          rows_buf.append((VINTAGE, sumlevel, geoid, ei_lo, ei_hi, yos_lo, yos_hi))

          if len(rows_buf) >= CHUNK_SIZE:
            out_df = pd.DataFrame(
              rows_buf,
              columns=[
                'vintage',
                'sumlevel',
                'geoid',
                'edu_education_index_lo90',
                'edu_education_index_hi90',
                'edu_years_of_school_lo90',
                'edu_years_of_school_hi90',
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
            'edu_education_index_lo90',
            'edu_education_index_hi90',
            'edu_years_of_school_lo90',
            'edu_years_of_school_hi90',
          ],
        )
        write_vre_batch(write_conn, out_df)

      write_conn.commit()


if __name__ == '__main__':
  main()
