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
CREATE TABLE IF NOT EXISTS viz.occupation_derived (
  vintage                         text NOT NULL,
  sumlevel                        integer NOT NULL,
  geoid                           text NOT NULL,

  -- Derived/simulated values
  occ_occupation_index            double precision,
  occ_occupation_index_lo90       double precision,
  occ_occupation_index_hi90       double precision,
  occ_occupation_index_ext        double precision,
  occ_occupation_index_ext_lo90   double precision,
  occ_occupation_index_ext_hi90   double precision,
  occ_occupation_index_ratio      double precision,
  occ_occupation_index_ratio_lo90 double precision,
  occ_occupation_index_ratio_hi90 double precision,

  -- Used for data quality purposes
  flags                           integer NOT NULL default 0,

  PRIMARY KEY (vintage, sumlevel, geoid)
);
"""

QUERY_RESET_TABLE = 'DELETE FROM viz.occupation_derived WHERE vintage = %s;'

QUERY_POPULATE_TABLE_WITH_FLAGS = """
INSERT INTO viz.occupation_derived (
  vintage, sumlevel, geoid,
  occ_occupation_index,
  occ_occupation_index_ext,
  occ_occupation_index_ratio,
  flags
)
SELECT
  b.vintage,
  b.sumlevel,
  b.geoid,

  CASE WHEN (
    g.total_population < 250
    OR b.occ_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    POWER(b.occ_population::double precision, 2)
    /
    NULLIF(
        POWER(b.occ_root_professional_m   + b.occ_root_professional_f,   2)
      + POWER(b.occ_root_service_m        + b.occ_root_service_f,        2)
      + POWER(b.occ_root_sales_m          + b.occ_root_sales_f,          2)
      + POWER(b.occ_root_skilled_trades_m + b.occ_root_skilled_trades_f, 2)
      + POWER(b.occ_root_blue_collar_m    + b.occ_root_blue_collar_f,    2),
      0.0
    )
  END AS occ_occupation_index,

  CASE WHEN (
    g.total_population < 250
    OR b.occ_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    POWER(b.occ_population::double precision, 2)
    /
    NULLIF(
        POWER(b.occ_leaf_mgmt_m         + b.occ_leaf_mgmt_f,         2)
      + POWER(b.occ_leaf_bus_fin_m      + b.occ_leaf_bus_fin_f,      2)
      + POWER(b.occ_leaf_comp_math_m    + b.occ_leaf_comp_math_f,    2)
      + POWER(b.occ_leaf_eng_m          + b.occ_leaf_eng_f,          2)
      + POWER(b.occ_leaf_sci_m          + b.occ_leaf_sci_f,          2)
      + POWER(b.occ_leaf_soc_svc_m      + b.occ_leaf_soc_svc_f,      2)
      + POWER(b.occ_leaf_legal_m        + b.occ_leaf_legal_f,        2)
      + POWER(b.occ_leaf_edu_m          + b.occ_leaf_edu_f,          2)
      + POWER(b.occ_leaf_ent_m          + b.occ_leaf_ent_f,          2)
      + POWER(b.occ_leaf_med_prac_m     + b.occ_leaf_med_prac_f,     2)
      + POWER(b.occ_leaf_med_tech_m     + b.occ_leaf_med_tech_f,     2)
      + POWER(b.occ_leaf_med_svc_m      + b.occ_leaf_med_svc_f,      2)
      + POWER(b.occ_leaf_fire_m         + b.occ_leaf_fire_f,         2)
      + POWER(b.occ_leaf_leo_m          + b.occ_leaf_leo_f,          2)
      + POWER(b.occ_leaf_food_m         + b.occ_leaf_food_f,         2)
      + POWER(b.occ_leaf_custod_maint_m + b.occ_leaf_custod_maint_f, 2)
      + POWER(b.occ_leaf_persnl_care_m  + b.occ_leaf_persnl_care_f,  2)
      + POWER(b.occ_leaf_sales_m        + b.occ_leaf_sales_f,        2)
      + POWER(b.occ_leaf_office_supt_m  + b.occ_leaf_office_supt_f,  2)
      + POWER(b.occ_leaf_ag_m           + b.occ_leaf_ag_f,           2)
      + POWER(b.occ_leaf_const_m        + b.occ_leaf_const_f,        2)
      + POWER(b.occ_leaf_maint_m        + b.occ_leaf_maint_f,        2)
      + POWER(b.occ_leaf_prod_m         + b.occ_leaf_prod_f,         2)
      + POWER(b.occ_leaf_trans_m        + b.occ_leaf_trans_f,        2)
      + POWER(b.occ_leaf_mat_mov_m      + b.occ_leaf_mat_mov_f,      2),
      0.0
    )
  END AS occ_occupation_index_ext,

  CASE WHEN (
    g.total_population < 250
    OR b.occ_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN NULL
  ELSE
    NULLIF(
        POWER(b.occ_root_professional_m   + b.occ_root_professional_f,   2)
      + POWER(b.occ_root_service_m        + b.occ_root_service_f,        2)
      + POWER(b.occ_root_sales_m          + b.occ_root_sales_f,          2)
      + POWER(b.occ_root_skilled_trades_m + b.occ_root_skilled_trades_f, 2)
      + POWER(b.occ_root_blue_collar_m    + b.occ_root_blue_collar_f,    2),
      0.0
    )
    /
    NULLIF(
        POWER(b.occ_leaf_mgmt_m         + b.occ_leaf_mgmt_f,         2)
      + POWER(b.occ_leaf_bus_fin_m      + b.occ_leaf_bus_fin_f,      2)
      + POWER(b.occ_leaf_comp_math_m    + b.occ_leaf_comp_math_f,    2)
      + POWER(b.occ_leaf_eng_m          + b.occ_leaf_eng_f,          2)
      + POWER(b.occ_leaf_sci_m          + b.occ_leaf_sci_f,          2)
      + POWER(b.occ_leaf_soc_svc_m      + b.occ_leaf_soc_svc_f,      2)
      + POWER(b.occ_leaf_legal_m        + b.occ_leaf_legal_f,        2)
      + POWER(b.occ_leaf_edu_m          + b.occ_leaf_edu_f,          2)
      + POWER(b.occ_leaf_ent_m          + b.occ_leaf_ent_f,          2)
      + POWER(b.occ_leaf_med_prac_m     + b.occ_leaf_med_prac_f,     2)
      + POWER(b.occ_leaf_med_tech_m     + b.occ_leaf_med_tech_f,     2)
      + POWER(b.occ_leaf_med_svc_m      + b.occ_leaf_med_svc_f,      2)
      + POWER(b.occ_leaf_fire_m         + b.occ_leaf_fire_f,         2)
      + POWER(b.occ_leaf_leo_m          + b.occ_leaf_leo_f,          2)
      + POWER(b.occ_leaf_food_m         + b.occ_leaf_food_f,         2)
      + POWER(b.occ_leaf_custod_maint_m + b.occ_leaf_custod_maint_f, 2)
      + POWER(b.occ_leaf_persnl_care_m  + b.occ_leaf_persnl_care_f,  2)
      + POWER(b.occ_leaf_sales_m        + b.occ_leaf_sales_f,        2)
      + POWER(b.occ_leaf_office_supt_m  + b.occ_leaf_office_supt_f,  2)
      + POWER(b.occ_leaf_ag_m           + b.occ_leaf_ag_f,           2)
      + POWER(b.occ_leaf_const_m        + b.occ_leaf_const_f,        2)
      + POWER(b.occ_leaf_maint_m        + b.occ_leaf_maint_f,        2)
      + POWER(b.occ_leaf_prod_m         + b.occ_leaf_prod_f,         2)
      + POWER(b.occ_leaf_trans_m        + b.occ_leaf_trans_f,        2)
      + POWER(b.occ_leaf_mat_mov_m      + b.occ_leaf_mat_mov_f,      2),
      0.0
    )
  END AS occ_occupation_index_ratio,

  CASE WHEN (
    g.total_population < 250
    OR b.occ_population < 200
    OR g.total_households < 100
    OR g.household_size >= 6
  )
  THEN %s ELSE 0 END AS flags

FROM viz.occupation_base b
JOIN viz.geoid_base g
  ON g.vintage  = b.vintage
 AND g.sumlevel = b.sumlevel
 AND g.geoid    = b.geoid
WHERE b.vintage = %s;
"""

QUERY_UPDATE_CREATE = """
CREATE TEMP TABLE tmp_occupation_derived (
  vintage                         text NOT NULL,
  sumlevel                        integer NOT NULL,
  geoid                           text NOT NULL,

  -- Derived/simulated values
  occ_occupation_index_lo90       double precision,
  occ_occupation_index_hi90       double precision,
  occ_occupation_index_ext_lo90   double precision,
  occ_occupation_index_ext_hi90   double precision,
  occ_occupation_index_ratio_lo90 double precision,
  occ_occupation_index_ratio_hi90 double precision
) ON COMMIT DROP;
"""

QUERY_UPDATE_FROM_CSV = (
  "COPY tmp_occupation_derived FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');"
)

QUERY_UPDATE_TO_DB = """
UPDATE viz.occupation_derived d
SET
  occ_occupation_index_lo90       = t.occ_occupation_index_lo90,
  occ_occupation_index_hi90       = t.occ_occupation_index_hi90,
  occ_occupation_index_ext_lo90   = t.occ_occupation_index_ext_lo90,
  occ_occupation_index_ext_hi90   = t.occ_occupation_index_ext_hi90,
  occ_occupation_index_ratio_lo90 = t.occ_occupation_index_ratio_lo90,
  occ_occupation_index_ratio_hi90 = t.occ_occupation_index_ratio_hi90
FROM tmp_occupation_derived t
WHERE d.vintage  = t.vintage
  AND d.sumlevel = t.sumlevel
  AND d.geoid    = t.geoid
  AND (d.flags & %s) = 0;
"""

VINTAGE = 'acs2024_5yr'
CHUNK_SIZE = 10_000
ACS_TABLE_CODE = 'C24010'

SLOT_ORDERS = [
  [{3, 39}, {19, 55}, {27, 63}, {30, 66}, {34, 70}],
  [
    {5, 41},
    {6, 42},
    {8, 44},
    {9, 45},
    {10, 46},
    {12, 48},
    {13, 49},
    {14, 50},
    {15, 51},
    {17, 53},
    {18, 54},
    {20, 56},
    {22, 58},
    {23, 59},
    {24, 60},
    {25, 61},
    {26, 62},
    {28, 64},
    {29, 65},
    {31, 67},
    {32, 68},
    {33, 69},
    {35, 71},
    {36, 72},
    {37, 73},
  ],
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

FLAG_POP_TOO_SMALL = (
  8  # population < 250, employed population over 16 < 200, households < 100, household size >= 6
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


def _compute_hill_for_slots(rows, slots):
  slot_ests = np.zeros(len(slots), dtype=np.float64)
  slot_reps = np.zeros((len(slots), 80), dtype=np.float64)

  for row in rows:
    order = int(row['ORDER'])
    for i, slot in enumerate(slots):
      if order in slot:
        slot_ests[i] += float(row['ESTIMATE'])
        slot_reps[i] += np.array(
          [float(row[f'Var_Rep{j}']) for j in range(1, 81)], dtype=np.float64
        )
        break

  total = slot_ests.sum()
  if total <= 0:
    return (None, None, None)

  p = slot_ests / total
  concentration = np.sum(p**2)
  hill = 1.0 / concentration

  total_r = slot_reps.sum(axis=0)
  valid = total_r > 0
  if not valid.any():
    return (hill, None, None)

  p_r = slot_reps[:, valid] / total_r[valid]
  concentration_r = np.sum(p_r**2, axis=0)
  hill_r = 1.0 / concentration_r

  factor = 4.0 / valid.sum()
  se_hill = np.sqrt(factor * np.sum((hill_r - hill) ** 2))
  moe_hill = 1.645 * se_hill

  hill_lo = max(hill - moe_hill, 1.0)
  hill_hi = min(hill + moe_hill, float(len(slots)))
  return (hill, hill_lo, hill_hi)


def compute_for_group(slot_container, rows):
  rows = list(rows)
  if len(slot_container) != 2:
    return None

  base_hill, base_lo, base_hi = _compute_hill_for_slots(rows, slot_container[0])
  ext_hill, ext_lo, ext_hi = _compute_hill_for_slots(rows, slot_container[1])

  if (
    base_hill is None
    or ext_hill is None
    or base_lo is None
    or base_hi is None
    or ext_lo is None
    or ext_hi is None
  ):
    return (base_lo, base_hi, ext_lo, ext_hi, None, None)

  ratio = ext_hill / base_hill
  ratio_lo = None
  ratio_hi = None

  # Recompute replicate Hill arrays only for ratio CI so covariance is preserved.
  base_slot_reps = np.zeros((len(slot_container[0]), 80), dtype=np.float64)
  ext_slot_reps = np.zeros((len(slot_container[1]), 80), dtype=np.float64)

  for row in rows:
    order = int(row['ORDER'])
    reps = np.array([float(row[f'Var_Rep{j}']) for j in range(1, 81)], dtype=np.float64)

    for i, slot in enumerate(slot_container[0]):
      if order in slot:
        base_slot_reps[i] += reps
        break

    for i, slot in enumerate(slot_container[1]):
      if order in slot:
        ext_slot_reps[i] += reps
        break

  base_total_r = base_slot_reps.sum(axis=0)
  ext_total_r = ext_slot_reps.sum(axis=0)
  valid_ratio = (base_total_r > 0) & (ext_total_r > 0)

  if valid_ratio.any():
    base_p_r = base_slot_reps[:, valid_ratio] / base_total_r[valid_ratio]
    ext_p_r = ext_slot_reps[:, valid_ratio] / ext_total_r[valid_ratio]
    base_hill_r = 1.0 / np.sum(base_p_r**2, axis=0)
    ext_hill_r = 1.0 / np.sum(ext_p_r**2, axis=0)
    ratio_r = ext_hill_r / base_hill_r

    factor = 4.0 / valid_ratio.sum()
    se_ratio = np.sqrt(factor * np.sum((ratio_r - ratio) ** 2))
    moe_ratio = 1.645 * se_ratio
    ratio_floor = 1.0
    ratio_ceil = float(len(slot_container[1]))
    ratio_lo = max(ratio - moe_ratio, ratio_floor)
    ratio_hi = min(ratio + moe_ratio, ratio_ceil)

  return (base_lo, base_hi, ext_lo, ext_hi, ratio_lo, ratio_hi)


def stream_records(path: Path):
  # load VRE file for given sumlevel
  with open(path, newline='', encoding='latin1') as f:
    r = csv.DictReader(f)

    # bypass the two label rows
    next(r)
    next(r)

    for geoid, group in itertools.groupby(r, key=lambda row: row['GEOID']):
      yield geoid, compute_for_group(SLOT_ORDERS, group)


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

  # Write occupation index and error bars to sql
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

          if res is None or len(res) != 6:
            continue

          occ_lo, occ_hi, occ_ext_lo, occ_ext_hi, occ_ratio_lo, occ_ratio_hi = res
          geoid = normalize_geoid(geoid_raw)

          rows_buf.append(
            (
              VINTAGE,
              sumlevel,
              geoid,
              occ_lo,
              occ_hi,
              occ_ext_lo,
              occ_ext_hi,
              occ_ratio_lo,
              occ_ratio_hi,
            )
          )

          if len(rows_buf) >= CHUNK_SIZE:
            out_df = pd.DataFrame(
              rows_buf,
              columns=[
                'vintage',
                'sumlevel',
                'geoid',
                'occ_occupation_index_lo90',
                'occ_occupation_index_hi90',
                'occ_occupation_index_ext_lo90',
                'occ_occupation_index_ext_hi90',
                'occ_occupation_index_ratio_lo90',
                'occ_occupation_index_ratio_hi90',
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
            'occ_occupation_index_lo90',
            'occ_occupation_index_hi90',
            'occ_occupation_index_ext_lo90',
            'occ_occupation_index_ext_hi90',
            'occ_occupation_index_ratio_lo90',
            'occ_occupation_index_ratio_hi90',
          ],
        )
        write_vre_batch(write_conn, out_df)

      write_conn.commit()


if __name__ == '__main__':
  main()
