import io
import os

import numpy as np
import pandas as pd
import psycopg
from dotenv import load_dotenv

QUERY_DROP_TABLE = 'DROP TABLE IF EXISTS viz.income_derived;'
QUERY_CREATE_TABLE = """
CREATE TABLE viz.income_derived (
  vintage           text NOT NULL,
  sumlevel          integer NOT NULL,
  geoid             text NOT NULL,

  -- Derived/simulated values
  hhi_sim_p90       bigint,
  hhi_sim_p90_lo90  bigint,
  hhi_sim_p90_hi90  bigint,
  hhi_sim_p95       bigint,
  hhi_sim_p95_lo90  bigint,
  hhi_sim_p95_hi90  bigint,
  hhi_sim_p99       bigint,
  hhi_sim_p99_lo90  bigint,
  hhi_sim_p99_hi90  bigint,
  hhi_sim_p999      bigint,
  hhi_sim_p999_lo90 bigint,
  hhi_sim_p999_hi90 bigint,

  -- Used for data quality purposes
  hhi_sim_anchor    integer,
  hhi_sim_acc       double precision,
  flags             integer NOT NULL default 0,

  PRIMARY KEY (vintage, sumlevel, geoid)
);
"""

QUERY_POPULATE_TABLE = """
INSERT INTO viz.income_derived (vintage, sumlevel, geoid)
SELECT vintage, sumlevel, geoid
FROM viz.income_base
WHERE vintage = %s
ON CONFLICT (vintage, sumlevel, geoid) DO NOTHING;
"""

QUERY_FLAG_GEOGRAPHIES_TOO_SMALL = """
UPDATE viz.income_derived d
SET flags = (d.flags | %s)
FROM viz.geoid_base g
WHERE d.vintage = %s
  AND d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
  AND (
    g.total_population < 250
    OR g.total_households < 100
    OR g.household_size >= 6
  );
"""

QUERY_FLAG_BLOCK_GROUPS = """
UPDATE viz.income_derived d
SET flags = (d.flags | %s)
FROM viz.geoid_base g
WHERE d.vintage = %s
  AND d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
  AND d.sumlevel = 150;
"""

QUERY_FETCH_ALL = """
SELECT
  d.vintage, d.sumlevel, d.geoid, d.flags,

  b.hhi_p95, b.hhi_p95_se,
  b.hhi_p80, b.hhi_p80_se,
  b.hhi_p60, b.hhi_p60_se,
  b.hhi_p40, b.hhi_p40_se,
  b.hhi_p20, b.hhi_p20_se,

  b.hhi_top5_mean, b.hhi_top5_mean_se,
  b.hhi_q5_mean, b.hhi_q5_mean_se,
  b.hhi_q4_mean, b.hhi_q4_mean_se,
  b.hhi_q3_mean, b.hhi_q3_mean_se,
  b.hhi_q2_mean, b.hhi_q2_mean_se

FROM viz.income_derived d
JOIN viz.income_base b
  ON b.vintage=d.vintage AND b.sumlevel=d.sumlevel AND b.geoid=d.geoid
WHERE (d.flags & %s) = 0
  AND d.vintage = %s
  AND d.sumlevel != 150;
"""

VINTAGE = 'acs2024_5yr'
CHUNK_SIZE = 10_000
SIM_COUNT = 1000

TOPCODE = 250_001
BOTTOMCODE = 2499

QS_SIM = [0.90, 0.95, 0.99, 0.999]
P0_ORDER = [0.95, 0.80, 0.60, 0.40, 0.20]
LOW_ACC_THRESH = 0.80

FLAG_LOWER_ANCHOR_WARN = 1  # used anchor < 0.95 for any >= p95 simulation
FLAG_LOW_ACC_WARN = 2  # accuracy below threshold
FLAG_NOT_COMPUTABLE_BG = 4  # block groups have no income data past median
FLAG_POP_TOO_SMALL = 8  # population < 250, households < 100, household size >= 6
FLAG_MISSING_DATA = 16  # thresholds exist but required mean(s) missing
FLAG_ALL_TOPCODED = 32  # p20 and up are all topcoded, no curve possible


def simulate_pareto_chunk(
  T: np.ndarray,
  T_se: np.ndarray,
  mu: np.ndarray,
  mu_se: np.ndarray,
  p0: float,
  qs: list[float],
  rng: np.random.Generator,
  sim_count: int,
) -> dict:
  """
  Inputs are 1D arrays of length m (rows in this anchor group).
  Returns dict[q] = (est, lo, hi, acc, n_valid) each length m.
  """
  m = T.shape[0]
  out = {}

  # Draw
  T_s = rng.normal(T, T_se, size=(sim_count, m))
  mu_s = rng.normal(mu, mu_se, size=(sim_count, m))

  valid_draw = np.isfinite(T_s) & np.isfinite(mu_s) & (T_s > 0) & (mu_s > 0) & (mu_s > T_s)

  acc = valid_draw.mean(axis=0)
  n_valid = valid_draw.sum(axis=0)

  # Precompute pieces for log-space computation
  logT = np.full_like(T_s, np.nan, dtype='float64')
  logT[valid_draw] = np.log(T_s[valid_draw])

  # e = (mu_s - T_s) / mu_s  in (0,1) for valid draws
  e = np.full_like(T_s, np.nan, dtype='float64')
  e[valid_draw] = (mu_s[valid_draw] - T_s[valid_draw]) / mu_s[valid_draw]

  for q in qs:
    if q < p0:
      continue
    R = (1.0 - p0) / (1.0 - q)
    logQ = logT + e * np.log(R)

    # clamp exponents within 2^1024 to clear out Infinity
    logQ = np.clip(logQ, -709.78, 709.78)

    sim = np.exp(logQ)  # NaN where invalid_draw

    est = np.nanmedian(sim, axis=0)
    lo = np.nanpercentile(sim, 5, axis=0)
    hi = np.nanpercentile(sim, 95, axis=0)

    out[q] = (est, lo, hi, acc, n_valid)

  return out


# nullable-int conversions that preserve NA
def to_bigint_round(x: np.ndarray) -> pd.Series:
  return pd.Series(pd.array(np.rint(x), dtype='Int64'))


def to_bigint_floor(x: np.ndarray) -> pd.Series:
  return pd.Series(pd.array(np.floor(x), dtype='Int64'))


def to_bigint_ceil(x: np.ndarray) -> pd.Series:
  return pd.Series(pd.array(np.ceil(x), dtype='Int64'))


def main() -> None:
  # Load .env from project root
  load_dotenv()

  db_url = os.getenv('DATABASE_URL')
  if not db_url:
    raise RuntimeError('DATABASE_URL not set.')

  # Two connections: read holds the server-side cursor; write commits per chunk.
  with psycopg.connect(db_url) as read_conn, psycopg.connect(db_url) as write_conn:
    # Setup on write conn
    with write_conn.cursor() as cur:
      cur.execute(QUERY_DROP_TABLE)
      cur.execute(QUERY_CREATE_TABLE)
      cur.execute(QUERY_POPULATE_TABLE, (VINTAGE,))
      cur.execute(
        QUERY_FLAG_BLOCK_GROUPS,
        (
          FLAG_NOT_COMPUTABLE_BG,
          VINTAGE,
        ),
      )
      cur.execute(
        QUERY_FLAG_GEOGRAPHIES_TOO_SMALL,
        (
          FLAG_POP_TOO_SMALL,
          VINTAGE,
        ),
      )
    write_conn.commit()

    # Stream from read conn (NO commits here)
    with read_conn.cursor(name='income_stream') as cur:
      cur.execute(
        QUERY_FETCH_ALL,
        (
          FLAG_POP_TOO_SMALL,
          VINTAGE,
        ),
      )

      # only to make Ruff stop screaming
      if cur.description is None:
        raise RuntimeError('cur.description is None')
      colnames = [d[0] for d in cur.description]

      # create RNG seed
      rng = np.random.default_rng(0)

      while True:
        rows = cur.fetchmany(CHUNK_SIZE)
        if not rows:
          break

        df = pd.DataFrame(rows, columns=colnames)

        n = len(df)
        if n == 0:
          continue

        # extract source arrays
        A = {}
        for c in [
          'hhi_p95',
          'hhi_p95_se',
          'hhi_p80',
          'hhi_p80_se',
          'hhi_p60',
          'hhi_p60_se',
          'hhi_p40',
          'hhi_p40_se',
          'hhi_p20',
          'hhi_p20_se',
          'hhi_top5_mean',
          'hhi_top5_mean_se',
          'hhi_q2_mean',
          'hhi_q2_mean_se',
          'hhi_q3_mean',
          'hhi_q3_mean_se',
          'hhi_q4_mean',
          'hhi_q4_mean_se',
          'hhi_q5_mean',
          'hhi_q5_mean_se',
        ]:
          A[c] = df[c].astype('float64').to_numpy()

        base_flags = df['flags'].astype('int32').to_numpy()
        flags = base_flags.copy()

        # output arrays (estimate, lo90, hi90)
        est = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}
        lo = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}
        hi = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}

        # track which anchor produced each q (95/80/60/40/20 as codes; 0 means missing)
        src = {q: np.zeros(n, dtype=np.uint16) for q in QS_SIM}

        # track accuracy for the anchor that filled each q
        acc_q = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}

        # build and run anchors in priority order
        for p0 in P0_ORDER:
          # Determine T / T_se
          if p0 == 0.95:
            T, T_se = A['hhi_p95'], A['hhi_p95_se']
            mu, mu_se = A['hhi_top5_mean'], A['hhi_top5_mean_se']
            anchor_code = 95
          elif p0 == 0.80:
            T, T_se = A['hhi_p80'], A['hhi_p80_se']
            mu, mu_se = A['hhi_q5_mean'], A['hhi_q5_mean_se']
            anchor_code = 80
          elif p0 == 0.60:
            T, T_se = A['hhi_p60'], A['hhi_p60_se']
            mu = (A['hhi_q4_mean'] + A['hhi_q5_mean']) / 2.0
            mu_se = np.sqrt(A['hhi_q4_mean_se'] ** 2 + A['hhi_q5_mean_se'] ** 2) / 2.0
            anchor_code = 60
          elif p0 == 0.40:
            T, T_se = A['hhi_p40'], A['hhi_p40_se']
            mu = (A['hhi_q3_mean'] + A['hhi_q4_mean'] + A['hhi_q5_mean']) / 3.0
            mu_se = (
              np.sqrt(
                A['hhi_q3_mean_se'] ** 2 + A['hhi_q4_mean_se'] ** 2 + A['hhi_q5_mean_se'] ** 2
              )
              / 3.0
            )
            anchor_code = 40
          else:  # p0 == 0.20
            T, T_se = A['hhi_p20'], A['hhi_p20_se']
            mu = (A['hhi_q2_mean'] + A['hhi_q3_mean'] + A['hhi_q4_mean'] + A['hhi_q5_mean']) / 4.0
            mu_se = (
              np.sqrt(
                A['hhi_q2_mean_se'] ** 2
                + A['hhi_q3_mean_se'] ** 2
                + A['hhi_q4_mean_se'] ** 2
                + A['hhi_q5_mean_se'] ** 2
              )
              / 4.0
            )
            anchor_code = 20

          # choose qs for this simulation; no simulating a lower q than current anchor percentile
          qs_here = [q for q in QS_SIM if q >= p0]
          if not qs_here:
            continue

          # keeps track of which geographies have been simulated
          need_to_simulate = np.zeros(n, dtype=bool)
          for q in qs_here:
            need_to_simulate |= np.isnan(est[q])

          # row-level validity for this anchor
          row_ok = (
            need_to_simulate
            & np.isfinite(T)
            & np.isfinite(T_se)
            & np.isfinite(mu)
            & np.isfinite(mu_se)
            & (T > 0)
            & (T < TOPCODE)
            & (T_se >= 0)
            & (mu_se >= 0)
            & (mu > T)
          )
          rows_idx = np.where(row_ok)[0]
          if rows_idx.size == 0:
            continue

          # simulate for this anchor group
          sim_out = simulate_pareto_chunk(
            T=T[rows_idx],
            T_se=T_se[rows_idx],
            mu=mu[rows_idx],
            mu_se=mu_se[rows_idx],
            p0=p0,
            qs=qs_here,
            rng=rng,
            sim_count=SIM_COUNT,
          )

          # fill outputs for each q (only where still missing)
          for q in qs_here:
            est_this, lo_this, hi_this, acc_vals_this, _ = sim_out[q]

            missing = np.isnan(est[q][rows_idx]) & np.isfinite(est_this)
            if not np.any(missing):
              continue

            target = rows_idx[missing]
            est[q][target] = est_this[missing]
            lo[q][target] = lo_this[missing]
            hi[q][target] = hi_this[missing]
            src[q][target] = anchor_code
            acc_q[q][target] = acc_vals_this[missing]

        anchor_used = np.zeros(n, dtype=np.uint16)
        for q in QS_SIM:
          anchor_used = np.maximum(anchor_used, src[q])

        # convert 0 -> NA (NULL in DB)
        anchor_used_ser = pd.Series(
          pd.array(np.where(anchor_used == 0, np.nan, anchor_used), dtype='Int64')
        )

        # set flags
        # 32: all top coded (extremely rare)
        p95 = A['hhi_p95']
        p80 = A['hhi_p80']
        p60 = A['hhi_p60']
        p40 = A['hhi_p40']
        p20 = A['hhi_p20']

        all_T_present = (
          np.isfinite(p95)
          & np.isfinite(p80)
          & np.isfinite(p60)
          & np.isfinite(p40)
          & np.isfinite(p20)
        )
        all_coded = (
          all_T_present
          & ((p95 == TOPCODE) | (p95 == BOTTOMCODE))
          & ((p80 == TOPCODE) | (p80 == BOTTOMCODE))
          & ((p60 == TOPCODE) | (p60 == BOTTOMCODE))
          & ((p40 == TOPCODE) | (p40 == BOTTOMCODE))
          & ((p20 == TOPCODE) | (p20 == BOTTOMCODE))
        )
        flags[all_coded] |= FLAG_ALL_TOPCODED

        # 1: used anchor <0.95 for any p95 simulation
        src95 = src[0.95]  # 95/80/60/40/20/0
        used_lower_anchor_for_p95 = (src95 != 0) & (src95 != 95)
        flags[used_lower_anchor_for_p95] |= FLAG_LOWER_ANCHOR_WARN

        # compute per-row "worst" accuracy across the percentiles that actually got filled
        q_labels = np.array(['p90', 'p95', 'p99', 'p999'], dtype=object)

        acc_mat = np.vstack([acc_q[q] for q in QS_SIM])  # shape (4, n)
        acc_for_min = np.where(np.isfinite(acc_mat), acc_mat, np.inf)

        worst_idx = np.argmin(acc_for_min, axis=0)
        worst_acc = np.min(acc_for_min, axis=0)
        has_acc = np.isfinite(worst_acc) & (worst_acc != np.inf)

        sim_q_text = np.full(n, None, dtype=object)  # None -> NULL in DB via CSV blank
        sim_q_text[has_acc] = q_labels[worst_idx[has_acc]]

        sim_accuracy = np.full(n, np.nan, dtype='float64')
        sim_accuracy[has_acc] = worst_acc[has_acc]

        # --- 2: accuracy below threshold (worst-case across filled percentiles) ---
        low_acc = has_acc & (sim_accuracy < LOW_ACC_THRESH)
        flags[low_acc] |= FLAG_LOW_ACC_WARN

        # --- 16: MISSING DATA OF ANY KIND ---
        # Define "computed_any" as: we successfully filled at least one simulated percentile.
        computed_any = np.zeros(n, dtype=bool)
        for q in QS_SIM:
          computed_any |= src[q] != 0

        # Missing data means: nothing computed AND not all-coded.
        # (BG and too-small rows never enter this loop; they already have flags 4/8.)
        missing_data = (~computed_any) & (~all_coded)
        flags[missing_data] |= (
          FLAG_MISSING_DATA  # your 16; consider renaming constant to FLAG_MISSING_DATA
        )

        # NOTE: sim_q_int and sim_accuracy need to be written to SQL; see below.

        # ---- Build out_df (nullable ints -> CSV -> COPY NULL '') ----
        out_df = pd.DataFrame(
          {
            'vintage': df['vintage'],
            'sumlevel': df['sumlevel'],
            'geoid': df['geoid'],
            'hhi_sim_p90': to_bigint_round(est[0.90]),
            'hhi_sim_p90_lo90': to_bigint_floor(lo[0.90]),
            'hhi_sim_p90_hi90': to_bigint_ceil(hi[0.90]),
            'hhi_sim_p95': to_bigint_round(est[0.95]),
            'hhi_sim_p95_lo90': to_bigint_floor(lo[0.95]),
            'hhi_sim_p95_hi90': to_bigint_ceil(hi[0.95]),
            'hhi_sim_p99': to_bigint_round(est[0.99]),
            'hhi_sim_p99_lo90': to_bigint_floor(lo[0.99]),
            'hhi_sim_p99_hi90': to_bigint_ceil(hi[0.99]),
            'hhi_sim_p999': to_bigint_round(est[0.999]),
            'hhi_sim_p999_lo90': to_bigint_floor(lo[0.999]),
            'hhi_sim_p999_hi90': to_bigint_ceil(hi[0.999]),
            'hhi_sim_anchor': anchor_used_ser,
            'hhi_sim_acc': sim_accuracy,
            'flags': flags.astype('int32'),
          }
        )

        with write_conn.cursor() as wcur:
          wcur.execute("""
                    CREATE TEMP TABLE tmp_income_derived (
                      vintage text,
                      sumlevel integer,
                      geoid text,
                      hhi_sim_p90 bigint,
                      hhi_sim_p90_lo90 bigint,
                      hhi_sim_p90_hi90 bigint,
                      hhi_sim_p95 bigint,
                      hhi_sim_p95_lo90 bigint,
                      hhi_sim_p95_hi90 bigint,
                      hhi_sim_p99 bigint,
                      hhi_sim_p99_lo90 bigint,
                      hhi_sim_p99_hi90 bigint,
                      hhi_sim_p999 bigint,
                      hhi_sim_p999_lo90 bigint,
                      hhi_sim_p999_hi90 bigint,
                      hhi_sim_anchor integer,
                      hhi_sim_acc double precision,
                      flags integer
                    ) ON COMMIT DROP;
                """)

          buf = io.StringIO()
          out_df.to_csv(buf, index=False, na_rep='')  # blanks for NULL
          buf.seek(0)

          with wcur.copy(
            "COPY tmp_income_derived FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
          ) as cp:
            cp.write(buf.getvalue())

          # Your same UPSERT (unchanged)...
          wcur.execute("""
            INSERT INTO viz.income_derived AS d (
              vintage, sumlevel, geoid,
              hhi_sim_p90, hhi_sim_p90_lo90, hhi_sim_p90_hi90,
              hhi_sim_p95, hhi_sim_p95_lo90, hhi_sim_p95_hi90,
              hhi_sim_p99, hhi_sim_p99_lo90, hhi_sim_p99_hi90,
              hhi_sim_p999, hhi_sim_p999_lo90, hhi_sim_p999_hi90,
              hhi_sim_anchor, hhi_sim_acc, flags
            )
            SELECT
              vintage, sumlevel, geoid,
              hhi_sim_p90, hhi_sim_p90_lo90, hhi_sim_p90_hi90,
              hhi_sim_p95, hhi_sim_p95_lo90, hhi_sim_p95_hi90,
              hhi_sim_p99, hhi_sim_p99_lo90, hhi_sim_p99_hi90,
              hhi_sim_p999, hhi_sim_p999_lo90, hhi_sim_p999_hi90,
              hhi_sim_anchor, hhi_sim_acc, flags
            FROM tmp_income_derived
            ON CONFLICT (vintage, sumlevel, geoid) DO UPDATE
            SET
              hhi_sim_p90 = EXCLUDED.hhi_sim_p90,
              hhi_sim_p90_lo90 = EXCLUDED.hhi_sim_p90_lo90,
              hhi_sim_p90_hi90 = EXCLUDED.hhi_sim_p90_hi90,

              hhi_sim_p95 = EXCLUDED.hhi_sim_p95,
              hhi_sim_p95_lo90 = EXCLUDED.hhi_sim_p95_lo90,
              hhi_sim_p95_hi90 = EXCLUDED.hhi_sim_p95_hi90,

              hhi_sim_p99 = EXCLUDED.hhi_sim_p99,
              hhi_sim_p99_lo90 = EXCLUDED.hhi_sim_p99_lo90,
              hhi_sim_p99_hi90 = EXCLUDED.hhi_sim_p99_hi90,

              hhi_sim_p999 = EXCLUDED.hhi_sim_p999,
              hhi_sim_p999_lo90 = EXCLUDED.hhi_sim_p999_lo90,
              hhi_sim_p999_hi90 = EXCLUDED.hhi_sim_p999_hi90,

              hhi_sim_anchor = EXCLUDED.hhi_sim_anchor,
              hhi_sim_acc = EXCLUDED.hhi_sim_acc,
              flags = (d.flags | EXCLUDED.flags);
            """)

        write_conn.commit()


if __name__ == '__main__':
  main()
