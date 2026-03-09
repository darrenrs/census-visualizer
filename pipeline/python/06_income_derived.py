import io
import os

import numpy as np
import pandas as pd
import psycopg
from dotenv import load_dotenv
from tqdm import tqdm

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS viz.income_derived (
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

QUERY_RESET_TABLE = 'DELETE FROM viz.income_derived WHERE vintage = %s;'

QUERY_POPULATE_TABLE = """
INSERT INTO viz.income_derived (vintage, sumlevel, geoid)
SELECT vintage, sumlevel, geoid
FROM viz.income_base
WHERE vintage = %s;
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
UPDATE viz.income_derived
SET flags = flags | %s
WHERE vintage = %s
  AND sumlevel = 150;
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
WHERE d.vintage = %s
  AND (d.flags & %s) = 0
  AND d.sumlevel != 150;
"""

QUERY_UPDATE_CREATE = """
CREATE TEMP TABLE tmp_income_derived (
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
  flags             integer NOT NULL default 0
) ON COMMIT DROP;
"""

QUERY_UPDATE_FROM_CSV = (
  "COPY tmp_income_derived FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');"
)

QUERY_UPDATE_TO_DB = """
UPDATE viz.income_derived d
SET
  hhi_sim_p90       = t.hhi_sim_p90,
  hhi_sim_p90_lo90  = t.hhi_sim_p90_lo90,
  hhi_sim_p90_hi90  = t.hhi_sim_p90_hi90,
  hhi_sim_p95       = t.hhi_sim_p95,
  hhi_sim_p95_lo90  = t.hhi_sim_p95_lo90,
  hhi_sim_p95_hi90  = t.hhi_sim_p95_hi90,
  hhi_sim_p99       = t.hhi_sim_p99,
  hhi_sim_p99_lo90  = t.hhi_sim_p99_lo90,
  hhi_sim_p99_hi90  = t.hhi_sim_p99_hi90,
  hhi_sim_p999      = t.hhi_sim_p999,
  hhi_sim_p999_lo90 = t.hhi_sim_p999_lo90,
  hhi_sim_p999_hi90 = t.hhi_sim_p999_hi90,

  hhi_sim_anchor    = t.hhi_sim_anchor,
  hhi_sim_acc       = t.hhi_sim_acc,
  flags             = (d.flags | t.flags)
FROM tmp_income_derived t
WHERE d.vintage  = t.vintage
  AND d.sumlevel = t.sumlevel
  AND d.geoid    = t.geoid;
"""

VINTAGE = 'acs2024_5yr'
CHUNK_SIZE = 10_000
SIM_COUNT = 1000

TOPCODE = 250_001
BOTTOMCODE = 2499

QS_SIM = [0.90, 0.95, 0.99, 0.999]
P0_ORDER = [0.95, 0.80]
LOW_ACC_THRESH = 0.80

FLAG_LOWER_ANCHOR_WARN = 1  # used means anchor instead of quantile anchor (P80 and P95 topcoded)
FLAG_LOW_ACC_WARN = 2  # share of valid draws is below threshold
FLAG_NOT_COMPUTABLE_BG = 4  # block groups have no income data past median
FLAG_POP_TOO_SMALL = 8  # population < 250, households < 100, household size >= 6
FLAG_MISSING_DATA = 16  # any missing data that prevents simulation
FLAG_ALL_TOPCODED = 32  # p20 and up are all topcoded, no curve possible
FLAG_SIM_P95_LT_SIM_P90 = 64  # simulated p95 was less than simulated p90 due to two different tails


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

  # Randomly draw values for T and mu
  T_s = rng.normal(T, T_se, size=(sim_count, m))
  mu_s = rng.normal(mu, mu_se, size=(sim_count, m))

  valid_draw = np.isfinite(T_s) & np.isfinite(mu_s) & (T_s > 0) & (mu_s > 0) & (mu_s > T_s)

  # Making sure that values don't become non-negative or mean is higher than quantile
  valid_share = valid_draw.mean(axis=0)
  n_valid = valid_draw.sum(axis=0)

  # Prepare for log-space computation
  logT = np.full_like(T_s, np.nan, dtype='float64')
  logT[valid_draw] = np.log(T_s[valid_draw])

  # e = (mu_s - T_s) / mu_s = 1 / alpha in (0,1) for valid draws
  e = np.full_like(T_s, np.nan, dtype='float64')
  e[valid_draw] = (mu_s[valid_draw] - T_s[valid_draw]) / mu_s[valid_draw]

  for q in qs:
    # safety check against simulating below anchor point
    if q < p0:
      continue

    # actual Pareto curve
    R = (1.0 - p0) / (1.0 - q)
    logQ = logT + e * np.log(R)

    # clamp exponents within 2^1024 to clear out Infinity
    logQ = np.clip(logQ, -709.78, 709.78)

    sim = np.exp(logQ)  # NaN where invalid_draw

    est = np.nanmedian(sim, axis=0)
    lo = np.nanpercentile(sim, 5, axis=0)
    hi = np.nanpercentile(sim, 95, axis=0)

    out[q] = (est, lo, hi, valid_share, n_valid)

  return out


def simulate_pareto_between_quantiles_chunk(
  T_lo: np.ndarray,
  T_lo_se: np.ndarray,
  T_hi: np.ndarray,
  T_hi_se: np.ndarray,
  p_lo: float,
  p_hi: float,
  qs: list[float],
  rng: np.random.Generator,
  sim_count: int,
) -> dict:
  """
  Inputs are 1D arrays of length m (rows in this anchor group).
  Returns dict[q] = (est, lo, hi, valid_share, n_valid) each length m.
  """
  m = T_lo.shape[0]
  out = {}

  # Randomly draw values for lower and upper quantiles
  T_lo_s = rng.normal(T_lo, T_lo_se, size=(sim_count, m))
  T_hi_s = rng.normal(T_hi, T_hi_se, size=(sim_count, m))

  valid_draw = (
    np.isfinite(T_lo_s) & np.isfinite(T_hi_s) & (T_lo_s > 0) & (T_hi_s > 0) & (T_hi_s > T_lo_s)
  )

  # Making sure that values don't become non-negative or upper quantile is not above lower quantile
  valid_share = valid_draw.mean(axis=0)
  n_valid = valid_draw.sum(axis=0)

  # Prepare for log-space computation
  logT = np.full_like(T_lo_s, np.nan, dtype='float64')
  logT[valid_draw] = np.log(T_lo_s[valid_draw])

  # e = 1 / alpha from the relationship between the two observed quantiles
  # T_hi = T_lo * (((1 - p_lo) / (1 - p_hi)) ** e)
  e = np.full_like(T_lo_s, np.nan, dtype='float64')
  denom = np.log((1.0 - p_lo) / (1.0 - p_hi))
  e[valid_draw] = np.log(T_hi_s[valid_draw] / T_lo_s[valid_draw]) / denom

  for q in qs:
    # safety check against simulating outside the observed quantile segment
    if q < p_lo or q > p_hi:
      continue

    # actual Pareto curve within the segment
    R = (1.0 - p_lo) / (1.0 - q)
    logQ = logT + e * np.log(R)

    # clamp exponents within 2^1024 to clear out Infinity
    logQ = np.clip(logQ, -709.78, 709.78)

    sim = np.exp(logQ)  # NaN where invalid_draw

    est = np.nanmedian(sim, axis=0)
    lo = np.nanpercentile(sim, 5, axis=0)
    hi = np.nanpercentile(sim, 95, axis=0)

    out[q] = (est, lo, hi, valid_share, n_valid)

  return out


def simulate_pareto_from_means_chunk(
  mu80: np.ndarray,
  mu80_se: np.ndarray,
  mu95: np.ndarray,
  mu95_se: np.ndarray,
  qs: list[float],
  rng: np.random.Generator,
  sim_count: int,
) -> dict:
  """
  Inputs are 1D arrays of length m (rows in this anchor group).
  Returns dict[q] = (est, lo, hi, valid_share, n_valid) each length m.

  Fits a Pareto tail above the 80th percentile using:
    mu80 = E[X | X > P80]
    mu95 = E[X | X > P95]

  where:
    alpha = ln(4) / ln(mu95 / mu80)
    P80   = (1 - 1/alpha) * mu80

  Note: in testing this was globally about 7x worse than the quantile-based algorithm, but it is
  much more necessary for affluent geographies where at least 20% of households are earning over
  $250k a year
  """
  m = mu80.shape[0]
  out = {}

  # Randomly draw values for top-20 and top-5 means
  mu80_s = rng.normal(mu80, mu80_se, size=(sim_count, m))
  mu95_s = rng.normal(mu95, mu95_se, size=(sim_count, m))

  # Need:
  #   mu80 > 0
  #   mu95 > mu80
  #   mu95 / mu80 < 4  (so alpha > 1 and implied P80 > 0)
  valid_draw = (
    np.isfinite(mu80_s)
    & np.isfinite(mu95_s)
    & (mu80_s > 0)
    & (mu95_s > mu80_s)
    & (mu95_s < 4.0 * mu80_s)
  )

  # Share of valid draws
  valid_share = valid_draw.mean(axis=0)
  n_valid = valid_draw.sum(axis=0)

  # e = 1 / alpha
  e = np.full_like(mu80_s, np.nan, dtype='float64')
  e[valid_draw] = np.log(mu95_s[valid_draw] / mu80_s[valid_draw]) / np.log(4.0)

  # implied latent P80
  T_s = np.full_like(mu80_s, np.nan, dtype='float64')
  T_s[valid_draw] = mu80_s[valid_draw] * (1.0 - e[valid_draw])

  # ensure implied threshold is positive
  valid_draw = valid_draw & np.isfinite(T_s) & (T_s > 0)

  logT = np.full_like(T_s, np.nan, dtype='float64')
  logT[valid_draw] = np.log(T_s[valid_draw])

  for q in qs:
    # means-only tail is defined above 0.80
    if q < 0.80:
      continue

    R = 0.20 / (1.0 - q)
    logQ = logT + e * np.log(R)

    # clamp exponents within float64 range
    logQ = np.clip(logQ, -709.78, 709.78)

    sim = np.exp(logQ)  # NaN where invalid_draw

    est = np.nanmedian(sim, axis=0)
    lo = np.nanpercentile(sim, 5, axis=0)
    hi = np.nanpercentile(sim, 95, axis=0)

    out[q] = (est, lo, hi, valid_share, n_valid)

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
      cur.execute(QUERY_CREATE_TABLE)
      cur.execute(QUERY_RESET_TABLE, (VINTAGE,))
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
          VINTAGE,
          FLAG_POP_TOO_SMALL,
        ),
      )

      # only to make Ruff stop screaming
      if cur.description is None:
        raise RuntimeError('cur.description is None')
      colnames = [d[0] for d in cur.description]

      # create RNG seed
      rng = np.random.default_rng(1)

      pbar = tqdm(desc='Processing rows')

      while True:
        rows = cur.fetchmany(CHUNK_SIZE)
        pbar.update(len(rows))

        if not rows:
          break

        df = pd.DataFrame(rows, columns=colnames)
        n = len(df)

        # extract source arrays
        source_arrays = {}
        for a in [
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
          'hhi_q5_mean',
          'hhi_q5_mean_se',
          'hhi_q4_mean',
          'hhi_q4_mean_se',
          'hhi_q3_mean',
          'hhi_q3_mean_se',
          'hhi_q2_mean',
          'hhi_q2_mean_se',
        ]:
          source_arrays[a] = df[a].astype('float64').to_numpy()

        flags = df['flags'].astype('int32').to_numpy().copy()

        # output arrays (estimate, lo90, hi90)
        est = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}
        lo = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}
        hi = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}

        # track which anchor produced each q (95/80/1 as codes; 0 means missing)
        src = {q: np.zeros(n, dtype=np.uint16) for q in QS_SIM}

        # track valid share of draws for the anchor that filled each q
        acc_q = {q: np.full(n, np.nan, dtype='float64') for q in QS_SIM}

        # build and run anchors in priority order
        for p0 in P0_ORDER:
          # Determine T / T_se
          if p0 == 0.95:
            T, T_se = source_arrays['hhi_p95'], source_arrays['hhi_p95_se']
            mu, mu_se = source_arrays['hhi_top5_mean'], source_arrays['hhi_top5_mean_se']
            anchor_code = 95
          elif p0 == 0.80:
            T, T_se = source_arrays['hhi_p80'], source_arrays['hhi_p80_se']
            mu, mu_se = source_arrays['hhi_q5_mean'], source_arrays['hhi_q5_mean_se']
            anchor_code = 80
          else:
            # for p0 < 0.80, run the means algorithm
            continue

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

        # Means-only fallback for rows where BOTH P95 and P80 are topcoded.
        # This only fills rows/quantiles that are still missing after the normal anchor loop.
        mu80 = source_arrays['hhi_q5_mean']
        mu80_se = source_arrays['hhi_q5_mean_se']
        mu95 = source_arrays['hhi_top5_mean']
        mu95_se = source_arrays['hhi_top5_mean_se']
        p80 = source_arrays['hhi_p80']
        p95 = source_arrays['hhi_p95']

        need_means_fallback = (p80 == TOPCODE) & (p95 == TOPCODE)

        # only bother if at least one target quantile is still missing
        still_missing_any = np.zeros(n, dtype=bool)
        for q in QS_SIM:
          still_missing_any |= np.isnan(est[q])

        row_ok = (
          need_means_fallback
          & still_missing_any
          & np.isfinite(mu80)
          & np.isfinite(mu80_se)
          & np.isfinite(mu95)
          & np.isfinite(mu95_se)
          & (mu80 > 0)
          & (mu80_se >= 0)
          & (mu95 > mu80)
          & (mu95_se >= 0)
          & (mu95 < 4.0 * mu80)
        )

        rows_idx = np.where(row_ok)[0]

        if rows_idx.size > 0:
          sim_out = simulate_pareto_from_means_chunk(
            mu80=mu80[rows_idx],
            mu80_se=mu80_se[rows_idx],
            mu95=mu95[rows_idx],
            mu95_se=mu95_se[rows_idx],
            qs=QS_SIM,
            rng=rng,
            sim_count=SIM_COUNT,
          )

          for q in QS_SIM:
            if q not in sim_out:
              continue

            est_this, lo_this, hi_this, acc_vals_this, _ = sim_out[q]

            missing = np.isnan(est[q][rows_idx]) & np.isfinite(est_this)
            if not np.any(missing):
              continue

            target = rows_idx[missing]
            est[q][target] = est_this[missing]
            lo[q][target] = lo_this[missing]
            hi[q][target] = hi_this[missing]
            src[q][target] = 1  # means-implied latent P80
            acc_q[q][target] = acc_vals_this[missing]

        # Overwrite P90 for rows where simulated P95 < simulated P90
        p80 = source_arrays['hhi_p80']
        p80_se = source_arrays['hhi_p80_se']
        p95 = source_arrays['hhi_p95']
        p95_se = source_arrays['hhi_p95_se']

        repair_p90_segment = (
          (src[0.95] == 95)
          & (src[0.90] == 80)
          & (est[0.90] > est[0.95])
          & np.isfinite(p80)
          & np.isfinite(p80_se)
          & np.isfinite(p95)
          & np.isfinite(p95_se)
          & (p80 > 0)
          & (p80 < TOPCODE)
          & (p80_se >= 0)
          & (p95 > p80)
          & (p95 < TOPCODE)
          & (p95_se >= 0)
        )

        # had to attempt a tail repair (NOT a guarantee)
        flags[repair_p90_segment] |= FLAG_SIM_P95_LT_SIM_P90

        seg_idx = np.where(repair_p90_segment)[0]

        if seg_idx.size > 0:
          seg_out = simulate_pareto_between_quantiles_chunk(
            T_lo=p80[seg_idx],
            T_lo_se=p80_se[seg_idx],
            T_hi=p95[seg_idx],
            T_hi_se=p95_se[seg_idx],
            p_lo=0.80,
            p_hi=0.95,
            qs=[0.90],
            rng=rng,
            sim_count=SIM_COUNT,
          )

          est_fix, lo_fix, hi_fix, valid_share_fix, _ = seg_out[0.90]

          good = np.isfinite(est_fix)
          target = seg_idx[good]

          est[0.90][target] = est_fix[good]
          lo[0.90][target] = lo_fix[good]
          hi[0.90][target] = hi_fix[good]
          acc_q[0.90][target] = valid_share_fix[good]

        anchor_used = np.zeros(n, dtype=np.uint16)
        for q in QS_SIM:
          anchor_used = np.maximum(anchor_used, src[q])

        # convert 0 -> NA (NULL in DB)
        anchor_used_series = pd.Series(
          pd.array(np.where(anchor_used == 0, np.nan, anchor_used), dtype='Int64')
        )

        # set flags
        # 32: all top coded (extremely rare)
        p95 = source_arrays['hhi_p95']
        p80 = source_arrays['hhi_p80']
        p60 = source_arrays['hhi_p60']
        p40 = source_arrays['hhi_p40']
        p20 = source_arrays['hhi_p20']

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

        # 1: used means anchor instead of quantile anchor
        used_means_for_p95 = src[0.95] == 1
        flags[used_means_for_p95] |= FLAG_LOWER_ANCHOR_WARN

        # 2: accuracy below threshold (chooses the lowest accuracy among simulated anchor points)
        # compute per-row "worst" accuracy
        acc_mat = np.vstack([acc_q[q] for q in QS_SIM])  # shape (QS_SIM, n)
        acc_for_min = np.where(np.isfinite(acc_mat), acc_mat, np.inf)

        worst_acc = np.min(acc_for_min, axis=0)
        has_acc = np.isfinite(worst_acc) & (worst_acc != np.inf)

        sim_accuracy = np.full(n, np.nan, dtype='float64')
        sim_accuracy[has_acc] = worst_acc[has_acc]

        low_acc = has_acc & (sim_accuracy < LOW_ACC_THRESH)
        flags[low_acc] |= FLAG_LOW_ACC_WARN

        # 16: any missing data that prevented simulation (missing means or thresholds)
        # Define "computed_any" as: we successfully filled at least one simulated percentile.
        computed_any = np.zeros(n, dtype=bool)
        for q in QS_SIM:
          computed_any |= src[q] != 0

        # Missing data means: nothing computed AND not all-coded.
        # (BG and too-small rows never enter this loop; they already have flags 4/8.)
        missing_data = (~computed_any) & (~all_coded)
        flags[missing_data] |= FLAG_MISSING_DATA

        # very "fun" SQL
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
            'hhi_sim_anchor': anchor_used_series,
            'hhi_sim_acc': sim_accuracy,
            'flags': flags.astype('int32'),
          }
        )

        with write_conn.cursor() as wcur:
          wcur.execute(QUERY_UPDATE_CREATE)

          buf = io.StringIO()
          out_df.to_csv(buf, index=False, na_rep='')  # blanks for NULL
          buf.seek(0)

          with wcur.copy(QUERY_UPDATE_FROM_CSV) as cp:
            cp.write(buf.getvalue())

          # send temp rows to DB
          wcur.execute(QUERY_UPDATE_TO_DB)

        write_conn.commit()


if __name__ == '__main__':
  main()
