BEGIN;

CREATE TEMP TABLE tmp_income_base
ON COMMIT DROP AS
SELECT
  g.vintage,
  g.sumlevel,
  g.geoid,

  -- Table B19013: Median Household Income
  viz.clean_int(hhi.b19013001)                    AS hhi_median,
  (viz.clean_dec(hhi.b19013001_moe) / 1.645)      AS hhi_median_se,

  -- Table B19080: Household Income Quintile Thresholds
  viz.clean_int(quintile_thresh.b19080001)               AS hhi_p20,
  (viz.clean_dec(quintile_thresh.b19080001_moe) / 1.645) AS hhi_p20_se,
  viz.clean_int(quintile_thresh.b19080002)               AS hhi_p40,
  (viz.clean_dec(quintile_thresh.b19080002_moe) / 1.645) AS hhi_p40_se,
  viz.clean_int(quintile_thresh.b19080003)               AS hhi_p60,
  (viz.clean_dec(quintile_thresh.b19080003_moe) / 1.645) AS hhi_p60_se,
  viz.clean_int(quintile_thresh.b19080004)               AS hhi_p80,
  (viz.clean_dec(quintile_thresh.b19080004_moe) / 1.645) AS hhi_p80_se,
  viz.clean_int(quintile_thresh.b19080005)               AS hhi_p95,
  (viz.clean_dec(quintile_thresh.b19080005_moe) / 1.645) AS hhi_p95_se,

  -- Table B19081: Household Income Quintile Averages
  viz.clean_int(quintile_avg.b19081002)               AS hhi_q2_mean,
  (viz.clean_dec(quintile_avg.b19081002_moe) / 1.645) AS hhi_q2_mean_se,
  viz.clean_int(quintile_avg.b19081003)               AS hhi_q3_mean,
  (viz.clean_dec(quintile_avg.b19081003_moe) / 1.645) AS hhi_q3_mean_se,
  viz.clean_int(quintile_avg.b19081004)               AS hhi_q4_mean,
  (viz.clean_dec(quintile_avg.b19081004_moe) / 1.645) AS hhi_q4_mean_se,
  viz.clean_int(quintile_avg.b19081005)               AS hhi_q5_mean,
  (viz.clean_dec(quintile_avg.b19081005_moe) / 1.645) AS hhi_q5_mean_se,
  viz.clean_int(quintile_avg.b19081006)               AS hhi_top5_mean,
  (viz.clean_dec(quintile_avg.b19081006_moe) / 1.645) AS hhi_top5_mean_se,

  -- Table B19083: Gini (0..1, keep as decimal)
  viz.clean_dec(gini.b19083001)               AS hhi_gini,
  (viz.clean_dec(gini.b19083001_moe) / 1.645) AS hhi_gini_se
FROM viz.geoid_base g
LEFT JOIN raw.b19013_moe hhi
  ON hhi.vintage = g.vintage
 AND hhi.geoid = g.geoid
LEFT JOIN raw.b19080_moe quintile_thresh
  ON quintile_thresh.vintage = g.vintage
 AND quintile_thresh.geoid = g.geoid
LEFT JOIN raw.b19081_moe quintile_avg
  ON quintile_avg.vintage = g.vintage
 AND quintile_avg.geoid = g.geoid
LEFT JOIN raw.b19083_moe gini
  ON gini.vintage = g.vintage
 AND gini.geoid = g.geoid
WHERE g.vintage = :'vintage';

CREATE TABLE IF NOT EXISTS viz.income_base (
  LIKE tmp_income_base INCLUDING DEFAULTS,
  PRIMARY KEY (vintage, geoid)
);

CREATE INDEX IF NOT EXISTS income_base_vintage_sumlevel_idx
ON viz.income_base (vintage, sumlevel);

DELETE FROM viz.income_base
WHERE vintage = :'vintage';

INSERT INTO viz.income_base
SELECT *
FROM tmp_income_base;

COMMIT;
