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
LEFT JOIN :"vintage".b19013_moe hhi USING (geoid)
LEFT JOIN :"vintage".b19080_moe quintile_thresh USING (geoid)
LEFT JOIN :"vintage".b19081_moe quintile_avg USING (geoid)
LEFT JOIN :"vintage".b19083_moe gini USING (geoid)
WHERE g.vintage = :'vintage';

CREATE TABLE IF NOT EXISTS viz.income_base (
  LIKE tmp_income_base INCLUDING DEFAULTS
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'income_base_pkey'
      AND conrelid = 'viz.income_base'::regclass
  ) THEN
    ALTER TABLE viz.income_base
    ADD CONSTRAINT income_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);
  END IF;
END
$$;

DELETE FROM viz.income_base
WHERE vintage = :'vintage';

INSERT INTO viz.income_base
SELECT *
FROM tmp_income_base;

COMMIT;
