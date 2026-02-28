DROP TABLE IF EXISTS viz.income_base;

CREATE TABLE viz.income_base AS
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
  viz.clean_int(quintile_avg.b19081001)               AS hhi_q1_mean,
  (viz.clean_dec(quintile_avg.b19081001_moe) / 1.645) AS hhi_q1_mean_se,
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

  -- Table B19082: Shares (percent units 0..100, keep as decimals)
  viz.clean_dec(quintile_share.b19082001)               AS hhi_q1_share,
  (viz.clean_dec(quintile_share.b19082001_moe) / 1.645) AS hhi_q1_share_se,
  viz.clean_dec(quintile_share.b19082002)               AS hhi_q2_share,
  (viz.clean_dec(quintile_share.b19082002_moe) / 1.645) AS hhi_q2_share_se,
  viz.clean_dec(quintile_share.b19082003)               AS hhi_q3_share,
  (viz.clean_dec(quintile_share.b19082003_moe) / 1.645) AS hhi_q3_share_se,
  viz.clean_dec(quintile_share.b19082004)               AS hhi_q4_share,
  (viz.clean_dec(quintile_share.b19082004_moe) / 1.645) AS hhi_q4_share_se,
  viz.clean_dec(quintile_share.b19082005)               AS hhi_q5_share,
  (viz.clean_dec(quintile_share.b19082005_moe) / 1.645) AS hhi_q5_share_se,
  viz.clean_dec(quintile_share.b19082006)               AS hhi_top5_share,
  (viz.clean_dec(quintile_share.b19082006_moe) / 1.645) AS hhi_top5_share_se,

  -- Table B19083: Gini (0..1, keep as decimal)
  viz.clean_dec(gini.b19083001)               AS hhi_gini,
  (viz.clean_dec(gini.b19083001_moe) / 1.645) AS hhi_gini_se
FROM viz.geoid_base g
LEFT JOIN acs2024_5yr.b19013_moe hhi USING (geoid)
LEFT JOIN acs2024_5yr.b19080_moe quintile_thresh USING (geoid)
LEFT JOIN acs2024_5yr.b19081_moe quintile_avg USING (geoid)
LEFT JOIN acs2024_5yr.b19082_moe quintile_share USING (geoid)
LEFT JOIN acs2024_5yr.b19083_moe gini USING (geoid);

ALTER TABLE viz.income_base
ADD CONSTRAINT income_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);