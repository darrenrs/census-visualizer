DROP TABLE IF EXISTS viz.diversity_base;

CREATE TABLE viz.diversity_base AS
SELECT
  g.vintage,
  g.sumlevel,
  g.geoid,

  -- Table B03002: Hispanic or Latino Origin by Race
  viz.clean_int(race.b03002003)               AS race_white_nh,
  (viz.clean_dec(race.b03002003_moe) / 1.645) AS race_white_nh_se,
  viz.clean_int(race.b03002004)               AS race_black_nh,
  (viz.clean_dec(race.b03002004_moe) / 1.645) AS race_black_nh_se,
  viz.clean_int(race.b03002005)               AS race_aian_nh,
  (viz.clean_dec(race.b03002005_moe) / 1.645) AS race_aian_nh_se,
  viz.clean_int(race.b03002006)               AS race_asian_nh,
  (viz.clean_dec(race.b03002006_moe) / 1.645) AS race_asian_nh_se,
  viz.clean_int(race.b03002007)               AS race_nhpi_nh,
  (viz.clean_dec(race.b03002007_moe) / 1.645) AS race_nhpi_nh_se,
  viz.clean_int(race.b03002008)               AS race_other_nh,
  (viz.clean_dec(race.b03002008_moe) / 1.645) AS race_other_nh_se,
  viz.clean_int(race.b03002009)               AS race_multi_nh,
  (viz.clean_dec(race.b03002009_moe) / 1.645) AS race_multi_nh_se,
  viz.clean_int(race.b03002012)               AS race_hispanic,
  (viz.clean_dec(race.b03002012_moe) / 1.645) AS race_hispanic_se
FROM viz.geoid_base g
LEFT JOIN acs2024_5yr.b03002_moe race USING (geoid);

ALTER TABLE viz.diversity_base
ADD CONSTRAINT diversity_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);