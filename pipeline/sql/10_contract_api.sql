CREATE SCHEMA IF NOT EXISTS api;

CREATE OR REPLACE VIEW api.geoid_v1 AS
SELECT
  -- geoid
  vintage AS vintage,
  geoid AS geoid,
  name AS name,
  sumlevel AS sumlevel,
  state_code AS state_code,

  -- base values
  total_population AS total_population,

  CASE
    WHEN total_population IS NULL OR total_population_se IS NULL THEN NULL
    WHEN total_population - total_population_se * 1.645 < 0 THEN 0
    ELSE ROUND(total_population - total_population_se * 1.645)::bigint
  END AS total_population_lo90,

  CASE
    WHEN total_population IS NULL OR total_population_se IS NULL THEN NULL
    ELSE ROUND(total_population + total_population_se * 1.645)::bigint
  END AS total_population_hi90,

  total_households AS total_households,

  CASE
    WHEN total_households IS NULL OR total_households_se IS NULL THEN NULL
    WHEN total_households - total_households_se * 1.645 < 0 THEN 0
    ELSE ROUND(total_households - total_households_se * 1.645)::bigint
  END AS total_households_lo90,

  CASE
    WHEN total_households IS NULL OR total_households_se IS NULL THEN NULL
    ELSE ROUND(total_households + total_households_se * 1.645)::bigint
  END AS total_households_hi90,

  ROUND(household_size::numeric, 2)::double precision AS avg_household_size,

  CASE
    WHEN household_size IS NULL OR household_size_se IS NULL THEN NULL
    WHEN household_size - household_size_se * 1.645 < 0 THEN 0
    ELSE ROUND((household_size - household_size_se * 1.645)::numeric, 2)::double precision
  END AS avg_household_size_lo90,

  CASE
    WHEN household_size IS NULL OR household_size_se IS NULL THEN NULL
    ELSE ROUND((household_size + household_size_se * 1.645)::numeric, 2)::double precision
  END AS avg_household_size_hi90

FROM viz.geoid_base;

CREATE OR REPLACE VIEW api.income_v1 AS
SELECT
  -- geoid
  g.vintage AS vintage,
  g.geoid AS geoid,
  g.name AS name,
  g.sumlevel AS sumlevel,
  g.state_code AS state_code,

  -- base values (convert to int from float)
  b.hhi_median::bigint AS hhi_median,
  b.hhi_p20::bigint AS hhi_p20,
  b.hhi_p40::bigint AS hhi_p40,
  b.hhi_p60::bigint AS hhi_p60,
  b.hhi_p80::bigint AS hhi_p80,
  b.hhi_p95::bigint AS hhi_p95,
  
  CASE
    WHEN b.hhi_median IS NULL OR b.hhi_median_se IS NULL THEN NULL
    WHEN b.hhi_median - b.hhi_median_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_median - b.hhi_median_se * 1.645)::bigint
  END AS hhi_median_lo90,

  CASE
    WHEN b.hhi_p20 IS NULL OR b.hhi_p20_se IS NULL THEN NULL
    WHEN b.hhi_p20 - b.hhi_p20_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_p20 - b.hhi_p20_se * 1.645)::bigint
  END AS hhi_p20_lo90,

  CASE
    WHEN b.hhi_p40 IS NULL OR b.hhi_p40_se IS NULL THEN NULL
    WHEN b.hhi_p40 - b.hhi_p40_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_p40 - b.hhi_p40_se * 1.645)::bigint
  END AS hhi_p40_lo90,

  CASE
    WHEN b.hhi_p60 IS NULL OR b.hhi_p60_se IS NULL THEN NULL
    WHEN b.hhi_p60 - b.hhi_p60_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_p60 - b.hhi_p60_se * 1.645)::bigint
  END AS hhi_p60_lo90,

  CASE
    WHEN b.hhi_p80 IS NULL OR b.hhi_p80_se IS NULL THEN NULL
    WHEN b.hhi_p80 - b.hhi_p80_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_p80 - b.hhi_p80_se * 1.645)::bigint
  END AS hhi_p80_lo90,

  CASE
    WHEN b.hhi_p95 IS NULL OR b.hhi_p95_se IS NULL THEN NULL
    WHEN b.hhi_p95 - b.hhi_p95_se * 1.645 < 0 THEN 0
    ELSE ROUND(b.hhi_p95 - b.hhi_p95_se * 1.645)::bigint
  END AS hhi_p95_lo90,

  (b.hhi_median + b.hhi_median_se * 1.645)::bigint AS hhi_median_hi90,
  (b.hhi_p20 + b.hhi_p20_se * 1.645)::bigint AS hhi_p20_hi90,
  (b.hhi_p40 + b.hhi_p40_se * 1.645)::bigint AS hhi_p40_hi90,
  (b.hhi_p60 + b.hhi_p60_se * 1.645)::bigint AS hhi_p60_hi90,
  (b.hhi_p80 + b.hhi_p80_se * 1.645)::bigint AS hhi_p80_hi90,
  (b.hhi_p95 + b.hhi_p95_se * 1.645)::bigint AS hhi_p95_hi90,

  -- derived values (already converted to int)
  d.hhi_sim_p90 AS hhi_sim_p90,
  d.hhi_sim_p95 AS hhi_sim_p95,
  d.hhi_sim_p99 AS hhi_sim_p99,
  d.hhi_sim_p999 AS hhi_sim_p999,

  d.hhi_sim_p90_lo90 AS hhi_sim_p90_lo90,
  d.hhi_sim_p95_lo90 AS hhi_sim_p95_lo90,
  d.hhi_sim_p99_lo90 AS hhi_sim_p99_lo90,
  d.hhi_sim_p999_lo90 AS hhi_sim_p999_lo90,

  d.hhi_sim_p90_hi90 AS hhi_sim_p90_hi90,
  d.hhi_sim_p95_hi90 AS hhi_sim_p95_hi90,
  d.hhi_sim_p99_hi90 AS hhi_sim_p99_hi90,
  d.hhi_sim_p999_hi90 AS hhi_sim_p999_hi90,

  ROUND(b.hhi_gini::numeric, 3) AS hhi_gini,

  CASE
    WHEN b.hhi_gini IS NULL OR b.hhi_gini_se IS NULL THEN NULL
    WHEN b.hhi_gini - b.hhi_gini_se * 1.645 < 0 THEN 0
    WHEN b.hhi_gini - b.hhi_gini_se * 1.645 > 1 THEN 1
    ELSE ROUND((b.hhi_gini - b.hhi_gini_se * 1.645)::numeric, 3)
  END AS hhi_gini_lo90,

  CASE
    WHEN b.hhi_gini IS NULL OR b.hhi_gini_se IS NULL THEN NULL
    WHEN b.hhi_gini + b.hhi_gini_se * 1.645 < 0 THEN 0
    WHEN b.hhi_gini + b.hhi_gini_se * 1.645 > 1 THEN 1
    ELSE ROUND((b.hhi_gini + b.hhi_gini_se * 1.645)::numeric, 3)
  END AS hhi_gini_hi90,

  d.hhi_sim_anchor AS hhi_sim_anchor,
  d.flags::integer AS flags

 FROM viz.geoid_base g
 JOIN viz.income_base b
   ON b.vintage = g.vintage
  AND b.sumlevel = g.sumlevel
  AND b.geoid = g.geoid
 JOIN viz.income_derived d
   ON d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
WHERE (d.flags & 8) = 0
  AND (hhi_median IS NOT NULL
   OR hhi_p20 IS NOT NULL
   OR hhi_p40 IS NOT NULL
   OR hhi_p60 IS NOT NULL
   OR hhi_p80 IS NOT NULL
   OR hhi_p95 IS NOT NULL);

CREATE OR REPLACE VIEW api.education_v1 AS
SELECT
  -- geoid
  g.vintage AS vintage,
  g.geoid AS geoid,
  g.name AS name,
  g.sumlevel AS sumlevel,
  g.state_code AS state_code,

  -- derived values (normalize to [0, 100] for education index)
  CASE
    WHEN d.edu_education_index IS NULL THEN NULL
    WHEN (d.edu_education_index - 1) * 100 < 0 THEN 0.0
    WHEN (d.edu_education_index - 1) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.edu_education_index - 1) * 100)::numeric, 2)::double precision
  END AS edu_education_index,

  CASE
    WHEN d.edu_education_index_lo90 IS NULL THEN NULL
    WHEN (d.edu_education_index_lo90 - 1) * 100 < 0 THEN 0.0
    WHEN (d.edu_education_index_lo90 - 1) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.edu_education_index_lo90 - 1) * 100)::numeric, 2)::double precision
  END AS edu_education_index_lo90,

  CASE
    WHEN d.edu_education_index_hi90 IS NULL THEN NULL
    WHEN (d.edu_education_index_hi90 - 1) * 100 < 0 THEN 0.0
    WHEN (d.edu_education_index_hi90 - 1) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.edu_education_index_hi90 - 1) * 100)::numeric, 2)::double precision
  END AS edu_education_index_hi90,

  CASE
    WHEN d.edu_years_of_school IS NULL THEN NULL
    ELSE ROUND(d.edu_years_of_school::numeric, 2)::double precision
  END AS edu_years_of_school,

  CASE
    WHEN d.edu_years_of_school_lo90 IS NULL THEN NULL
    WHEN d.edu_years_of_school_lo90 < 0 THEN 0.0
    WHEN d.edu_years_of_school_lo90 > 22 THEN 22.0
    ELSE ROUND(d.edu_years_of_school_lo90::numeric, 2)::double precision
  END AS edu_years_of_school_lo90,

  CASE
    WHEN d.edu_years_of_school_hi90 IS NULL THEN NULL
    WHEN d.edu_years_of_school_hi90 < 0 THEN 0.0
    WHEN d.edu_years_of_school_hi90 > 22 THEN 22.0
    ELSE ROUND(d.edu_years_of_school_hi90::numeric, 2)::double precision
  END AS edu_years_of_school_hi90,
  
  d.flags::integer AS flags

 FROM viz.geoid_base g
 JOIN viz.education_derived d
   ON d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
WHERE (d.flags & 8) = 0;

CREATE OR REPLACE VIEW api.diversity_v1 AS
SELECT
  -- geoid
  g.vintage AS vintage,
  g.geoid AS geoid,
  g.name AS name,
  g.sumlevel AS sumlevel,
  g.state_code AS state_code,

  -- derived values (normalize to [0, 100])
  CASE
    WHEN d.race_diversity_index IS NULL THEN NULL
    WHEN (d.race_diversity_index / (5.0/6.0)) * 100 < 0 THEN 0.0
    WHEN (d.race_diversity_index / (5.0/6.0)) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.race_diversity_index / (5.0/6.0)) * 100)::numeric, 2)::double precision
  END AS race_diversity_index,

  CASE
    WHEN d.race_diversity_index_lo90 IS NULL THEN NULL
    WHEN (d.race_diversity_index_lo90 / (5.0/6.0)) * 100 < 0 THEN 0.0
    WHEN (d.race_diversity_index_lo90 / (5.0/6.0)) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.race_diversity_index_lo90 / (5.0/6.0)) * 100)::numeric, 2)::double precision
  END AS race_diversity_index_lo90,

  CASE
    WHEN d.race_diversity_index_hi90 IS NULL THEN NULL
    WHEN (d.race_diversity_index_hi90 / (5.0/6.0)) * 100 < 0 THEN 0.0
    WHEN (d.race_diversity_index_hi90 / (5.0/6.0)) * 100 > 100 THEN 100.0
    ELSE ROUND(((d.race_diversity_index_hi90 / (5.0/6.0)) * 100)::numeric, 2)::double precision
  END AS race_diversity_index_hi90,
  
  d.flags::integer AS flags

 FROM viz.geoid_base g
 JOIN viz.diversity_derived d
   ON d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
WHERE (d.flags & 8) = 0;

CREATE OR REPLACE VIEW api.occupation_v1 AS
SELECT
  -- geoid
  g.vintage AS vintage,
  g.geoid AS geoid,
  g.name AS name,
  g.sumlevel AS sumlevel,
  g.state_code AS state_code,

  -- derived values
  CASE
    WHEN d.occ_occupation_index IS NULL THEN NULL
    ELSE ROUND(d.occ_occupation_index::numeric, 2)::double precision
  END AS occ_occupation_index,

  CASE
    WHEN d.occ_occupation_index_lo90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_lo90 < 0 THEN 0.0
    WHEN d.occ_occupation_index_lo90 > 5 THEN 5.0
    ELSE ROUND(d.occ_occupation_index_lo90::numeric, 2)::double precision
  END AS occ_occupation_index_lo90,

  CASE
    WHEN d.occ_occupation_index_hi90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_hi90 < 0 THEN 0.0
    WHEN d.occ_occupation_index_hi90 > 5 THEN 5.0
    ELSE ROUND(d.occ_occupation_index_hi90::numeric, 2)::double precision
  END AS occ_occupation_index_hi90,

  CASE
    WHEN d.occ_occupation_index_ext IS NULL THEN NULL
    ELSE ROUND(d.occ_occupation_index_ext::numeric, 2)::double precision
  END AS occ_occupation_index_ext,

  CASE
    WHEN d.occ_occupation_index_ext_lo90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_ext_lo90 < 0 THEN 0.0
    WHEN d.occ_occupation_index_ext_lo90 > 25 THEN 25.0
    ELSE ROUND(d.occ_occupation_index_ext_lo90::numeric, 2)::double precision
  END AS occ_occupation_index_ext_lo90,

  CASE
    WHEN d.occ_occupation_index_ext_hi90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_ext_hi90 < 0 THEN 0.0
    WHEN d.occ_occupation_index_ext_hi90 > 25 THEN 25.0
    ELSE ROUND(d.occ_occupation_index_ext_hi90::numeric, 2)::double precision
  END AS occ_occupation_index_ext_hi90,

  CASE
    WHEN d.occ_occupation_index_ratio IS NULL THEN NULL
    ELSE ROUND(d.occ_occupation_index_ratio::numeric, 2)::double precision
  END AS occ_occupation_index_ratio,

  CASE
    WHEN d.occ_occupation_index_ratio_lo90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_ratio_lo90 < 1 THEN 1.0
    ELSE ROUND(d.occ_occupation_index_ratio_lo90::numeric, 2)::double precision
  END AS occ_occupation_index_ratio_lo90,

  CASE
    WHEN d.occ_occupation_index_ratio_hi90 IS NULL THEN NULL
    WHEN d.occ_occupation_index_ratio_hi90 < 1 THEN 1.0
    ELSE ROUND(d.occ_occupation_index_ratio_hi90::numeric, 2)::double precision
  END AS occ_occupation_index_ratio_hi90,
  
  d.flags::integer AS flags

 FROM viz.geoid_base g
 JOIN viz.occupation_derived d
   ON d.vintage = g.vintage
  AND d.sumlevel = g.sumlevel
  AND d.geoid = g.geoid
WHERE (d.flags & 8) = 0;
