BEGIN;

CREATE TEMP TABLE tmp_geoid_base
ON COMMIT DROP AS
SELECT
  :'vintage'::text AS vintage,
  g.geoid,
  g.name,
  g.sumlevel,
  g.stusab AS state_code,

  -- Table B01003: Total Population
  viz.clean_int(pop.b01003001)               AS total_population,
  (viz.clean_dec(pop.b01003001_moe) / 1.645) AS total_population_se,

  -- Table B11001: Total Households
  viz.clean_int(hh.b11001001)                AS total_households,
  (viz.clean_dec(hh.b11001001_moe) / 1.645)  AS total_households_se,

  -- Table B25010: Average Household Size
  viz.clean_dec(hhs.b25010001)               AS household_size,
  (viz.clean_dec(hhs.b25010001_moe) / 1.645) AS household_size_se
FROM raw.geoheader g
JOIN raw.b01003_moe pop
  ON pop.vintage = g.vintage
 AND pop.geoid = g.geoid
JOIN raw.b11001_moe hh
  ON hh.vintage = g.vintage
 AND hh.geoid = g.geoid
JOIN raw.b25010_moe hhs
  ON hhs.vintage = g.vintage
 AND hhs.geoid = g.geoid
WHERE
  g.vintage = :'vintage'
  AND g.sumlevel IN (10,40,50,60,140,150,160,310,500,860)
  AND g.component = '00'
  AND pop.b01003001 IS NOT NULL
  AND hh.b11001001 IS NOT NULL;

CREATE TABLE IF NOT EXISTS viz.geoid_base (
  LIKE tmp_geoid_base INCLUDING DEFAULTS,
  PRIMARY KEY (vintage, geoid)
);

CREATE INDEX IF NOT EXISTS geoid_base_vintage_sumlevel_idx
ON viz.geoid_base (vintage, sumlevel);

DELETE FROM viz.geoid_base
WHERE vintage = :'vintage';

INSERT INTO viz.geoid_base
SELECT *
FROM tmp_geoid_base;

COMMIT;
