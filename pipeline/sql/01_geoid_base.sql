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
FROM :"vintage".geoheader g
JOIN :"vintage".b01003_moe pop USING (geoid)
JOIN :"vintage".b11001_moe hh USING (geoid)
JOIN :"vintage".b25010_moe hhs USING (geoid)
WHERE
  sumlevel IN ('010','040','050','060','140','150','160','310','500','860')
  AND component = '00'
  AND pop.b01003001 IS NOT NULL
  AND hh.b11001001 IS NOT NULL
  AND hhs.b25010001 IS NOT NULL;

CREATE TABLE IF NOT EXISTS viz.geoid_base (
  LIKE tmp_geoid_base INCLUDING DEFAULTS
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'geoid_base_pkey'
      AND conrelid = 'viz.geoid_base'::regclass
  ) THEN
    ALTER TABLE viz.geoid_base
    ADD CONSTRAINT geoid_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);
  END IF;
END
$$;

DELETE FROM viz.geoid_base
WHERE vintage = :'vintage';

INSERT INTO viz.geoid_base
SELECT *
FROM tmp_geoid_base;

COMMIT;
