DROP TABLE IF EXISTS viz.geoid_base;

CREATE TABLE viz.geoid_base AS
SELECT
  'acs2024_5yr'::text AS vintage,
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
FROM acs2024_5yr.geoheader g
JOIN acs2024_5yr.b01003_moe pop USING (geoid)
JOIN acs2024_5yr.b11001_moe hh USING (geoid)
JOIN acs2024_5yr.b25010_moe hhs USING (geoid)
WHERE
  sumlevel IN ('010','040','050','060','140','150','160','310','500','860')
  AND component = '00'
  AND pop.b01003001 IS NOT NULL
  AND hh.b11001001 IS NOT NULL
  AND hhs.b25010001 IS NOT NULL;

ALTER TABLE viz.geoid_base
ADD CONSTRAINT geoid_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);