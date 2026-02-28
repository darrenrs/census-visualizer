DROP TABLE IF EXISTS viz.occupation_base;

CREATE TABLE viz.occupation_base AS
SELECT
  g.vintage,
  g.sumlevel,
  g.geoid,

  -- Table C24010: Detailed Occupation Breakdown for employed persons aged 16 and over
  viz.clean_int(occ.c24010001)               AS occ_population,
  (viz.clean_dec(occ.c24010001_moe) / 1.645) AS occ_population_se,

  viz.clean_int(occ.c24010003)               AS occ_professional_m,
  (viz.clean_dec(occ.c24010003_moe) / 1.645) AS occ_professional_m_se,
  viz.clean_int(occ.c24010039)               AS occ_professional_f,
  (viz.clean_dec(occ.c24010039_moe) / 1.645) AS occ_professional_f_se,

  viz.clean_int(occ.c24010019)               AS occ_service_m,
  (viz.clean_dec(occ.c24010019_moe) / 1.645) AS occ_service_m_se,
  viz.clean_int(occ.c24010055)               AS occ_service_f,
  (viz.clean_dec(occ.c24010055_moe) / 1.645) AS occ_service_f_se,

  viz.clean_int(occ.c24010027)               AS occ_sales_m,
  (viz.clean_dec(occ.c24010027_moe) / 1.645) AS occ_sales_m_se,
  viz.clean_int(occ.c24010063)               AS occ_sales_f,
  (viz.clean_dec(occ.c24010063_moe) / 1.645) AS occ_sales_f_se,

  viz.clean_int(occ.c24010030)               AS occ_skilled_trades_m,
  (viz.clean_dec(occ.c24010030_moe) / 1.645) AS occ_skilled_trades_m_se,
  viz.clean_int(occ.c24010066)               AS occ_skilled_trades_f,
  (viz.clean_dec(occ.c24010066_moe) / 1.645) AS occ_skilled_trades_f_se,

  viz.clean_int(occ.c24010034)               AS occ_blue_collar_m,
  (viz.clean_dec(occ.c24010034_moe) / 1.645) AS occ_blue_collar_m_se,
  viz.clean_int(occ.c24010070)               AS occ_blue_collar_f,
  (viz.clean_dec(occ.c24010070_moe) / 1.645) AS occ_blue_collar_f_se

FROM viz.geoid_base g
LEFT JOIN acs2024_5yr.c24010_moe occ USING (geoid);

ALTER TABLE viz.occupation_base
ADD CONSTRAINT occupation_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);