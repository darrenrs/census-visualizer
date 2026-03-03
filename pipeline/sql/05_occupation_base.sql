DROP TABLE IF EXISTS viz.occupation_base;

CREATE TABLE viz.occupation_base AS
SELECT
  g.vintage,
  g.sumlevel,
  g.geoid,

  -- Table C24010: Detailed Occupation Breakdown for employed persons aged 16 and over
  viz.clean_int(occ.c24010001)               AS occ_population,
  (viz.clean_dec(occ.c24010001_moe) / 1.645) AS occ_population_se,

  -- Root Occupations (5) --
  viz.clean_int(occ.c24010003)               AS occ_root_professional_m,
  (viz.clean_dec(occ.c24010003_moe) / 1.645) AS occ_root_professional_m_se,
  viz.clean_int(occ.c24010039)               AS occ_root_professional_f,
  (viz.clean_dec(occ.c24010039_moe) / 1.645) AS occ_root_professional_f_se,

  viz.clean_int(occ.c24010019)               AS occ_root_service_m,
  (viz.clean_dec(occ.c24010019_moe) / 1.645) AS occ_root_service_m_se,
  viz.clean_int(occ.c24010055)               AS occ_root_service_f,
  (viz.clean_dec(occ.c24010055_moe) / 1.645) AS occ_root_service_f_se,

  viz.clean_int(occ.c24010027)               AS occ_root_sales_m,
  (viz.clean_dec(occ.c24010027_moe) / 1.645) AS occ_root_sales_m_se,
  viz.clean_int(occ.c24010063)               AS occ_root_sales_f,
  (viz.clean_dec(occ.c24010063_moe) / 1.645) AS occ_root_sales_f_se,

  viz.clean_int(occ.c24010030)               AS occ_root_skilled_trades_m,
  (viz.clean_dec(occ.c24010030_moe) / 1.645) AS occ_root_skilled_trades_m_se,
  viz.clean_int(occ.c24010066)               AS occ_root_skilled_trades_f,
  (viz.clean_dec(occ.c24010066_moe) / 1.645) AS occ_root_skilled_trades_f_se,

  viz.clean_int(occ.c24010034)               AS occ_root_blue_collar_m,
  (viz.clean_dec(occ.c24010034_moe) / 1.645) AS occ_root_blue_collar_m_se,
  viz.clean_int(occ.c24010070)               AS occ_root_blue_collar_f,
  (viz.clean_dec(occ.c24010070_moe) / 1.645) AS occ_root_blue_collar_f_se,

  -- Leaf Occupations (25) --
  viz.clean_int(occ.c24010005)               AS occ_leaf_mgmt_m,
  (viz.clean_dec(occ.c24010005_moe) / 1.645) AS occ_leaf_mgmt_m_se,
  viz.clean_int(occ.c24010041)               AS occ_leaf_mgmt_f,
  (viz.clean_dec(occ.c24010041_moe) / 1.645) AS occ_leaf_mgmt_f_se,

  viz.clean_int(occ.c24010006)               AS occ_leaf_bus_fin_m,
  (viz.clean_dec(occ.c24010006_moe) / 1.645) AS occ_leaf_bus_fin_m_se,
  viz.clean_int(occ.c24010042)               AS occ_leaf_bus_fin_f,
  (viz.clean_dec(occ.c24010042_moe) / 1.645) AS occ_leaf_bus_fin_f_se,
 
  viz.clean_int(occ.c24010008)               AS occ_leaf_comp_math_m,
  (viz.clean_dec(occ.c24010008_moe) / 1.645) AS occ_leaf_comp_math_m_se,
  viz.clean_int(occ.c24010044)               AS occ_leaf_comp_math_f,
  (viz.clean_dec(occ.c24010044_moe) / 1.645) AS occ_leaf_comp_math_f_se,
 
  viz.clean_int(occ.c24010009)               AS occ_leaf_eng_m,
  (viz.clean_dec(occ.c24010009_moe) / 1.645) AS occ_leaf_eng_m_se,
  viz.clean_int(occ.c24010045)               AS occ_leaf_eng_f,
  (viz.clean_dec(occ.c24010045_moe) / 1.645) AS occ_leaf_eng_f_se,
 
  viz.clean_int(occ.c24010010)               AS occ_leaf_sci_m,
  (viz.clean_dec(occ.c24010010_moe) / 1.645) AS occ_leaf_sci_m_se,
  viz.clean_int(occ.c24010046)               AS occ_leaf_sci_f,
  (viz.clean_dec(occ.c24010046_moe) / 1.645) AS occ_leaf_sci_f_se,

  viz.clean_int(occ.c24010012)               AS occ_leaf_soc_svc_m,
  (viz.clean_dec(occ.c24010012_moe) / 1.645) AS occ_leaf_soc_svc_m_se,
  viz.clean_int(occ.c24010048)               AS occ_leaf_soc_svc_f,
  (viz.clean_dec(occ.c24010048_moe) / 1.645) AS occ_leaf_soc_svc_f_se,

  viz.clean_int(occ.c24010013)               AS occ_leaf_legal_m,
  (viz.clean_dec(occ.c24010013_moe) / 1.645) AS occ_leaf_legal_m_se,
  viz.clean_int(occ.c24010049)               AS occ_leaf_legal_f,
  (viz.clean_dec(occ.c24010049_moe) / 1.645) AS occ_leaf_legal_f_se,

  viz.clean_int(occ.c24010014)               AS occ_leaf_edu_m,
  (viz.clean_dec(occ.c24010014_moe) / 1.645) AS occ_leaf_edu_m_se,
  viz.clean_int(occ.c24010050)               AS occ_leaf_edu_f,
  (viz.clean_dec(occ.c24010050_moe) / 1.645) AS occ_leaf_edu_f_se,

  viz.clean_int(occ.c24010015)               AS occ_leaf_ent_m,
  (viz.clean_dec(occ.c24010015_moe) / 1.645) AS occ_leaf_ent_m_se,
  viz.clean_int(occ.c24010051)               AS occ_leaf_ent_f,
  (viz.clean_dec(occ.c24010051_moe) / 1.645) AS occ_leaf_ent_f_se,

  viz.clean_int(occ.c24010017)               AS occ_leaf_med_prac_m,
  (viz.clean_dec(occ.c24010017_moe) / 1.645) AS occ_leaf_med_prac_m_se,
  viz.clean_int(occ.c24010053)               AS occ_leaf_med_prac_f,
  (viz.clean_dec(occ.c24010053_moe) / 1.645) AS occ_leaf_med_prac_f_se,

  viz.clean_int(occ.c24010018)               AS occ_leaf_med_tech_m,
  (viz.clean_dec(occ.c24010018_moe) / 1.645) AS occ_leaf_med_tech_m_se,
  viz.clean_int(occ.c24010054)               AS occ_leaf_med_tech_f,
  (viz.clean_dec(occ.c24010054_moe) / 1.645) AS occ_leaf_med_tech_f_se,

  viz.clean_int(occ.c24010020)               AS occ_leaf_med_svc_m,
  (viz.clean_dec(occ.c24010020_moe) / 1.645) AS occ_leaf_med_svc_m_se,
  viz.clean_int(occ.c24010056)               AS occ_leaf_med_svc_f,
  (viz.clean_dec(occ.c24010056_moe) / 1.645) AS occ_leaf_med_svc_f_se,

  viz.clean_int(occ.c24010022)               AS occ_leaf_fire_m,
  (viz.clean_dec(occ.c24010022_moe) / 1.645) AS occ_leaf_fire_m_se,
  viz.clean_int(occ.c24010058)               AS occ_leaf_fire_f,
  (viz.clean_dec(occ.c24010058_moe) / 1.645) AS occ_leaf_fire_f_se,

  viz.clean_int(occ.c24010023)               AS occ_leaf_leo_m,
  (viz.clean_dec(occ.c24010023_moe) / 1.645) AS occ_leaf_leo_m_se,
  viz.clean_int(occ.c24010059)               AS occ_leaf_leo_f,
  (viz.clean_dec(occ.c24010059_moe) / 1.645) AS occ_leaf_leo_f_se,

  viz.clean_int(occ.c24010024)               AS occ_leaf_food_m,
  (viz.clean_dec(occ.c24010024_moe) / 1.645) AS occ_leaf_food_m_se,
  viz.clean_int(occ.c24010060)               AS occ_leaf_food_f,
  (viz.clean_dec(occ.c24010060_moe) / 1.645) AS occ_leaf_food_f_se,

  viz.clean_int(occ.c24010025)               AS occ_leaf_custod_maint_m,
  (viz.clean_dec(occ.c24010025_moe) / 1.645) AS occ_leaf_custod_maint_m_se,
  viz.clean_int(occ.c24010061)               AS occ_leaf_custod_maint_f,
  (viz.clean_dec(occ.c24010061_moe) / 1.645) AS occ_leaf_custod_maint_f_se,

  viz.clean_int(occ.c24010026)               AS occ_leaf_persnl_care_m,
  (viz.clean_dec(occ.c24010026_moe) / 1.645) AS occ_leaf_persnl_care_m_se,
  viz.clean_int(occ.c24010062)               AS occ_leaf_persnl_care_f,
  (viz.clean_dec(occ.c24010062_moe) / 1.645) AS occ_leaf_persnl_care_f_se,

  viz.clean_int(occ.c24010028)               AS occ_leaf_sales_m,
  (viz.clean_dec(occ.c24010028_moe) / 1.645) AS occ_leaf_sales_m_se,
  viz.clean_int(occ.c24010064)               AS occ_leaf_sales_f,
  (viz.clean_dec(occ.c24010064_moe) / 1.645) AS occ_leaf_sales_f_se,

  viz.clean_int(occ.c24010029)               AS occ_leaf_office_supt_m,
  (viz.clean_dec(occ.c24010029_moe) / 1.645) AS occ_leaf_office_supt_m_se,
  viz.clean_int(occ.c24010065)               AS occ_leaf_office_supt_f,
  (viz.clean_dec(occ.c24010065_moe) / 1.645) AS occ_leaf_office_supt_f_se,

  viz.clean_int(occ.c24010031)               AS occ_leaf_ag_m,
  (viz.clean_dec(occ.c24010031_moe) / 1.645) AS occ_leaf_ag_m_se,
  viz.clean_int(occ.c24010067)               AS occ_leaf_ag_f,
  (viz.clean_dec(occ.c24010067_moe) / 1.645) AS occ_leaf_ag_f_se,

  viz.clean_int(occ.c24010032)               AS occ_leaf_const_m,
  (viz.clean_dec(occ.c24010032_moe) / 1.645) AS occ_leaf_const_m_se,
  viz.clean_int(occ.c24010068)               AS occ_leaf_const_f,
  (viz.clean_dec(occ.c24010068_moe) / 1.645) AS occ_leaf_const_f_se,

  viz.clean_int(occ.c24010033)               AS occ_leaf_maint_m,
  (viz.clean_dec(occ.c24010033_moe) / 1.645) AS occ_leaf_maint_m_se,
  viz.clean_int(occ.c24010069)               AS occ_leaf_maint_f,
  (viz.clean_dec(occ.c24010069_moe) / 1.645) AS occ_leaf_maint_f_se,

  viz.clean_int(occ.c24010035)               AS occ_leaf_prod_m,
  (viz.clean_dec(occ.c24010035_moe) / 1.645) AS occ_leaf_prod_m_se,
  viz.clean_int(occ.c24010071)               AS occ_leaf_prod_f,
  (viz.clean_dec(occ.c24010071_moe) / 1.645) AS occ_leaf_prod_f_se,

  viz.clean_int(occ.c24010036)               AS occ_leaf_trans_m,
  (viz.clean_dec(occ.c24010036_moe) / 1.645) AS occ_leaf_trans_m_se,
  viz.clean_int(occ.c24010072)               AS occ_leaf_trans_f,
  (viz.clean_dec(occ.c24010072_moe) / 1.645) AS occ_leaf_trans_f_se,

  viz.clean_int(occ.c24010037)               AS occ_leaf_mat_mov_m,
  (viz.clean_dec(occ.c24010037_moe) / 1.645) AS occ_leaf_mat_mov_m_se,
  viz.clean_int(occ.c24010073)               AS occ_leaf_mat_mov_f,
  (viz.clean_dec(occ.c24010073_moe) / 1.645) AS occ_leaf_mat_mov_f_se

FROM viz.geoid_base g
LEFT JOIN acs2024_5yr.c24010_moe occ USING (geoid);

ALTER TABLE viz.occupation_base
ADD CONSTRAINT occupation_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);