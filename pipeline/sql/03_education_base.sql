DROP TABLE IF EXISTS viz.education_base;

CREATE TABLE viz.education_base AS
SELECT
  g.vintage,
  g.sumlevel,
  g.geoid,

  -- Table B15002: Educational Attainment for persons aged 25 and over
  viz.clean_int(edu.b15002001)               AS edu_population,
  (viz.clean_dec(edu.b15002001_moe) / 1.645) AS edu_population_se,

  viz.clean_int(edu.b15002003)               AS edu_no_schooling_m,
  (viz.clean_dec(edu.b15002003_moe) / 1.645) AS edu_no_schooling_m_se,
  viz.clean_int(edu.b15002020)               AS edu_no_schooling_f,
  (viz.clean_dec(edu.b15002020_moe) / 1.645) AS edu_no_schooling_f_se,

  viz.clean_int(edu.b15002004)               AS edu_grade_0_4_m,
  (viz.clean_dec(edu.b15002004_moe) / 1.645) AS edu_grade_0_4_m_se,
  viz.clean_int(edu.b15002021)               AS edu_grade_0_4_f,
  (viz.clean_dec(edu.b15002021_moe) / 1.645) AS edu_grade_0_4_f_se,

  viz.clean_int(edu.b15002005)               AS edu_grade_5_6_m,
  (viz.clean_dec(edu.b15002005_moe) / 1.645) AS edu_grade_5_6_m_se,
  viz.clean_int(edu.b15002022)               AS edu_grade_5_6_f,
  (viz.clean_dec(edu.b15002022_moe) / 1.645) AS edu_grade_5_6_f_se,

  viz.clean_int(edu.b15002006)               AS edu_grade_7_8_m,
  (viz.clean_dec(edu.b15002006_moe) / 1.645) AS edu_grade_7_8_m_se,
  viz.clean_int(edu.b15002023)               AS edu_grade_7_8_f,
  (viz.clean_dec(edu.b15002023_moe) / 1.645) AS edu_grade_7_8_f_se,

  viz.clean_int(edu.b15002007)               AS edu_grade_9_m,
  (viz.clean_dec(edu.b15002007_moe) / 1.645) AS edu_grade_9_m_se,
  viz.clean_int(edu.b15002024)               AS edu_grade_9_f,
  (viz.clean_dec(edu.b15002024_moe) / 1.645) AS edu_grade_9_f_se,

  viz.clean_int(edu.b15002008)               AS edu_grade_10_m,
  (viz.clean_dec(edu.b15002008_moe) / 1.645) AS edu_grade_10_m_se,
  viz.clean_int(edu.b15002025)               AS edu_grade_10_f,
  (viz.clean_dec(edu.b15002025_moe) / 1.645) AS edu_grade_10_f_se,

  viz.clean_int(edu.b15002009)               AS edu_grade_11_m,
  (viz.clean_dec(edu.b15002009_moe) / 1.645) AS edu_grade_11_m_se,
  viz.clean_int(edu.b15002026)               AS edu_grade_11_f,
  (viz.clean_dec(edu.b15002026_moe) / 1.645) AS edu_grade_11_f_se,

  viz.clean_int(edu.b15002010)               AS edu_grade_12_m,
  (viz.clean_dec(edu.b15002010_moe) / 1.645) AS edu_grade_12_m_se,
  viz.clean_int(edu.b15002027)               AS edu_grade_12_f,
  (viz.clean_dec(edu.b15002027_moe) / 1.645) AS edu_grade_12_f_se,

  viz.clean_int(edu.b15002011)               AS edu_high_school_m,
  (viz.clean_dec(edu.b15002011_moe) / 1.645) AS edu_high_school_m_se,
  viz.clean_int(edu.b15002028)               AS edu_high_school_f,
  (viz.clean_dec(edu.b15002028_moe) / 1.645) AS edu_high_school_f_se,

  viz.clean_int(edu.b15002012)               AS edu_some_college_less_than_1_year_m,
  (viz.clean_dec(edu.b15002012_moe) / 1.645) AS edu_some_college_less_than_1_year_m_se,
  viz.clean_int(edu.b15002029)               AS edu_some_college_less_than_1_year_f,
  (viz.clean_dec(edu.b15002029_moe) / 1.645) AS edu_some_college_less_than_1_year_f_se,

  viz.clean_int(edu.b15002013)               AS edu_some_college_no_degree_m,
  (viz.clean_dec(edu.b15002013_moe) / 1.645) AS edu_some_college_no_degree_m_se,
  viz.clean_int(edu.b15002030)               AS edu_some_college_no_degree_f,
  (viz.clean_dec(edu.b15002030_moe) / 1.645) AS edu_some_college_no_degree_f_se,

  viz.clean_int(edu.b15002014)               AS edu_associate_degree_m,
  (viz.clean_dec(edu.b15002014_moe) / 1.645) AS edu_associate_degree_m_se,
  viz.clean_int(edu.b15002031)               AS edu_associate_degree_f,
  (viz.clean_dec(edu.b15002031_moe) / 1.645) AS edu_associate_degree_f_se,

  viz.clean_int(edu.b15002015)               AS edu_bachelors_degree_m,
  (viz.clean_dec(edu.b15002015_moe) / 1.645) AS edu_bachelors_degree_m_se,
  viz.clean_int(edu.b15002032)               AS edu_bachelors_degree_f,
  (viz.clean_dec(edu.b15002032_moe) / 1.645) AS edu_bachelors_degree_f_se,

  viz.clean_int(edu.b15002016)               AS edu_masters_degree_m,
  (viz.clean_dec(edu.b15002016_moe) / 1.645) AS edu_masters_degree_m_se,
  viz.clean_int(edu.b15002033)               AS edu_masters_degree_f,
  (viz.clean_dec(edu.b15002033_moe) / 1.645) AS edu_masters_degree_f_se,

  viz.clean_int(edu.b15002017)               AS edu_professional_degree_m,
  (viz.clean_dec(edu.b15002017_moe) / 1.645) AS edu_professional_degree_m_se,
  viz.clean_int(edu.b15002034)               AS edu_professional_degree_f,
  (viz.clean_dec(edu.b15002034_moe) / 1.645) AS edu_professional_degree_f_se,

  viz.clean_int(edu.b15002018)               AS edu_doctorate_degree_m,
  (viz.clean_dec(edu.b15002018_moe) / 1.645) AS edu_doctorate_degree_m_se,
  viz.clean_int(edu.b15002035)               AS edu_doctorate_degree_f,
  (viz.clean_dec(edu.b15002035_moe) / 1.645) AS edu_doctorate_degree_f_se
FROM viz.geoid_base g
LEFT JOIN acs2024_5yr.b15002_moe edu USING (geoid);

ALTER TABLE viz.education_base
ADD CONSTRAINT education_base_pkey PRIMARY KEY (vintage, sumlevel, geoid);