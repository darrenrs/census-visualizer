export type ApiErrorResponse = {
  error: string;
};

export type HealthResponse = {
  ok: true;
};

export type Sumlevel = 10 | 40 | 50 | 60 | 140 | 150 | 160 | 310 | 500 | 860;

export type GeographyListItem = {
  sumlevel: Sumlevel;
  label: string;
  long_label: string;
  description: string;
  geography_count: number;
};

export type GeographyListResponse = {
  vintage: string;
  geographies: GeographyListItem[];
};

export type GeographyMeta = {
  geoid: string;
  name: string;
  vintage: string;
  state_code: string | null;
  sumlevel: Sumlevel;
};

export type CoreResponse = {
  total_population: number;
  total_population_lo90: number | null;
  total_population_hi90: number | null;
  total_households: number;
  total_households_lo90: number | null;
  total_households_hi90: number | null;
  avg_household_size: number | null;
  avg_household_size_lo90: number | null;
  avg_household_size_hi90: number | null;
};

export type IncomeResponse = {
  hhi_median: number | null;
  hhi_p20: number | null;
  hhi_p40: number | null;
  hhi_p60: number | null;
  hhi_p80: number | null;
  hhi_p95: number | null;
  hhi_median_lo90: number | null;
  hhi_p20_lo90: number | null;
  hhi_p40_lo90: number | null;
  hhi_p60_lo90: number | null;
  hhi_p80_lo90: number | null;
  hhi_p95_lo90: number | null;
  hhi_median_hi90: number | null;
  hhi_p20_hi90: number | null;
  hhi_p40_hi90: number | null;
  hhi_p60_hi90: number | null;
  hhi_p80_hi90: number | null;
  hhi_p95_hi90: number | null;
  hhi_sim_p90: number | null;
  hhi_sim_p95: number | null;
  hhi_sim_p99: number | null;
  hhi_sim_p999: number | null;
  hhi_sim_p90_lo90: number | null;
  hhi_sim_p95_lo90: number | null;
  hhi_sim_p99_lo90: number | null;
  hhi_sim_p999_lo90: number | null;
  hhi_sim_p90_hi90: number | null;
  hhi_sim_p95_hi90: number | null;
  hhi_sim_p99_hi90: number | null;
  hhi_sim_p999_hi90: number | null;
  hhi_gini: number | null;
  hhi_gini_lo90: number | null;
  hhi_gini_hi90: number | null;
  hhi_sim_anchor: number | null;
  flags: number;
};

export type EducationResponse = {
  edu_education_index: number | null;
  edu_education_index_lo90: number | null;
  edu_education_index_hi90: number | null;
  edu_years_of_school: number | null;
  edu_years_of_school_lo90: number | null;
  edu_years_of_school_hi90: number | null;
  flags: number;
};

export type DiversityResponse = {
  race_diversity_index: number | null;
  race_diversity_index_lo90: number | null;
  race_diversity_index_hi90: number | null;
  flags: number;
};

export type OccupationResponse = {
  occ_occupation_index: number | null;
  occ_occupation_index_lo90: number | null;
  occ_occupation_index_hi90: number | null;
  occ_occupation_index_ext: number | null;
  occ_occupation_index_ext_lo90: number | null;
  occ_occupation_index_ext_hi90: number | null;
  occ_occupation_index_ratio: number | null;
  occ_occupation_index_ratio_lo90: number | null;
  occ_occupation_index_ratio_hi90: number | null;
  flags: number;
};

export type GeographyResponse = {
  geography: GeographyMeta;
  core: CoreResponse;
  income: IncomeResponse | null;
  education: EducationResponse | null;
  diversity: DiversityResponse | null;
  occupation: OccupationResponse | null;
};
