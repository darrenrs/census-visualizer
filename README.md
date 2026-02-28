# Census Visualizer

You're seeing an early commit of this website.

## Features

- Robust data pipeline from raw Census Reporter ACS data into Sqlite
- Precomputed margin of errors from variance replicate tables (if available) or analytic function
- Node/Express backend, MapLibre frontend

## Sumlevels

- 010: Nation
- 040: State
- 050: County
- 060: County Subdivision
- 140: Tract
- 150: Block Group
- 160: Place
- 310: MSA
- 500: Congressional District
- 860: Zip Code

## Metrics

### Descriptive

- GEOID (PK)
- Name
- Summary Level
- Stusab (State/Territory Code)
- Population (B01003)
- Households (B11001)

### Income Tail Distribution

_Household income percentile extremes and confidence intervals estimated by Pareto distribution function._

- Median Household Income (B19013)
- Household Income Thresholds P20, P40, P60, P80, P95 (B19080)
- Mean Household Income of Quintiles 1, 2, 3, 4, 5% and top 5% (B19081)
- Share of Aggregate Household Income by Quintiles 1, 2, 3, 4, 5, and top 5% (B19082)

### Gini Index

_Measure of income inequality. Confidence intervals provided._

- Gini Index (B19083)

### Education Index

_Normalized index in the range 0-100, plus estimated years of schooling. Confidence intervals derived from Variance Replicate Tables._

- Educational Attainment (B15002)

### Occupational Diversity

_Confidence intervals derived from Variance Replicate Tables._

- Five High Level Groups for Occupations (C24010)

### Racial/Ethnic Diversity

- Categories (B03002)
