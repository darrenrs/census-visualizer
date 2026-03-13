# Census Visualizer

An ETL pipeline + full-stack website that presents Census data and custom metrics with no fluff.

## Features

- Robust data pipeline from raw Census Reporter American Community Survey (ACS) data into Postgres offering support for more than 400,000 geographies
- Derived metrics such as upper income percentiles using Pareto curves and normalized education, racial/ethnic diversity, and occupation indices
- 90% margin of errors for all computable fields from Census variance replicate tables or Monte Carlo simulation
- Node/Express backend, React/Vite frontend

## Build Instructions

### Running the Pipeline

(This will be integrated into one Makefile soon. From a clean slate this entire process will take about 30 min to complete.)

1. Ensure Postgres is installed and the [Census Reporter ACS data dump](https://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data/amp) has been loaded into a new schema.
2. Run `pipeline/vre/download_vre_tables.py`
3. Run each of the scripts in `pipeline/sql` and `pipeline/python` in order based on their number.
4. Run `pipeline/geo/build_geo.sh` (a sample for sumlevel `050` is provided as a ground truth example)

- Prereqs: curl, unzip, ogrinfo, ogr2ogr

### Running the Website

1. `cp .env.example .env` and set values
2. `cd web/server`
3. `npm install`
4. `npm run dev`
5. `cd web/client`
6. `npm install`
7. `npm run dev`
8. Open `http://127.0.0.1:5173`

Default local URLs:

- Frontend: `http://127.0.0.1:5173`
- API: `http://127.0.0.1:3000`

Port/base path override knobs in root `.env`:

- `PORT_SERVER` for API server
- `PORT_CLIENT` for Vite dev server
- `VITE_BASE_PATH` for frontend mount path
- `VITE_API_BASE` for browser API base path
- `VITE_GEO_BASE` for browser GeoJSON base path (local `/geo` or remote URL)
- `GEO_ASSET_MODE` with `local|remote` (Express serves local geo only when `local`)
- `GEO_ASSET_DIR` local directory mounted to `VITE_GEO_BASE` (default `pipeline/geo/out`)

In local mode, Express serves GeoJSON from `pipeline/geo/out` at `/geo` by default.

#### API

The Express API currently exposes:

- `GET /api/v1/geographies`
  - Gets list and count of all geography summary levels
- `GET /api/v1/geography/:geoid`
  - Path params:
    - `geoid` (required)
  - Response keys:
    - `geography`, `core`, `income`, `education`, `diversity`, `occupation`
    - `geography` and `core` are always available. The others may be `null` for certain geographies.

## Metrics

Metrics are listed with their ACS table if applicable. Flags are used to describe data quality errors.

### Geography

- GEOID (Primary Key)
- Vintage (e.g., `acs2024_5yr`)
- Summary Level (integer)
- Name
- State Code (e.g., `CA`)

#### Summary Levels

- 010: United States of America
- 040: State or State-equivalent
- 050: County or County-equivalent
- 060: County Subdivision
- 140: Tract
- 150: Block Group
- 160: Place
- 310: Metropolitan or Micropolitan Statistical Area
- 500: Congressional District (119th Congress)
- 860: Zip Code Tabulation Area

### Core Demographics

- Total Population (B01003)
- Total Households (B11001)
- Average Household Size (B25010)

### Income

_Household income percentile extremes and confidence intervals estimated by Pareto distribution function._

- Median Household Income (B19013)
- Household Income Thresholds at percentiles 20, 40, 60, 80, and 95 (B19080)
  - _Census topcodes values above $250,000 as "250001"_
- Mean Household Income of quintiles 2, 3, 4, 5 and top 5% (B19081)
  - _These values are not topcoded_
- Gini Index of Income Inequality (B19083)

### Education

_Normalized index in the range 0-100, plus estimated years of schooling. Confidence intervals derived from Variance Replicate Estimate tables._

- Educational Attainment by Sex (B15002)

### Racial/Ethnic Diversity

_Normalized index in the range 0-100. Confidence intervals derived from Variance Replicate Estimate tables._

- Hispanic or Latino Origin by Race (B03002)

### Occupational Diversity

_Five root occupational groups, twenty-five leaf occupational groups, ratio of basic to extended index. Confidence intervals derived from Variance Replicate Estimate tables._

- Detailed Occupation Breakdown (C24010)

## Todo (2026-03-12)

### Features

_(High: Required for v1)_

- High: Add custom vector tiles (via CDN) to support devices that have a normal amount of RAM
- Medium: Show the full path of a geography (BG in this County, which is in this State...)
- Medium: Add simple geography search
- Medium: Fill in about page (FAQ/Privacy Policy)
- Medium: Add LICENSE
- Medium: Make vintage name completely non-hardcoded (input from .env)
- Low: Display a "long name" shorter than the full description in geography header
- Low: Add percentile ranks grouped by geography
- Low: Add separate pages for "leaderboards"
- Low: Add support for 1yr vintage
- Low: Add comparisons between geographies
- Low: Add more attributes, introduce clustering algorithms

### Bugs

- Medium Priority: Disconnected from WiFi on iPhone browser (leaving LAN), then attempted to load a geography. Got stuck on "Loading" forever. Upon reconnect page immediately refreshed and was fine. Not sure what this means for "normal connection lost". (Addendum: also tested "API server is just down" and it worked normally. The weird behavior was only when disconnected from LAN.)
- Low Priority: If an offline error occurred, trying to load the same GEOID will not trigger anything even if network reconnects. User can load another geography then go back to the original one and both will work, it's just the initial state seems to be stuck if you try to reload same one.

## Acknowledgements

- U.S. Census Bureau
- Census Reporter
