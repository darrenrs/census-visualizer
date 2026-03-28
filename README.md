# Census Visualizer

An ETL pipeline + full-stack website that presents Census data and custom metrics with no fluff.

## Features

- Robust data pipeline from American Community Survey (ACS) data into Postgres offering support for more than 400,000 geographies
- Derived metrics such as upper income percentiles using Pareto curves and normalized education, racial/ethnic diversity, and occupation indices
- 90% margin of errors for all computable fields from Census variance replicate tables or Monte Carlo simulation
- Node/Express API server that gets all information for any geography in one request
- Responsive React/Vite single-page application with MapLibre map utilizing vector tiles (PMTiles) for quick and seamless rendering
- Support for local or remote PMTiles storage

## Build Instructions

### .env

A sample `.env` file is provided at `.env.example`.

- `VINTAGE` for primary/active ACS vintage ID (e.g., `acs2024_5yr`)
- `DATABASE_URL` for the target Postgres database connection URL
- `SOURCE_DATABASE_URL` optional source Postgres database URL for sql-based raw ingest
- `RAW_INGEST_MODE` with `dump|sql`
  - `dump` downloads official ACS Summary File `.dat` files from 2018 and newer to `pipeline/ingest/work/<VINTAGE>` (strongly recommended)
  - `sql` uses a PostgreSQL server with a loaded Census Reporter database dump
  - Note: `VINTAGE` influences raw ACS ingest, downstream pipeline output, and VRE estimates; you'll still need to manually update the URLs in `pipeline/geo/build_geo.sh` for a different set of geography tiles
- `PORT_SERVER` for API server
- `PORT_CLIENT` for Vite dev server
- `VITE_BASE_PATH` for frontend mount path
- `VITE_API_BASE` for browser API base path
- `VITE_GEO_BASE` for browser geo asset base path (local `/geo` or remote URL)
- `GEO_ASSET_MODE` with `local|remote` (Express serves local geo only when `local`)
- `GEO_ASSET_DIR` local directory mounted to `VITE_GEO_BASE` (default `pipeline/geo/out`)

### Running the Pipeline

From a clean slate this entire process will take about 30 min to complete. Tested with Python 3.12.7; if latest version of Python is failing you'll need to set up a virtual environment.

#### Makefile

1. Run `make all`.
2. Or run `make ingest_sql` or `make ingest_dump` explicitly before `make sql_base`.

#### Manual

1. Ensure Postgres is installed.
2. Choose a raw ingest path:
   - SQL mode: set `RAW_INGEST_MODE=sql`, load the Census Reporter ACS dump into a schema named after `VINTAGE`, then run `python3 pipeline/ingest/ingest_sql.py`.
   - Dump mode: set `RAW_INGEST_MODE=dump`, then run `python3 pipeline/ingest/ingest_dump.py` to download and load official ACS Summary File `.dat` files for `acs2018_5yr` and newer.
3. Run `python3 pipeline/vre/download_vre_tables.py`.
   This writes per-vintage VRE files under `pipeline/vre/work/<VINTAGE>/<TABLE>/`.
4. Run each of the scripts in `pipeline/sql` and `pipeline/python` in order based on their number.
5. Run `python3 pipeline/geo/build_geo.sh` (ensure dependencies are installed.)
6. Run `python3 pipeline/geo/build_tiles.sh` (ensure dependencies are installed.)

#### Shell Dependencies

- `build_geo.sh`
  - curl
  - unzip
  - ogrinfo
  - ogr2ogr
  - psql
- `build_tiles.sh`
  - tippecanoe
  - pmtiles

### Running the Website

#### Makefile

1. `cp .env.example .env` and set values
2. Run `make dev`.

#### Manual

1. `cp .env.example .env` and set values
2. `cd web/server`
3. `npm install`
4. `npm run dev` - the API will now be running at `http://localhost:{PORT_SERVER|3000}`
5. `cd web/client`
6. `npm install`
7. `npm run dev` - the frontend will now be running at `http://localhost:{PORT_CLIENT|5173}`

#### API

The Express API currently exposes:

- `GET /api/v1/geographies`
  - Query params:
    - `vintage` (optional, defaults to `VINTAGE`)
  - Gets list and count of all geography summary levels
- `GET /api/v1/geography/:geoid`
  - Path params:
    - `geoid` (required)
  - Query params:
    - `vintage` (optional, defaults to `VINTAGE`)
  - Response keys:
    - `geography`, `core`, `income`, `education`, `diversity`, `occupation`
    - `geography` and `core` are always available. The others may be `null` for certain geographies.

## Metrics

Metrics are listed with their ACS table if applicable. Flags are used to describe data quality errors.

### Geography

- GEOID (Primary Key)
- Vintage (e.g., `acs2024_5yr`)
- Summary Level (integer)
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
- Name
- State Code (e.g., `CA`)

### Core Demographics

- Total Population (B01003)
  - _Margins of Error are missing for large geographies for some reason_
- Total Households (B11001)
- Average Household Size (B25010)

### Income

_Median and selected percentile ranks (observed and extrapolated via Pareto function) for household income and Gini coefficient of income inequality._

#### Tables

- Median Household Income (B19013)
- Household Income Thresholds at percentiles 20, 40, 60, 80, and 95 (B19080)
  - _Census topcodes values above $250,000 as "250001" and are effectively unusable as point estimates_
- Mean Household Income of quintiles 2, 3, 4, 5 and top 5% (B19081)
  - _These values are not topcoded and can be used_
- Gini Index of Income Inequality (B19083)

#### Formulae

Illustrative Pareto tail model:

$$
\Pr(X \ge x) = \left(\frac{x_m}{x}\right)^\alpha
\quad \text{for } x \ge x_m,\ \alpha > 0
$$

which implies the corresponding quantile function:

$$
Q(p) = \frac{x_m}{(1-p)^{1/\alpha}}
\quad \text{for } 0 < p < 1
$$

In the simulation code, this is reparameterized as:

$$
\frac{\mu_s - T_s}{\mu_s} = \frac{1}{\alpha}
$$

where $T_s$ is a simulated threshold draw and $\mu_s$ is a simulated conditional mean draw above that threshold.

Examples of what gets plugged in from the pipeline:

- Standard upper-tail fit above the 95th percentile:
  - $p_0 = 0.95$
  - $T = \texttt{hhi\_p95}$
  - $\mu = \texttt{hhi\_top5\_mean}$
- Standard upper-tail fit above the 80th percentile:
  - $p_0 = 0.80$
  - $T = \texttt{hhi\_p80}$
  - $\mu = \texttt{hhi\_q5\_mean}$
- Means-only fallback when observed upper quantiles are topcoded:
  - $\mu_{80} = \texttt{hhi\_q5\_mean}$
  - $\mu_{95} = \texttt{hhi\_top5\_mean}$
  - then infer a latent $P_{80}$ and extrapolate the tail from there
- Segment repair for the 80th to 95th percentile range:
  - $T_{\mathrm{lo}} = \texttt{hhi\_p80}$
  - $T_{\mathrm{hi}} = \texttt{hhi\_p95}$
  - $p_{\mathrm{lo}} = 0.80$
  - $p_{\mathrm{hi}} = 0.95$
  - use this segment to estimate $P_{90}$

### Education

_Normalized index in the range 0-100, plus estimated years of schooling. Confidence intervals derived from Variance Replicate Estimate tables._

#### Tables

- Educational Attainment by Sex (B15002)

#### Incomes

Illustrative weighted-attainment formula:

$$
\mathrm{EI}
=
\frac{\sum_n w_n \left(M_n + F_n\right)}
{\sum_n \left(M_n + F_n\right)}
$$

where $M_n$ and $F_n$ are the male and female counts in attainment bucket $n$.

In the pipeline, this is implemented with explicit bucket weights such as:

$$
\frac{
0.00(\texttt{edu\_no\_schooling\_m} + \texttt{edu\_no\_schooling\_f})
 + 0.10(\texttt{edu\_grade\_0\_4\_m} + \texttt{edu\_grade\_0\_4\_f})
 + \cdots
 + 3.00(\texttt{edu\_doctorate\_degree\_m} + \texttt{edu\_doctorate\_degree\_f})
}{
\texttt{edu\_population}
}
$$

Estimated years of schooling uses the same structure with year-based weights instead of normalized education-index weights:

$$
\mathrm{YOS}
=
\frac{\sum_n y_n \left(M_n + F_n\right)}
{\sum_n \left(M_n + F_n\right)}
$$

### Racial/Ethnic Diversity

_Normalized index in the range 0-100. Confidence intervals derived from Variance Replicate Estimate tables._

#### Tables

- Hispanic or Latino Origin by Race (B03002)

#### Formulae

Illustrative Simpson diversity formula:

$$
D = 1 - \sum_k p_k^2
$$

where $p_k = c_k / N$ is the share of category $k$ in the total population $N$.

In the pipeline, this is implemented as:

$$
D
=
1
-
\frac{
\texttt{race\_white\_nh}^2
+ \texttt{race\_black\_nh}^2
+ \texttt{race\_aian\_nh}^2
+ (\texttt{race\_asian\_nh} + \texttt{race\_nhpi\_nh})^2
+ (\texttt{race\_other\_nh} + \texttt{race\_multi\_nh})^2
+ \texttt{race\_hispanic}^2
}{
\texttt{total\_population}^2
}
$$

### Occupational Diversity

_Five root occupational groups, twenty-five leaf occupational groups, ratio of basic to extended index. Confidence intervals derived from Variance Replicate Estimate tables._

#### Tables

- Detailed Occupation Breakdown (C24010)

#### Formulae

Illustrative Hill-number concentration formula:

$$
H = \frac{1}{\sum_k p_k^2}
=
\frac{N^2}{\sum_k c_k^2}
$$

where $c_k$ is the count in occupation category $k$ and $N$ is the total employed population represented by the table.

In the pipeline, the root occupation index is implemented as:

$$
\mathrm{OccRoot}
=
\frac{\texttt{occ\_population}^2}{
\sum_{k \in \text{5 root groups}} (M_k + F_k)^2
}
$$

and the extended occupation index is:

$$
\mathrm{OccExt}
=
\frac{\texttt{occ\_population}^2}{
\sum_{k \in \text{25 leaf groups}} (M_k + F_k)^2
}
$$

The reported occupation ratio is then:

$$
\mathrm{OccRatio}
=
\frac{\sum_{k \in \text{5 root groups}} (M_k + F_k)^2}
{\sum_{k \in \text{25 leaf groups}} (M_k + F_k)^2}
=
\frac{\mathrm{OccExt}}{\mathrm{OccRoot}}
$$

## Todo (2026-03-18)

## Improvements

- (v1.1) Remove pipeline file names having numbers
- (v1.1) Add window title
- (v1.1) Add basic favicon
- (v1.1) Show `state_code` in GeographyPanel
- (v1.1) Add simple geography search
- (v1.2) Add address geocoder from Census Geocoder API
- (v1.2) Much more detailed about page with math explanations
- (v1.2) Show full path (block group in X County, in X State ...) in GeographyPanel
- (v1.2) PMTiles updated for each year (actually not too difficult)
- (v1.2+) Add graphs/charts (because they are pretty)

### New Features

- (v1.1; complete) Data ingestion pipeline through online DAT files rather than raw census reporter dump
- (v1.2) Add percentile ranks for attributes
- (v1.2) Add separate pages for leaderboards (e.g., top places by education index)
- (v1.2+) Compare two or more geographies
- (v1.2+) Compare two or more years
- (v1.2+) Add more attributes
- (v1.3) Introduce clustering algorithms (branching into ML now)

### Bugs

_These are also in GitHub Issues_

- (v1.1) Low Priority: If an offline error occurred, trying to load the same GEOID will not trigger anything even if network reconnects. User can load another geography then go back to the original one and both will work, it's just the initial state seems to be stuck if you try to reload same one. This is caused by general behavior of not being able to refresh currently geo.

## Acknowledgements

- U.S. Census Bureau
- Census Reporter
