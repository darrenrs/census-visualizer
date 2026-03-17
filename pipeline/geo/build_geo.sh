#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEO_DIR="$ROOT_DIR/pipeline/geo"
WORK_DIR="$GEO_DIR/work"
ZIP_DIR="$WORK_DIR/zips"
RAW_DIR="$WORK_DIR/raw"
MBTILES_DIR="$WORK_DIR/mbtiles"
GEOID_WORK_DIR="$WORK_DIR/geoid"
OUT_DIR="$GEO_DIR/out"
OUT_GEOJSON_DIR="$OUT_DIR/geojson"
OUT_PMTILES_DIR="$OUT_DIR/pmtiles"
ROOT_ENV_FILE="$ROOT_DIR/.env"

mkdir -p \
  "$ZIP_DIR" \
  "$RAW_DIR" \
  "$MBTILES_DIR" \
  "$GEOID_WORK_DIR" \
  "$OUT_GEOJSON_DIR" \
  "$OUT_PMTILES_DIR"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd curl
require_cmd unzip
require_cmd ogrinfo
require_cmd ogr2ogr
require_cmd psql

if [[ -f "$ROOT_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ROOT_ENV_FILE"
  set +a
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required. Set it in environment or $ROOT_ENV_FILE." >&2
  exit 1
fi

if [[ -z "${VINTAGE:-}" ]]; then
  echo "VINTAGE is required (set in .env or environment)." >&2
  exit 1
fi

# sumlevel|url
# Updated through 2024 (ZCTA are still stuck in 2020)
MAPS=(
  "010|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_nation_5m.zip"
  "040|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_state_500k.zip"
  "050|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"
  "060|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_cousub_500k.zip"
  "140|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_tract_500k.zip"
  "150|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_bg_500k.zip"
  "160|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_place_500k.zip"
  "310|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_cbsa_500k.zip"
  "500|https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_cd119_500k.zip"
  "860|https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip"
)

has_field() {
  local meta="$1"
  local field="$2"
  grep -Eiq "^[[:space:]]*${field}[[:space:]]*:" <<<"$meta"
}

for row in "${MAPS[@]}"; do
  IFS="|" read -r sumlevel url <<<"$row"

  zip_path="$ZIP_DIR/${sumlevel}.zip"
  raw_path="$RAW_DIR/${sumlevel}"
  tmp_gpkg="$GEOID_WORK_DIR/${sumlevel}.gpkg"
  lookup_csv="$GEOID_WORK_DIR/${sumlevel}_lookup.csv"
  out_path="$OUT_GEOJSON_DIR/${sumlevel}.geojson"

  echo "==> [$sumlevel] Downloading"
  curl -fL "$url" -o "$zip_path"

  echo "==> [$sumlevel] Extracting"
  rm -rf "$raw_path"
  mkdir -p "$raw_path"
  unzip -oq "$zip_path" -d "$raw_path"

  shp_path="$(find "$raw_path" -maxdepth 1 -name '*.shp' | head -n1)"
  if [[ -z "${shp_path:-}" ]]; then
    echo "No .shp found for sumlevel $sumlevel" >&2
    exit 1
  fi

  layer="$(basename "$shp_path" .shp)"
  meta="$(ogrinfo -so "$shp_path" "$layer")"

  geoid_raw_expr="NULL"

  if [[ "$sumlevel" == "860" ]]; then
    if has_field "$meta" "AFFGEOID20"; then
      geoid_raw_expr="AFFGEOID20"
    elif has_field "$meta" "GEOID20"; then
      geoid_raw_expr="'${sumlevel}00US' || GEOID20"
    fi
  else
    if has_field "$meta" "GEOIDFQ"; then
      geoid_raw_expr="GEOIDFQ"
    elif has_field "$meta" "AFFGEOID"; then
      geoid_raw_expr="AFFGEOID"
    elif has_field "$meta" "GEOID"; then
      geoid_raw_expr="'${sumlevel}00US' || GEOID"
    fi
  fi

  if [[ "$geoid_raw_expr" == "NULL" ]]; then
    echo "Could not determine source GEOID field for sumlevel $sumlevel" >&2
    exit 1
  fi

  normalize_sql="
    SELECT
      CASE
        WHEN ${geoid_raw_expr} IS NULL THEN NULL
        ELSE substr(${geoid_raw_expr}, 1, 3) || substr(${geoid_raw_expr}, 6)
      END AS geoid_norm,
      geometry
    FROM \"${layer}\"
  "

  echo "==> [$sumlevel] Building normalized geometry layer"
  rm -f "$tmp_gpkg" "$lookup_csv"
  ogr2ogr \
    -f GPKG \
    -t_srs EPSG:4326 \
    -dialect SQLite \
    -sql "$normalize_sql" \
    -nln tiger_norm \
    -overwrite \
    "$tmp_gpkg" \
    "$shp_path"

  echo "==> [$sumlevel] Exporting SQL lookup from api.geoid_v1"
  vintage_sql="${VINTAGE//\'/\'\'}"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "
    COPY (
      SELECT geoid, name, state_code
      FROM api.geoid_v1
      WHERE vintage = '${vintage_sql}'
        AND sumlevel = ${sumlevel}
    ) TO STDOUT WITH (FORMAT csv, HEADER true)
  " > "$lookup_csv"

  echo "==> [$sumlevel] Loading lookup CSV"
  ogr2ogr \
    -f GPKG \
    "$tmp_gpkg" \
    "$lookup_csv" \
    -nln geoid_lookup \
    -oo AUTODETECT_TYPE=YES \
    -overwrite

  join_sql="
    SELECT
      t.geoid_norm AS GEOID,
      l.name AS NAME,
      l.state_code AS STATE_CODE,
      t.geometry
    FROM tiger_norm t
    INNER JOIN geoid_lookup l
      ON l.geoid = t.geoid_norm
  "

  echo "==> [$sumlevel] Writing final GeoJSON"
  ogr2ogr \
    -f GeoJSON \
    -dialect SQLite \
    -sql "$join_sql" \
    -lco RFC7946=YES \
    "$out_path" \
    "$tmp_gpkg"

  echo "==> [$sumlevel] Wrote $out_path"
done

echo "Done. GeoJSON outputs are in: $OUT_GEOJSON_DIR"
