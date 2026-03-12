#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEO_DIR="$ROOT_DIR/pipeline/geo"
ZIP_DIR="$GEO_DIR/zips"
RAW_DIR="$GEO_DIR/raw"
OUT_DIR="$GEO_DIR/out"

mkdir -p "$ZIP_DIR" "$RAW_DIR" "$OUT_DIR"

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
  out_path="$OUT_DIR/${sumlevel}.geojson"

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

  statefp_expr="NULL"
  geoidfq_expr="NULL"
  namelsad_expr="NULL"
  stusps_expr="NULL"

  if [[ "$sumlevel" == "860" ]]; then
    # ZCTA can cross state lines, so keep state-specific fields NULL.
    statefp_expr="NULL"
    stusps_expr="NULL"

    if has_field "$meta" "AFFGEOID20"; then
      geoidfq_expr="AFFGEOID20"
    fi

    # Keep display name as the raw 5-digit ZIP code string.
    if has_field "$meta" "ZCTA5CE20"; then
      namelsad_expr="ZCTA5CE20"
    elif has_field "$meta" "NAME20"; then
      namelsad_expr="NAME20"
    fi
  else
    if has_field "$meta" "STATEFP"; then
      statefp_expr="STATEFP"
    fi

    if has_field "$meta" "GEOIDFQ"; then
      geoidfq_expr="GEOIDFQ"
    elif has_field "$meta" "GEOID"; then
      geoidfq_expr="'${sumlevel}00US' || GEOID"
    fi

    if has_field "$meta" "NAMELSAD"; then
      namelsad_expr="NAMELSAD"
    elif has_field "$meta" "NAME"; then
      namelsad_expr="NAME"
    fi

    if has_field "$meta" "STUSPS"; then
      stusps_expr="STUSPS"
    fi
  fi

  sql="
    SELECT
      ${statefp_expr} AS STATEFP,
      ${geoidfq_expr} AS GEOIDFQ,
      ${namelsad_expr} AS NAMELSAD,
      ${stusps_expr} AS STUSPS,
      '${sumlevel}' AS SUMLEVEL,
      geometry
    FROM \"${layer}\"
  "

  echo "==> [$sumlevel] Converting to GeoJSON"
  ogr2ogr \
    -f GeoJSON \
    -t_srs EPSG:4326 \
    -dialect SQLite \
    -sql "$sql" \
    -lco RFC7946=YES \
    "$out_path" \
    "$shp_path"

  echo "==> [$sumlevel] Wrote $out_path"
done

echo "Done. GeoJSON outputs are in: $OUT_DIR"
