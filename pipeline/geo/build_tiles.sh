#!/usr/bin/env bash
set -euo pipefail

# Build PMTiles archives from enriched GeoJSON outputs.
#
# Inputs:
# - GeoJSON files named 010.geojson, 040.geojson, ... in GEOJSON_DIR
# Outputs:
# - PMTiles archives in PMTILES_DIR
# - intermediate MBTiles archives in MBTILES_DIR
#
# Environment overrides:
#   GEOJSON_DIR=/path/to/geojson
#   MBTILES_DIR=/path/to/mbtiles
#   PMTILES_DIR=/path/to/pmtiles
#   SUMLEVELS="010 040 050"
#
# Usage:
#   ./pipeline/geo/build_tiles.sh
#   SUMLEVELS="050 140 150" ./pipeline/geo/build_tiles.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEO_DIR="$ROOT_DIR/pipeline/geo"

DEFAULT_GEOJSON_DIR="$GEO_DIR/out/geojson"
DEFAULT_MBTILES_DIR="$GEO_DIR/work/mbtiles"
DEFAULT_PMTILES_DIR="$GEO_DIR/out/pmtiles"

GEOJSON_DIR="${GEOJSON_DIR:-$DEFAULT_GEOJSON_DIR}"
MBTILES_DIR="${MBTILES_DIR:-$DEFAULT_MBTILES_DIR}"
PMTILES_DIR="${PMTILES_DIR:-$DEFAULT_PMTILES_DIR}"
SUMLEVELS="${SUMLEVELS:-010 040 050 060 140 150 160 310 500 860}"

mkdir -p "$MBTILES_DIR"
mkdir -p "$PMTILES_DIR"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd tippecanoe
require_cmd pmtiles

zooms_for_sumlevel() {
  local sumlevel="$1"
  # Must match values in web/client/src/MapViewer.tsx
  case "$sumlevel" in
    010) echo "0 8" ;;
    040) echo "0 9" ;;
    050) echo "2 11" ;;
    060) echo "4 12" ;;
    140) echo "5 13" ;;
    150) echo "6 13" ;;
    160) echo "4 12" ;;
    310) echo "2 10" ;;
    500) echo "2 11" ;;
    860) echo "5 13" ;;
    *)
      echo "Unknown sumlevel: $sumlevel" >&2
      exit 1
      ;;
  esac
}

echo "GeoJSON dir: $GEOJSON_DIR"
echo "MBTiles dir: $MBTILES_DIR"
echo "PMTiles dir: $PMTILES_DIR"

for sumlevel in $SUMLEVELS; do
  input_path="$GEOJSON_DIR/${sumlevel}.geojson"
  mbtiles_path="$MBTILES_DIR/${sumlevel}.mbtiles"
  pmtiles_path="$PMTILES_DIR/${sumlevel}.pmtiles"
  layer_name="geo_${sumlevel}"

  if [[ ! -f "$input_path" ]]; then
    echo "Skipping [$sumlevel]: missing input $input_path"
    continue
  fi

  zooms="$(zooms_for_sumlevel "$sumlevel")"
  zmin="${zooms%% *}"
  zmax="${zooms##* }"

  echo "==> [$sumlevel] Building $mbtiles_path (z${zmin}-z${zmax}, layer=$layer_name)"
  tippecanoe \
    -o "$mbtiles_path" \
    -f \
    -l "$layer_name" \
    -Z "$zmin" \
    -z "$zmax" \
    --drop-densest-as-needed \
    --extend-zooms-if-still-dropping \
    --detect-shared-borders \
    -pk \
    -pf \
    -y GEOID \
    -y NAME \
    -y STATE_CODE \
    "$input_path"

  echo "==> [$sumlevel] Converting to $pmtiles_path"
  rm -f "$pmtiles_path"
  pmtiles convert "$mbtiles_path" "$pmtiles_path"

done

echo "Done. PMTiles outputs are in: $PMTILES_DIR"
