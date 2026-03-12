import { useEffect, useRef, useState } from "react";
import maplibregl, {
  GeoJSONSource,
  type MapLayerMouseEvent,
} from "maplibre-gl";
import type { FeatureCollection, Geometry } from "geojson";
import { type Sumlevel } from "@/types/api.ts";

type MapViewerProps = {
  sumlevel: Sumlevel;
  onSelectGeoid: (geoid: string) => void;
};

// load this blank data initially to prevent geographyDataUrl from being in the dependency array
const INIT_FEATURE_COLLECTION: FeatureCollection<Geometry> = {
  type: "FeatureCollection",
  features: [],
};

// Sumlevel is an int, convert it to a padded string for URL
function normalizeSumlevel(sumlevel: Sumlevel) {
  const paddedSumlevel = String(sumlevel).padStart(3, "0");
  return paddedSumlevel;
}

// GEOIDs have to be adjusted for some reason
function normalizeGeoid(geoid: string) {
  const firstThreeChars = geoid.slice(0, 3);

  switch (firstThreeChars) {
    case "310":
      return geoid.replace("310M700US", "31000US");
    case "500":
      return geoid.replace("5001900US", "50000US");
    case "860":
      return geoid.replace("860Z200US", "86000US");
    default:
      return geoid.replace("00000US", "000US");
  }
}

export function MapViewer({ onSelectGeoid, sumlevel }: MapViewerProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const geoBase = (import.meta.env.VITE_GEO_BASE || "/geo").replace(/\/+$/, "");
  const geographiesDataUrl = `${geoBase}/${normalizeSumlevel(sumlevel)}.geojson`;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: {
        version: 8,
        sources: {
          osm: {
            type: "raster",
            tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "© OpenStreetMap contributors",
          },
        },
        layers: [
          {
            id: "osm",
            type: "raster",
            source: "osm",
          },
        ],
      },
      center: [-98.5795, 39.8283],
      zoom: 3,
    });

    map.on("error", (e) => {
      console.error("MapLibre error:", e.error);
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      map.addSource("geographies", {
        type: "geojson",
        data: INIT_FEATURE_COLLECTION,
      });

      map.addLayer({
        id: "geographies-fill",
        type: "fill",
        source: "geographies",
        paint: {
          "fill-color": "#2b715b",
          "fill-opacity": 0.15,
        },
      });

      map.addLayer({
        id: "geographies-line",
        type: "line",
        source: "geographies",
        paint: {
          "line-color": "#4b5563",
          "line-width": 1.5,
          "line-opacity": 0.3,
        },
      });

      map.on("mouseenter", "geographies-fill", () => {
        map.getCanvas().style.cursor = "pointer";
      });

      map.on("mouseleave", "geographies-fill", () => {
        map.getCanvas().style.cursor = "";
      });

      map.on("click", "geographies-fill", (e: MapLayerMouseEvent) => {
        const feature = e.features?.[0];
        const clickedGeoid = feature?.properties?.GEOIDFQ;
        if (!clickedGeoid || typeof clickedGeoid !== "string") return;
        onSelectGeoid(normalizeGeoid(clickedGeoid));
      });
      setMapLoaded(true);
    });

    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
      setMapLoaded(false);
    };
  }, [onSelectGeoid]);

  useEffect(() => {
    if (!mapLoaded) return;

    const map = mapRef.current;
    if (!map) return;

    const source = map.getSource("geographies") as GeoJSONSource | undefined;
    if (!source) return;

    source.setData(geographiesDataUrl);
  }, [geographiesDataUrl, mapLoaded]);

  return (
    <section className="map-container" aria-label="Map">
      <div ref={mapContainerRef} className="map-canvas" />
    </section>
  );
}
