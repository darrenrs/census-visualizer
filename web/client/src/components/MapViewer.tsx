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
const HOVER_MEDIA_QUERY = "(hover: hover) and (pointer: fine)";

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
  const [tooltip, setTooltip] = useState({
    visible: false,
    label: "",
    x: 0,
    y: 0,
  });
  const geoBase = (import.meta.env.VITE_GEO_BASE || "/geo").replace(/\/+$/, "");
  const geographiesDataUrl = `${geoBase}/${normalizeSumlevel(sumlevel)}.geojson`;
  const isHoverMode =
    typeof window !== "undefined" &&
    window.matchMedia(HOVER_MEDIA_QUERY).matches;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;
    let cleanupDocumentPointerDown: (() => void) | null = null;

    const hideTooltip = () => {
      setTooltip((prev) => (prev.visible ? { ...prev, visible: false } : prev));
    };

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
      zoom: 4,
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
        hideTooltip();
      });

      map.on("mousemove", "geographies-fill", (e: MapLayerMouseEvent) => {
        if (!isHoverMode) return;
        const feature = e.features?.[0];
        const label =
          feature?.properties?.NAMELSAD ??
          feature?.properties?.NAME ??
          "Unknown geography";
        if (typeof label !== "string" || !label.trim()) return;
        setTooltip({
          visible: true,
          label,
          x: e.point.x + 6,
          y: e.point.y + 6,
        });
      });

      map.on("click", "geographies-fill", (e: MapLayerMouseEvent) => {
        const feature = e.features?.[0];
        const label =
          feature?.properties?.NAMELSAD ??
          feature?.properties?.NAME ??
          "Unknown geography";
        const clickedGeoid = feature?.properties?.GEOIDFQ;
        if (!clickedGeoid || typeof clickedGeoid !== "string") return;
        if (!isHoverMode && typeof label === "string" && label.trim()) {
          setTooltip({
            visible: true,
            label,
            x: e.point.x + 6,
            y: e.point.y + 6,
          });
        }
        onSelectGeoid(normalizeGeoid(clickedGeoid));
      });

      if (!isHoverMode) {
        map.on("click", (e) => {
          const clickedFeatures = map.queryRenderedFeatures(e.point, {
            layers: ["geographies-fill"],
          });
          if (clickedFeatures.length === 0) {
            hideTooltip();
          }
        });
        map.on("movestart", hideTooltip);
        map.on("zoomstart", hideTooltip);

        const onDocumentPointerDown = (event: PointerEvent) => {
          const mapContainer = mapContainerRef.current;
          if (!mapContainer) return;
          if (event.target instanceof Node && !mapContainer.contains(event.target)) {
            hideTooltip();
          }
        };
        document.addEventListener("pointerdown", onDocumentPointerDown);
        cleanupDocumentPointerDown = () => {
          document.removeEventListener("pointerdown", onDocumentPointerDown);
        };
      }
      setMapLoaded(true);
    });

    mapRef.current = map;
    return () => {
      cleanupDocumentPointerDown?.();
      map.remove();
      mapRef.current = null;
      setMapLoaded(false);
    };
  }, [isHoverMode, onSelectGeoid]);

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
      {tooltip.visible && (
        <div
          className="map-tooltip"
          style={{ left: `${tooltip.x}px`, top: `${tooltip.y}px` }}
        >
          {tooltip.label}
        </div>
      )}
    </section>
  );
}
