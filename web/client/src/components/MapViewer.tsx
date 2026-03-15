import { useEffect, useRef, useState } from "react";
import maplibregl, { type MapLayerMouseEvent } from "maplibre-gl";
import { Protocol } from "pmtiles";
import { type Sumlevel } from "@/types/api.ts";

type MapViewerProps = {
  sumlevel: Sumlevel;
  onSelectGeoid: (geoid: string) => void;
  selectedGeoid: string | null;
};

const HOVER_MEDIA_QUERY = "(hover: hover) and (pointer: fine)";
const SOURCE_ID = "geographies";
const FILL_ACTIVE_ID = "geographies-fill-active";
const FILL_ID = "geographies-fill";
const LINE_ID = "geographies-line";
const PMTILES_PROTOCOL = new Protocol();
let pmtilesProtocolRegistered = false;

function normalizeSumlevel(sumlevel: Sumlevel) {
  return String(sumlevel).padStart(3, "0");
}

// Must match values in pipeline/geo/build_tiles.sh
function zoomsForSumlevel(sumlevel: Sumlevel) {
  switch (sumlevel) {
    case 10:
      return { minzoom: 0, maxzoom: 8 };
    case 40:
      return { minzoom: 0, maxzoom: 9 };
    case 50:
      return { minzoom: 2, maxzoom: 11 };
    case 60:
      return { minzoom: 4, maxzoom: 12 };
    case 140:
      return { minzoom: 5, maxzoom: 13 };
    case 150:
      return { minzoom: 6, maxzoom: 13 };
    case 160:
      return { minzoom: 4, maxzoom: 12 };
    case 310:
      return { minzoom: 2, maxzoom: 10 };
    case 500:
      return { minzoom: 2, maxzoom: 11 };
    case 860:
      return { minzoom: 5, maxzoom: 13 };
    default:
      return { minzoom: 0, maxzoom: 9 };
  }
}

export function MapViewer({
  onSelectGeoid,
  sumlevel,
  selectedGeoid,
}: MapViewerProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [tooltip, setTooltip] = useState({
    visible: false,
    label: "",
    x: 0,
    y: 0,
  });

  const geoBaseRaw = (import.meta.env.VITE_GEO_BASE || "/geo").replace(
    /\/+$/,
    "",
  );
  const geoBase =
    geoBaseRaw.startsWith("http://") || geoBaseRaw.startsWith("https://")
      ? geoBaseRaw
      : typeof window !== "undefined"
        ? `${window.location.origin}${geoBaseRaw}`
        : geoBaseRaw;
  const normalizedSumlevel = normalizeSumlevel(sumlevel);
  const geographiesPmtilesUrl = `${geoBase}/pmtiles/${normalizedSumlevel}.pmtiles`;
  const sourceLayer = `geo_${normalizedSumlevel}`;
  const zoomRange = zoomsForSumlevel(sumlevel);
  const isHoverMode =
    typeof window !== "undefined" &&
    window.matchMedia(HOVER_MEDIA_QUERY).matches;

  // create the map, nothing else
  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    if (!pmtilesProtocolRegistered) {
      try {
        maplibregl.addProtocol("pmtiles", PMTILES_PROTOCOL.tile);
      } catch {
        // protocol may already be registered during hot reload
      }
      pmtilesProtocolRegistered = true;
    }

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
      maxZoom: 14,
    });

    map.on("error", (e) => {
      console.error("MapLibre error:", e.error);
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");
    map.on("load", () => {
      setMapLoaded(true);
    });

    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
      setMapLoaded(false);
    };
  }, []);

  // any updates to the map after init
  useEffect(() => {
    if (!mapLoaded) return;

    const map = mapRef.current;
    if (!map) return;

    const hideTooltip = () => {
      setTooltip((prev) => (prev.visible ? { ...prev, visible: false } : prev));
    };

    hideTooltip();

    if (map.getLayer(FILL_ACTIVE_ID)) map.removeLayer(FILL_ACTIVE_ID);
    if (map.getLayer(FILL_ID)) map.removeLayer(FILL_ID);
    if (map.getLayer(LINE_ID)) map.removeLayer(LINE_ID);
    if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);

    map.addSource(SOURCE_ID, {
      type: "vector",
      url: `pmtiles://${geographiesPmtilesUrl}`,
      minzoom: zoomRange.minzoom,
      maxzoom: zoomRange.maxzoom,
    });

    map.addLayer({
      id: FILL_ID,
      type: "fill",
      source: SOURCE_ID,
      "source-layer": sourceLayer,
      paint: {
        "fill-color": "#2b715b",
        "fill-opacity": 0.15,
      },
    });

    map.addLayer({
      id: FILL_ACTIVE_ID,
      type: "fill",
      source: SOURCE_ID,
      "source-layer": sourceLayer,
      paint: {
        "fill-color": "#ffff2fff",
        "fill-opacity": 0.3,
      },
      filter: ["==", ["get", "GEOID"], ""],
    });

    map.addLayer({
      id: LINE_ID,
      type: "line",
      source: SOURCE_ID,
      "source-layer": sourceLayer,
      paint: {
        "line-color": "#4b5563",
        "line-width": 1.5,
        "line-opacity": 0.3,
      },
    });

    const onMouseEnter = () => {
      map.getCanvas().style.cursor = "pointer";
    };

    const onMouseLeave = () => {
      map.getCanvas().style.cursor = "";
      hideTooltip();
    };

    const onMouseMove = (e: MapLayerMouseEvent) => {
      if (!isHoverMode) return;
      const feature = e.features?.[0];
      const label = feature?.properties?.NAME ?? "Unknown geography";
      if (typeof label !== "string" || !label.trim()) return;
      setTooltip({
        visible: true,
        label,
        x: e.point.x + 6,
        y: e.point.y + 6,
      });
    };

    const onLayerClick = (e: MapLayerMouseEvent) => {
      const feature = e.features?.[0];
      const label = feature?.properties?.NAME ?? "Unknown geography";
      const clickedGeoid = feature?.properties?.GEOID;
      if (!clickedGeoid || typeof clickedGeoid !== "string") return;

      if (!isHoverMode && typeof label === "string" && label.trim()) {
        setTooltip({
          visible: true,
          label,
          x: e.point.x + 6,
          y: e.point.y + 6,
        });
      }

      onSelectGeoid(clickedGeoid);
    };

    map.on("mouseenter", FILL_ID, onMouseEnter);
    map.on("mouseleave", FILL_ID, onMouseLeave);
    map.on("mousemove", FILL_ID, onMouseMove);
    map.on("click", FILL_ID, onLayerClick);

    let cleanupDocumentPointerDown: (() => void) | null = null;
    const onMapClick = (e: maplibregl.MapMouseEvent) => {
      const clickedFeatures = map.queryRenderedFeatures(e.point, {
        layers: [FILL_ID],
      });
      if (clickedFeatures.length === 0) {
        hideTooltip();
      }
    };

    if (!isHoverMode) {
      map.on("click", onMapClick);
      map.on("movestart", hideTooltip);
      map.on("zoomstart", hideTooltip);

      const onDocumentPointerDown = (event: PointerEvent) => {
        const mapContainer = mapContainerRef.current;
        if (!mapContainer) return;
        if (
          event.target instanceof Node &&
          !mapContainer.contains(event.target)
        ) {
          hideTooltip();
        }
      };
      document.addEventListener("pointerdown", onDocumentPointerDown);
      cleanupDocumentPointerDown = () => {
        document.removeEventListener("pointerdown", onDocumentPointerDown);
      };
    }

    return () => {
      map.off("mouseenter", FILL_ID, onMouseEnter);
      map.off("mouseleave", FILL_ID, onMouseLeave);
      map.off("mousemove", FILL_ID, onMouseMove);
      map.off("click", FILL_ID, onLayerClick);

      if (!isHoverMode) {
        map.off("click", onMapClick);
        map.off("movestart", hideTooltip);
        map.off("zoomstart", hideTooltip);
      }

      cleanupDocumentPointerDown?.();
    };
  }, [
    geographiesPmtilesUrl,
    isHoverMode,
    mapLoaded,
    onSelectGeoid,
    sourceLayer,
    zoomRange.maxzoom,
    zoomRange.minzoom,
  ]);

  // highlight active tile sync
  useEffect(() => {
    if (!mapLoaded) return;

    const map = mapRef.current;
    if (!map) return;
    if (!map.getLayer(FILL_ACTIVE_ID)) return;

    map.setFilter(FILL_ACTIVE_ID, [
      "==",
      ["get", "GEOID"],
      selectedGeoid ?? "",
    ]);
  }, [mapLoaded, sourceLayer, selectedGeoid]);

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
