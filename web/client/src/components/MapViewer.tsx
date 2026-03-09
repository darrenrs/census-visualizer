import { useEffect, useRef } from "react";
import maplibregl, { type MapLayerMouseEvent } from "maplibre-gl";

type MapViewerProps = {
  onSelectGeoid: (geoid: string) => void;
};

export function MapViewer({ onSelectGeoid }: MapViewerProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const countiesDataUrl = `${import.meta.env.BASE_URL}geo/counties_sample.geojson`;

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
      map.addSource("counties", {
        type: "geojson",
        data: countiesDataUrl,
      });

      map.addLayer({
        id: "counties-fill",
        type: "fill",
        source: "counties",
        paint: {
          "fill-color": "#2b715b",
          "fill-opacity": 0.12,
        },
      });

      map.addLayer({
        id: "counties-line",
        type: "line",
        source: "counties",
        paint: {
          "line-color": "#1f5744",
          "line-width": 0.6,
          "line-opacity": 0.45,
        },
      });

      map.on("mouseenter", "counties-fill", () => {
        map.getCanvas().style.cursor = "pointer";
      });

      map.on("mouseleave", "counties-fill", () => {
        map.getCanvas().style.cursor = "";
      });

      map.on("click", "counties-fill", (e: MapLayerMouseEvent) => {
        const feature = e.features?.[0];
        const clickedGeoid = feature?.properties?.GEOID;
        if (!clickedGeoid || typeof clickedGeoid !== "string") return;
        onSelectGeoid("05000US" + clickedGeoid);
      });
    });

    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [countiesDataUrl, onSelectGeoid]);

  return (
    <section className="map-container" aria-label="Map">
      <div ref={mapContainerRef} className="map-canvas" />
    </section>
  );
}
