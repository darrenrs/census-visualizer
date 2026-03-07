import { useEffect, useRef, useState, type SubmitEventHandler } from "react";
import maplibregl, { type MapLayerMouseEvent } from "maplibre-gl";

type GeographyResponse = {
  geography?: {
    geoid?: string;
    name?: string;
    vintage?: string;
    state_code?: string;
    sumlevel?: number;
  };
  core?: Record<string, number | null>;
  income?: Record<string, number | null> | null;
  education?: Record<string, number | null> | null;
  diversity?: Record<string, number | null> | null;
  occupation?: Record<string, number | null> | null;
};

export default function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  const [geoid, setGeoid] = useState("");
  const [geographyResponse, setGeographyResponse] =
    useState<GeographyResponse | null>(null);
  const countiesDataUrl = `${import.meta.env.BASE_URL}geo/counties_sample.geojson`;

  const fetchGeography = (targetGeoid: string) => {
    if (!targetGeoid) return;
    setGeoid(targetGeoid);
    fetch(`/census-visualizer/api/v1/geography/${targetGeoid}`)
      .then((response) => response.json())
      .then((data) => setGeographyResponse(data))
      .catch((error) => console.error("Error fetching data:", error));
  };

  const loadGeography: SubmitEventHandler<HTMLFormElement> = (event) => {
    event.preventDefault();
    fetchGeography(geoid);
  };

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
      center: [-98.5795, 39.8283], // US center
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

      map.on("click", "counties-fill", async (e: MapLayerMouseEvent) => {
        const feature = e.features?.[0];
        const clickedGeoid = feature?.properties?.GEOID;
        if (!clickedGeoid || typeof clickedGeoid !== "string") return;

        const apiGeoid = "05000US" + clickedGeoid;
        try {
          fetchGeography(apiGeoid);
        } catch (err) {
          console.error(err);
        }
      });
    });

    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [countiesDataUrl]);

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand">Census Visualizer</div>
        <nav className="top-nav" aria-label="Primary">
          <a href="/census-visualizer/faq">FAQ</a>
          <a
            href="https://github.com/darrenrs/census-visualizer"
            target="_blank"
            rel="noreferrer"
          >
            GitHub
          </a>
        </nav>
      </header>

      <main className="content-shell">
        <section className="map-container" aria-label="Map">
          <div ref={mapContainerRef} className="map-canvas" />
        </section>

        <aside className="detail-panel" aria-label="Details panel">
          <h2>Manual GEOID Input (DEV ONLY)</h2>
          <form onSubmit={loadGeography}>
            <div className="input-row">
              <input
                id="geoid-input"
                type="text"
                className="input-box"
                placeholder="e.g., 16000US3651000"
                value={geoid}
                onChange={(e) => setGeoid(e.target.value)}
              />
              <button type="submit" className="input-button">
                Load
              </button>
            </div>
          </form>
          <br />
          <h2>
            {(geographyResponse && geographyResponse?.geography?.name) ||
              "Enter a GEOID to view details"}
          </h2>
          <div className="panel-section">
            <h3>Demographics</h3>
            <p>
              Population:{" "}
              {geographyResponse &&
                geographyResponse?.core?.total_population?.toLocaleString()}
            </p>
            <p>
              Households:{" "}
              {geographyResponse &&
                geographyResponse?.core?.total_households?.toLocaleString()}
            </p>
          </div>
          <div className="panel-section">
            <h3>Household Income (Simulated)</h3>
            <p>
              90th Percentile: {" $"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p90?.toLocaleString()}
              {" ($"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p90_lo90?.toLocaleString()}
              {"-$"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p90_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              95th Percentile: {" $"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p95?.toLocaleString()}
              {" ($"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p95_lo90?.toLocaleString()}
              {"-$"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p95_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              99th Percentile: {" $"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p99?.toLocaleString()}
              {" ($"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p99_lo90?.toLocaleString()}
              {"-$"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p99_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              99.9th Percentile: {" $"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p999?.toLocaleString()}
              {" ($"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p999_lo90?.toLocaleString()}
              {"-$"}
              {geographyResponse &&
                geographyResponse?.income?.hhi_sim_p999_hi90?.toLocaleString()}
              {")"}
            </p>
          </div>
          <div className="panel-section">
            <h3>Education</h3>
            <p>
              Education Index (0-100):{" "}
              {geographyResponse &&
                geographyResponse?.education?.edu_education_index}
              {" ("}
              {geographyResponse &&
                geographyResponse?.education?.edu_education_index_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.education?.edu_education_index_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              Average Years of Schooling (0-22):{" "}
              {geographyResponse &&
                geographyResponse?.education?.edu_years_of_school}
              {" ("}
              {geographyResponse &&
                geographyResponse?.education?.edu_years_of_school_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.education?.edu_years_of_school_hi90?.toLocaleString()}
              {")"}
            </p>
          </div>
          <div className="panel-section">
            <h3>Diversity</h3>
            <p>
              Diversity Index (0-100):{" "}
              {geographyResponse &&
                geographyResponse?.diversity?.race_diversity_index}
              {" ("}
              {geographyResponse &&
                geographyResponse?.diversity?.race_diversity_index_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.diversity?.race_diversity_index_hi90?.toLocaleString()}
              {")"}
            </p>
          </div>
          <div className="panel-section">
            <h3>Occupation</h3>
            <p>
              Basic Occupation Index:{" "}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index}
              {" ("}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              Extended Occupation Index:{" "}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ext}
              {" ("}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ext_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ext_hi90?.toLocaleString()}
              {")"}
            </p>
            <p>
              Intra Domain Occupation Ratio:{" "}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ratio}
              {" ("}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ratio_lo90?.toLocaleString()}
              {"-"}
              {geographyResponse &&
                geographyResponse?.occupation?.occ_occupation_index_ratio_hi90?.toLocaleString()}
              {")"}
            </p>
          </div>
        </aside>
      </main>
    </div>
  );
}
