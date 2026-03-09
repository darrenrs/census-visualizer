import { useCallback, useState, type SubmitEventHandler } from "react";
import { GeographyPanel } from "@/components/GeographyPanel.tsx";
import { MapViewer } from "@/components/MapViewer.tsx";
import { type GeographyResponse } from "@/types/api.ts";

export default function App() {
  const [geoid, setGeoid] = useState("");
  const [geographyResponse, setGeographyResponse] =
    useState<GeographyResponse | null>(null);
  const apiBase = (import.meta.env.VITE_API_BASE || "/api").replace(/\/+$/, "");
  const appBase = import.meta.env.BASE_URL;

  const fetchGeography = useCallback(
    (targetGeoid: string) => {
      if (!targetGeoid) return;
      setGeoid(targetGeoid);
      fetch(`${apiBase}/v1/geography/${targetGeoid}`)
        .then((response) => response.json())
        .then((data) => setGeographyResponse(data))
        .catch((error) => console.error("Error fetching data:", error));
    },
    [apiBase],
  );

  const loadGeography: SubmitEventHandler<HTMLFormElement> = (event) => {
    event.preventDefault();
    fetchGeography(geoid);
  };

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand">Census Visualizer</div>
        <nav className="top-nav" aria-label="Primary">
          <a href={`${appBase}faq`}>FAQ</a>
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
        <MapViewer onSelectGeoid={fetchGeography} />
        <GeographyPanel
          geoid={geoid}
          onSubmit={loadGeography}
          onGeoidChange={setGeoid}
          geographyResponse={geographyResponse}
        />
      </main>
    </div>
  );
}
