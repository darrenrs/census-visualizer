import {
  useCallback,
  useState,
  useEffect,
  type SubmitEventHandler,
} from "react";
import { GeographyPanel } from "@/components/GeographyPanel.tsx";
import { MapViewer } from "@/components/MapViewer.tsx";
import {
  type ApiErrorResponse,
  type Sumlevel,
  type GeographiesResponse,
  type GeographyResponse,
} from "@/types/api.ts";
import { SumlevelSelector } from "./components/SumlevelSelector";

const DEFAULT_SUMLEVEL = 40; // US State is the default sumlevel

async function readApiError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as ApiErrorResponse;
    if (body && typeof body.error === "string" && body.error.trim()) {
      return body.error;
    }
  } catch {
    // no-op: fallback message below
  }
  return `Request failed with status ${response.status}`;
}

export default function App() {
  const [geoid, setGeoid] = useState("");
  const [geographyResponse, setGeographyResponse] =
    useState<GeographyResponse | null>(null);
  const [geographyError, setGeographyError] = useState<string | null>(null);
  const [isGeographyLoading, setIsGeographyLoading] = useState(false);
  const [activeSumlevel, setActiveSumlevel] =
    useState<Sumlevel>(DEFAULT_SUMLEVEL);
  const [geographyListResponse, setGeographyListResponse] =
    useState<GeographiesResponse | null>(null);
  const [geographyListError, setGeographyListError] = useState<string | null>(
    null,
  );
  const apiBase = (import.meta.env.VITE_API_BASE || "/api").replace(/\/+$/, "");
  const appBase = import.meta.env.BASE_URL;

  const fetchGeography = useCallback(
    async (targetGeoid: string) => {
      const normalizedGeoid = targetGeoid.trim();
      if (!normalizedGeoid) return;

      setGeoid(normalizedGeoid);
      setIsGeographyLoading(true);
      setGeographyError(null);

      try {
        const response = await fetch(
          `${apiBase}/v1/geography/${normalizedGeoid}`,
        );

        if (!response.ok) {
          if (response.status === 404) {
            setGeographyResponse(null);
            setGeographyError(
              `No geography found for GEOID "${normalizedGeoid}".`,
            );
            return;
          }
          const errorMessage = await readApiError(response);
          setGeographyResponse(null);
          setGeographyError(errorMessage);
          return;
        }

        const data = (await response.json()) as GeographyResponse;
        setGeographyResponse(data);
      } catch (error) {
        console.error("Error fetching geography:", error);
        setGeographyResponse(null);
        setGeographyError("Could not reach the API. Please try again.");
      } finally {
        setIsGeographyLoading(false);
      }
    },
    [apiBase],
  );

  const loadGeography: SubmitEventHandler<HTMLFormElement> = (event) => {
    event.preventDefault();
    fetchGeography(geoid);
  };

  useEffect(() => {
    const fetchGeographyList = async () => {
      setGeographyListError(null);
      try {
        const response = await fetch(`${apiBase}/v1/geographies`);
        if (!response.ok) {
          const errorMessage = await readApiError(response);
          setGeographyListError(errorMessage);
          setGeographyListResponse(null);
          return;
        }
        const data = (await response.json()) as GeographiesResponse;
        setGeographyListResponse(data);
      } catch (error) {
        console.error("Error fetching geographies:", error);
        setGeographyListError("Could not load geography types.");
        setGeographyListResponse(null);
      }
    };

    void fetchGeographyList();
  }, [apiBase]);

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand">Census Visualizer</div>
        <nav className="top-nav" aria-label="Primary">
          {/* <a href={`${appBase}faq`}>FAQ</a> */}
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
        <MapViewer onSelectGeoid={fetchGeography} sumlevel={activeSumlevel} />
        <SumlevelSelector
          activeSumlevel={activeSumlevel}
          onSelectSumlevel={setActiveSumlevel}
          geographyListResponse={geographyListResponse}
          geographyListError={geographyListError}
        />
        <GeographyPanel
          geoid={geoid}
          onSubmit={loadGeography}
          onGeoidChange={setGeoid}
          geographyResponse={geographyResponse}
          geographyError={geographyError}
          isGeographyLoading={isGeographyLoading}
        />
      </main>
    </div>
  );
}
