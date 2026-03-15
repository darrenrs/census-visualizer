import {
  useCallback,
  useRef,
  useState,
  useEffect,
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  type SubmitEventHandler,
} from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { GeographyPanel } from "@/components/GeographyPanel.tsx";
import { MapViewer } from "@/components/MapViewer.tsx";
import {
  type ApiErrorResponse,
  type Sumlevel,
  type GeographyListResponse,
  type GeographyResponse,
} from "@/types/api.ts";
import { SumlevelSelector } from "@/components/SumlevelSelector";

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

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

export default function MainPage() {
  const contentShellRef = useRef<HTMLElement | null>(null);
  const panelRef = useRef<HTMLElement | null>(null);

  // states concerning sumlevel/geography list API
  const [activeSumlevel, setActiveSumlevel] =
    useState<Sumlevel>(DEFAULT_SUMLEVEL);
  const [geographyListResponse, setGeographyListResponse] =
    useState<GeographyListResponse | null>(null);
  const [geographyListError, setGeographyListError] = useState<string | null>(
    null,
  );

  // states concerning geoid/geography API
  const [geoid, setGeoid] = useState("");
  const [isGeographyLoading, setIsGeographyLoading] = useState(false);
  const [geographyResponse, setGeographyResponse] =
    useState<GeographyResponse | null>(null);
  const [geographyError, setGeographyError] = useState<string | null>(null);
  const [panelWidth, setPanelWidth] = useState<number | null>(null);
  const [mapHeight, setMapHeight] = useState<number | null>(null);

  const apiBase = (import.meta.env.VITE_API_BASE || "/api").replace(/\/+$/, "");
  const location = useLocation();
  const navigate = useNavigate();
  const geoidFromHash = decodeURIComponent(location.hash.replace(/^#/, ""));

  const updateHash = useCallback(
    (nextGeoid: string) => {
      const normalizedGeoid = nextGeoid.trim();
      if (!normalizedGeoid) {
        navigate({ hash: "" });
        return;
      }
      navigate({ hash: `#${encodeURIComponent(normalizedGeoid)}` });
    },
    [navigate],
  );

  const startHorizontalResize = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (window.matchMedia("(max-width: 960px)").matches) return;
      const panelEl = panelRef.current;
      const shellEl = contentShellRef.current;
      if (!panelEl || !shellEl) return;

      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      const startX = event.clientX;
      const startWidth = panelEl.getBoundingClientRect().width;
      const shellWidth = shellEl.getBoundingClientRect().width;
      const minWidth = 420;
      const maxWidth = Math.max(minWidth, shellWidth - 420);

      const previousCursor = document.body.style.cursor;
      const previousSelect = document.body.style.userSelect;
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";

      const onMove = (moveEvent: PointerEvent) => {
        const deltaX = moveEvent.clientX - startX;
        const nextWidth = clamp(startWidth - deltaX, minWidth, maxWidth);
        setPanelWidth(nextWidth);
      };

      const onUp = () => {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch {
          // no-op
        }
        document.body.style.cursor = previousCursor;
        document.body.style.userSelect = previousSelect;
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
      };

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    },
    [],
  );

  const startVerticalResize = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (!window.matchMedia("(max-width: 960px)").matches) return;
      const shellEl = contentShellRef.current;
      if (!shellEl) return;

      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      const shellRect = shellEl.getBoundingClientRect();
      const minMapHeight = 180;
      const maxMapHeight = Math.max(minMapHeight + 40, shellRect.height - 120);

      const previousCursor = document.body.style.cursor;
      const previousSelect = document.body.style.userSelect;
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";

      const onMove = (moveEvent: PointerEvent) => {
        const relativeY = moveEvent.clientY - shellRect.top;
        const nextMapHeight = clamp(relativeY, minMapHeight, maxMapHeight);
        setMapHeight(nextMapHeight);
      };

      const onUp = () => {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch {
          // no-op
        }
        document.body.style.cursor = previousCursor;
        document.body.style.userSelect = previousSelect;
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
      };

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    },
    [],
  );

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
    updateHash(geoid);
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
        const data = (await response.json()) as GeographyListResponse;
        setGeographyListResponse(data);
      } catch (error) {
        console.error("Error fetching geographies:", error);
        setGeographyListError("Could not load geography types.");
        setGeographyListResponse(null);
      }
    };

    void fetchGeographyList();
  }, [apiBase]);

  useEffect(() => {
    setGeoid(geoidFromHash);
    if (geoidFromHash) {
      void fetchGeography(geoidFromHash);
      return;
    }
    setGeographyResponse(null);
    setGeographyError(null);
    setIsGeographyLoading(false);
  }, [geoidFromHash, fetchGeography]);

  const contentShellStyle: CSSProperties = {
    ...(panelWidth ? { "--panel-width": `${panelWidth}px` } : {}),
    ...(mapHeight ? { "--map-height": `${mapHeight}px` } : {}),
  } as CSSProperties;

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand">Census Visualizer</div>
        <nav className="top-nav" aria-label="Primary">
          <Link to={"/about"}>About</Link>
          <a
            href="https://github.com/darrenrs/census-visualizer"
            target="_blank"
            rel="noreferrer"
          >
            GitHub
          </a>
        </nav>
      </header>

      <main
        ref={contentShellRef}
        className="content-shell"
        style={contentShellStyle}
      >
        <MapViewer
          onSelectGeoid={updateHash}
          sumlevel={activeSumlevel}
          selectedGeoid={geoidFromHash}
        />
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
          geographyListResponse={geographyListResponse}
          geographyResponse={geographyResponse}
          geographyError={geographyError}
          isGeographyLoading={isGeographyLoading}
          panelRef={panelRef}
          onStartHorizontalResize={startHorizontalResize}
          onStartVerticalResize={startVerticalResize}
        />
      </main>
    </div>
  );
}
