import { type RefObject, type SubmitEventHandler, type PointerEventHandler } from "react";
import {
  type GeographyListResponse,
  type GeographyResponse,
} from "@/types/api.ts";
import { SectionCore } from "@/components/sections/SectionCore.tsx";
import { SectionDiversity } from "@/components/sections/SectionDiversity.tsx";
import { SectionEducation } from "@/components/sections/SectionEducation.tsx";
import { SectionIncome } from "@/components/sections/SectionIncome.tsx";
import { SectionOccupation } from "@/components/sections/SectionOccupation.tsx";

type GeographyPanelProps = {
  geoid: string;
  onSubmit: SubmitEventHandler<HTMLFormElement>;
  onGeoidChange: (next: string) => void;
  geographyListResponse: GeographyListResponse | null;
  geographyResponse: GeographyResponse | null;
  geographyError: string | null;
  isGeographyLoading: boolean;
  panelRef: RefObject<HTMLElement | null>;
  onStartHorizontalResize: PointerEventHandler<HTMLDivElement>;
  onStartVerticalResize: PointerEventHandler<HTMLDivElement>;
};

export function GeographyPanel({
  geoid,
  onSubmit,
  onGeoidChange,
  geographyListResponse,
  geographyResponse,
  geographyError,
  isGeographyLoading,
  panelRef,
  onStartHorizontalResize,
  onStartVerticalResize,
}: GeographyPanelProps) {
  const showErrorState = Boolean(geographyError) && !isGeographyLoading;
  const statusText = isGeographyLoading
    ? "Loading..."
    : geographyError
      ? geographyError
      : null;
  const selectedGeographyType =
    geographyListResponse?.geographies.find(
      (item) => item.sumlevel === geographyResponse?.geography.sumlevel,
    ) ?? null;

  return (
    <aside ref={panelRef} className="detail-panel" aria-label="Details panel">
      <div
        className="panel-resize-handle panel-resize-handle--horizontal"
        onPointerDown={onStartHorizontalResize}
      />
      <div
        className="panel-resize-handle panel-resize-handle--vertical"
        onPointerDown={onStartVerticalResize}
      />
      <div className="detail-row">
        {showErrorState ? (
          <>
            <h2>Could not load geography</h2>
            <p className="panel-error">{geographyError}</p>
          </>
        ) : isGeographyLoading && !geographyResponse ? (
          <>
            <h2>Loading geography...</h2>
            <p>{statusText}</p>
          </>
        ) : !geographyResponse ? (
          <>
            <h2>Welcome to Census Visualizer!</h2>
            <p className={geographyError ? "panel-error" : undefined}>
              {statusText ||
                "Please click a place on the map or type in a GEOID manually to get started."}
            </p>
          </>
        ) : (
          <>
            <h2>{geographyResponse.geography.name}</h2>
            <p>
              {selectedGeographyType ? selectedGeographyType.label : "\u00A0"}
            </p>
            <p className={geographyError ? "panel-error" : undefined}>
              {statusText ?? "\u00A0"}
            </p>
            <SectionCore geographyResponse={geographyResponse} />
            <SectionIncome geographyResponse={geographyResponse} />
            <SectionEducation geographyResponse={geographyResponse} />
            <SectionDiversity geographyResponse={geographyResponse} />
            <SectionOccupation geographyResponse={geographyResponse} />
          </>
        )}
      </div>
      <hr />
      <div className="detail-row">
        <h2>Manual GEOID Input</h2>
        <p>
          Format: <code>(sumlevel)00US(geoid_after_us)</code>
        </p>
        <form onSubmit={onSubmit}>
          <div className="input-row">
            <input
              id="geoid-input"
              type="text"
              className="input-box"
              placeholder="e.g., 16000US3651000"
              value={geoid}
              onChange={(e) => onGeoidChange(e.target.value)}
            />
            <button type="submit" className="input-button">
              Load
            </button>
          </div>
        </form>
      </div>
    </aside>
  );
}
