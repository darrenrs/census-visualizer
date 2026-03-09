import { type SubmitEventHandler } from "react";
import { type GeographyResponse } from "@/types/api.ts";
import { SectionCore } from "@/components/sections/SectionCore.tsx";
import { SectionDiversity } from "@/components/sections/SectionDiversity.tsx";
import { SectionEducation } from "@/components/sections/SectionEducation.tsx";
import { SectionIncome } from "@/components/sections/SectionIncome.tsx";
import { SectionOccupation } from "@/components/sections/SectionOccupation.tsx";

type GeographyPanelProps = {
  geoid: string;
  onSubmit: SubmitEventHandler<HTMLFormElement>;
  onGeoidChange: (next: string) => void;
  geographyResponse: GeographyResponse | null;
};

export function GeographyPanel({
  geoid,
  onSubmit,
  onGeoidChange,
  geographyResponse,
}: GeographyPanelProps) {
  return (
    <aside className="detail-panel" aria-label="Details panel">
      <div className="detail-row">
        <h2>
          {(geographyResponse && geographyResponse.geography.name) ||
            "Enter a GEOID to view details"}
        </h2>
        <SectionCore geographyResponse={geographyResponse} />
        <SectionIncome geographyResponse={geographyResponse} />
        <SectionEducation geographyResponse={geographyResponse} />
        <SectionDiversity geographyResponse={geographyResponse} />
        <SectionOccupation geographyResponse={geographyResponse} />
      </div>
      <div className="detail-row">
        <h2>Manual GEOID Input (DEV ONLY)</h2>
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
