import { type Sumlevel, type GeographyListResponse } from "@/types/api.ts";

type SumlevelSelectorProps = {
  activeSumlevel: Sumlevel;
  onSelectSumlevel: (next: Sumlevel) => void;
  geographyListResponse: GeographyListResponse | null;
  geographyListError: string | null;
};

export function SumlevelSelector({
  activeSumlevel,
  onSelectSumlevel,
  geographyListResponse,
  geographyListError,
}: SumlevelSelectorProps) {
  if (geographyListError) {
    return (
      <div className="sumlevel-selector">
        <p className="sumlevel-selector-error">{geographyListError}</p>
      </div>
    );
  }

  if (!geographyListResponse) {
    return <div className="sumlevel-selector">Loading ...</div>;
  }

  const selectedItem =
    geographyListResponse.geographies.find(
      (item) => item.sumlevel === activeSumlevel,
    ) || null;

  return (
    <div className="sumlevel-selector">
      <div className="sumlevel-selector-buttons">
        {geographyListResponse.geographies.map((sumlevel) => {
          const isActive = sumlevel.sumlevel === activeSumlevel;

          return (
            <button
              key={sumlevel.sumlevel}
              type="button"
              className={isActive ? "is-active" : ""}
              onClick={() => onSelectSumlevel(sumlevel.sumlevel)}
              value={sumlevel.sumlevel}
            >
              {sumlevel.label}
            </button>
          );
        })}
      </div>
      <p className="sumlevel-selector-meta">
        {selectedItem
          ? `${selectedItem.description ?? "No description"} (${selectedItem.geography_count.toLocaleString()})`
          : "Choose a geography type."}
      </p>
    </div>
  );
}
