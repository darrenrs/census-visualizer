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
        {geographyListResponse.geographies.map((geographyItem) => {
          const isActive = geographyItem.sumlevel === activeSumlevel;

          return (
            <button
              key={geographyItem.sumlevel}
              type="button"
              className={isActive ? "is-active" : ""}
              onClick={() => onSelectSumlevel(geographyItem.sumlevel)}
              value={geographyItem.sumlevel}
            >
              {geographyItem.label}{" "}
              {isActive
                ? `(${geographyItem.geography_count.toLocaleString()})`
                : ""}
            </button>
          );
        })}
      </div>
      <p className="sumlevel-selector-meta">
        {selectedItem
          ? `${selectedItem.description}`
          : "Choose a geography type."}
      </p>
    </div>
  );
}
