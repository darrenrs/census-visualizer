import { type GeographyResponse } from "@/types/api.ts";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionDiversity({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Diversity</h3>
      <p>
        The racial/ethnic diversity based on the major groups White, Black,
        Hispanic, Native American, Asian and Pacific Islander, and Other.
      </p>
      <div className="value-section">
        <ValueContainer
          label="Diversity Index (0-100)"
          numberType="number"
          numberFormatMode="rawInteger"
          mainValue={geographyResponse?.diversity?.race_diversity_index}
          valueLow90={geographyResponse?.diversity?.race_diversity_index_lo90}
          valueHigh90={geographyResponse?.diversity?.race_diversity_index_hi90}
        />
      </div>
    </div>
  );
}
