import { type GeographyResponse } from "@/types/api.ts";
import { InformationCard } from "@/components/common/InformationCard.tsx";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionDiversity({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Diversity</h3>
      <p>
        Racial/ethnic diversity based on six broad groups: All Hispanic
        Americans as well as Non-Hispanic White, Black, Native American, Asian
        and Pacific Islander, and Other Americans. Normalized on a scale of 0 to
        100 and roughly represents how evenly divided are the proportions of
        these six groups.
      </p>
      <InformationCard
        section="diversity"
        flags={geographyResponse?.diversity?.flags}
      />
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
