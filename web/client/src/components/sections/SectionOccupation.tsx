import { type GeographyResponse } from "@/types/api.ts";
import { InformationCard } from "@/components/common/InformationCard.tsx";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionOccupation({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Occupation</h3>
      <p>
        Meausures of occupational diversity. Simple Occupation Index represents
        the effective number of high level occupational groups (professional,
        service, sales, skilled trades, blue collar.) Extended Occupation Index
        represents the effective number of specific occupation groups (out of a
        total of 25.) The Extended/Simple Occupation Ratio is a measure of the
        diversity of jobs within occupational groups.
      </p>
      <InformationCard
        section="occupation"
        flags={geographyResponse?.occupation?.flags}
      />
      <div className="value-section">
        <ValueContainer
          label="Simple Occupation Index"
          numberType="number"
          numberFormatMode="twoDecimalPlaces"
          mainValue={geographyResponse?.occupation?.occ_occupation_index}
          valueLow90={geographyResponse?.occupation?.occ_occupation_index_lo90}
          valueHigh90={geographyResponse?.occupation?.occ_occupation_index_hi90}
        />
        <ValueContainer
          label="Extended Occupation Index"
          numberType="number"
          numberFormatMode="twoDecimalPlaces"
          mainValue={geographyResponse?.occupation?.occ_occupation_index_ext}
          valueLow90={
            geographyResponse?.occupation?.occ_occupation_index_ext_lo90
          }
          valueHigh90={
            geographyResponse?.occupation?.occ_occupation_index_ext_hi90
          }
        />
        <ValueContainer
          label="Ext/Sim Occupation Ratio"
          numberType="number"
          numberFormatMode="twoDecimalPlaces"
          mainValue={geographyResponse?.occupation?.occ_occupation_index_ratio}
          valueLow90={
            geographyResponse?.occupation?.occ_occupation_index_ratio_lo90
          }
          valueHigh90={
            geographyResponse?.occupation?.occ_occupation_index_ratio_hi90
          }
        />
      </div>
    </div>
  );
}
