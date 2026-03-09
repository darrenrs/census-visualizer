import { type GeographyResponse } from "@/types/api.ts";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionOccupation({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Occupation</h3>
      <p>Meausures of occupational diversity.</p>
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
