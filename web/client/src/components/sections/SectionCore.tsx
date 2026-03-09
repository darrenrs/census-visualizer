import { type GeographyResponse } from "@/types/api.ts";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionCore({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Demographics</h3>
      <p>Core population and household totals.</p>
      <div className="value-section">
        <ValueContainer
          label="Population"
          numberType="number"
          numberFormatMode="rawInteger"
          mainValue={geographyResponse?.core.total_population}
          valueLow90={geographyResponse?.core.total_population_lo90}
          valueHigh90={geographyResponse?.core.total_population_hi90}
        />
        <ValueContainer
          label="Households"
          numberType="number"
          numberFormatMode="rawInteger"
          mainValue={geographyResponse?.core.total_households}
          valueLow90={geographyResponse?.core.total_households_lo90}
          valueHigh90={geographyResponse?.core.total_households_hi90}
        />
        <ValueContainer
          label="Average Household Size"
          numberType="number"
          numberFormatMode="twoDecimalPlaces"
          mainValue={geographyResponse?.core.avg_household_size}
          valueLow90={geographyResponse?.core.avg_household_size_lo90}
          valueHigh90={geographyResponse?.core.avg_household_size_hi90}
        />
      </div>
    </div>
  );
}
