import { type GeographyResponse } from "@/types/api.ts";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";
import { IncomePercentileTable } from "@/components/common/IncomePercentileTable.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionIncome({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Household Income</h3>
      <p>
        Estimated household income for extreme upper percentiles using a Pareto
        function.
      </p>
      <div className="value-section">
        <ValueContainer
          label="Median Household Income"
          numberType="currency"
          numberFormatMode="rawInteger"
          mainValue={geographyResponse?.income?.hhi_median}
          valueLow90={geographyResponse?.income?.hhi_median_lo90}
          valueHigh90={geographyResponse?.income?.hhi_median_hi90}
        />
        <ValueContainer
          label="Gini Index of Inequality"
          numberType="number"
          numberFormatMode="threeDecimalPlaces"
          mainValue={geographyResponse?.income?.hhi_gini}
          valueLow90={geographyResponse?.income?.hhi_gini_lo90}
          valueHigh90={geographyResponse?.income?.hhi_gini_hi90}
        />
      </div>
      <h4>Real Percentiles</h4>
      <IncomePercentileTable
        type="original"
        numberType="currency"
        numberFormatMode="prefixInteger"
        rows={[
          [
            20,
            geographyResponse?.income?.hhi_p20,
            geographyResponse?.income?.hhi_p20_lo90,
            geographyResponse?.income?.hhi_p20_hi90,
          ],
          [
            40,
            geographyResponse?.income?.hhi_p40,
            geographyResponse?.income?.hhi_p40_lo90,
            geographyResponse?.income?.hhi_p40_hi90,
          ],
          [
            60,
            geographyResponse?.income?.hhi_p60,
            geographyResponse?.income?.hhi_p60_lo90,
            geographyResponse?.income?.hhi_p60_hi90,
          ],
          [
            80,
            geographyResponse?.income?.hhi_p80,
            geographyResponse?.income?.hhi_p80_lo90,
            geographyResponse?.income?.hhi_p80_hi90,
          ],
          [
            95,
            geographyResponse?.income?.hhi_p95,
            geographyResponse?.income?.hhi_p95_lo90,
            geographyResponse?.income?.hhi_p95_hi90,
          ],
        ]}
      />

      <h4>Simulated Percentiles</h4>
      <IncomePercentileTable
        type="simulated"
        numberType="currency"
        numberFormatMode="prefixInteger"
        rows={[
          [
            90,
            geographyResponse?.income?.hhi_sim_p90,
            geographyResponse?.income?.hhi_sim_p90_lo90,
            geographyResponse?.income?.hhi_sim_p90_hi90,
          ],
          [
            95,
            geographyResponse?.income?.hhi_sim_p95,
            geographyResponse?.income?.hhi_sim_p95_lo90,
            geographyResponse?.income?.hhi_sim_p95_hi90,
          ],
          [
            99,
            geographyResponse?.income?.hhi_sim_p99,
            geographyResponse?.income?.hhi_sim_p99_lo90,
            geographyResponse?.income?.hhi_sim_p99_hi90,
          ],
          [
            99.9,
            geographyResponse?.income?.hhi_sim_p999,
            geographyResponse?.income?.hhi_sim_p999_lo90,
            geographyResponse?.income?.hhi_sim_p999_hi90,
          ],
        ]}
      />
    </div>
  );
}
