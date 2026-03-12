import { type GeographyResponse } from "@/types/api.ts";
import { InformationCard } from "@/components/common/InformationCard.tsx";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";
import { IncomePercentileTable } from "@/components/common/IncomePercentileTable.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionIncome({ geographyResponse }: SectionProps) {
  const incomeFlags = geographyResponse?.income?.flags ?? 0;
  const hideExtrapolatedPercentiles = (incomeFlags & (1 | 8 | 16 | 32)) !== 0;
  const hideObservedPercentiles = (incomeFlags & (1 | 8)) !== 0;
  const hideAll = (incomeFlags & 1) !== 0;

  return (
    <div className="panel-section">
      <h3>Household Income</h3>
      <p>
        Median and selected percentile ranks (observed and extrapolated via
        Pareto function) for household income and Gini coefficient of income
        inequality.
      </p>
      <InformationCard
        section="income"
        flags={geographyResponse?.income?.flags}
      />
      {!hideAll && (
        <>
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
              label="Gini Coefficient"
              numberType="number"
              numberFormatMode="threeDecimalPlaces"
              mainValue={geographyResponse?.income?.hhi_gini}
              valueLow90={geographyResponse?.income?.hhi_gini_lo90}
              valueHigh90={geographyResponse?.income?.hhi_gini_hi90}
            />
          </div>
          {!hideObservedPercentiles && (
            <>
              <h4>Observed Percentiles</h4>
              <IncomePercentileTable
                type="original"
                numberType="currency"
                numberFormatMode="prefixInteger"
                households={geographyResponse?.core.total_households}
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
            </>
          )}
          {!hideExtrapolatedPercentiles && (
            <>
              <h4>Simulated Percentiles</h4>
              <IncomePercentileTable
                type="simulated"
                numberType="currency"
                numberFormatMode="prefixInteger"
                households={geographyResponse?.core.total_households}
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
            </>
          )}
        </>
      )}
    </div>
  );
}
