import { type GeographyResponse } from "@/types/api.ts";
import { ValueContainer } from "@/components/common/ValueContainer.tsx";

type SectionProps = {
  geographyResponse: GeographyResponse | null;
};

export function SectionEducation({ geographyResponse }: SectionProps) {
  return (
    <div className="panel-section">
      <h3>Education</h3>
      <p>
        Educational attainment and average years of schooling for the population
        25 years and over.
      </p>
      <div className="value-section">
        <ValueContainer
          label="Education Index (0-100)"
          numberType="number"
          numberFormatMode="rawInteger"
          mainValue={geographyResponse?.education?.edu_education_index}
          valueLow90={geographyResponse?.education?.edu_education_index_lo90}
          valueHigh90={geographyResponse?.education?.edu_education_index_hi90}
        />
        <ValueContainer
          label="Average Years of Schooling"
          numberType="number"
          numberFormatMode="twoDecimalPlaces"
          mainValue={geographyResponse?.education?.edu_years_of_school}
          valueLow90={geographyResponse?.education?.edu_years_of_school_lo90}
          valueHigh90={geographyResponse?.education?.edu_years_of_school_hi90}
        />
      </div>
    </div>
  );
}
