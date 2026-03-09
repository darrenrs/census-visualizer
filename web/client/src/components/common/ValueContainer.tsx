import {
  formatCurrency,
  formatNumber,
  type NumberFormatMode,
  type NumberType,
} from "@/lib/format.ts";

type ValueContainerProps = {
  label: string;
  numberType: NumberType;
  numberFormatMode: NumberFormatMode;
  mainValue: number | null | undefined;
  valueLow90?: number | null;
  valueHigh90?: number | null;
};

export function ValueContainer({
  label,
  numberType,
  numberFormatMode,
  mainValue,
  valueLow90,
  valueHigh90,
}: ValueContainerProps) {
  if (mainValue === null || mainValue === undefined) {
    return null;
  }

  const ciIsKnown =
    valueLow90 !== null &&
    valueLow90 !== undefined &&
    valueHigh90 !== null &&
    valueHigh90 !== undefined;
  const formatValue = numberType === "currency" ? formatCurrency : formatNumber;

  return (
    <div className="value-container">
      <div className="value-label">{label}</div>
      <div className="main-value">
        {numberType === "currency" && mainValue === 250001
          ? ">$250,000"
          : formatValue(mainValue, numberFormatMode)}
      </div>
      <div className="margin-of-error">
        {ciIsKnown
          ? `(90% CI ${formatValue(valueLow90, numberFormatMode)}-${formatValue(valueHigh90, numberFormatMode)})`
          : "(90% CI unknown)"}
      </div>
    </div>
  );
}
