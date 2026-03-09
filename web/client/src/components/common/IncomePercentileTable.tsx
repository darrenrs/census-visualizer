import {
  formatCurrency,
  formatNumber,
  type NumberFormatMode,
  type NumberType,
} from "@/lib/format.ts";

type IncomePercentileMatrixRow = [
  pr: number,
  value: number | null | undefined,
  lo90: number | null | undefined,
  hi90: number | null | undefined,
];

type IncomePercentileTableProps = {
  type?: "original" | "simulated";
  numberType: NumberType;
  numberFormatMode: NumberFormatMode;
  rows: IncomePercentileMatrixRow[];
};

export function IncomePercentileTable({
  type,
  numberType,
  numberFormatMode,
  rows,
}: IncomePercentileTableProps) {
  const formatValue = numberType === "currency" ? formatCurrency : formatNumber;

  return (
    <table className="income-percentile-table">
      <thead>
        <tr>
          <th>PR</th>
          <th>Value</th>
          <th>90% CI</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([pr, value, lo90, hi90]) => {
          const hasKnownCi =
            lo90 !== null &&
            lo90 !== undefined &&
            hi90 !== null &&
            hi90 !== undefined;

          return (
            <tr key={pr}>
              <td className="pr-cell">{pr}</td>
              <td className="value-cell">
                {type === "original" && value === 250001
                  ? ">$250k"
                  : formatValue(value, numberFormatMode)}
              </td>
              <td className="value-cell">
                {hasKnownCi
                  ? `${formatValue(lo90, numberFormatMode)}-${formatValue(hi90, numberFormatMode)}`
                  : "unknown"}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
