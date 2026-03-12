export type NumberFormatMode =
  | "rawInteger"
  | "prefixInteger"
  | "twoDecimalPlaces"
  | "threeDecimalPlaces"
  | "ordinal";

export type NumberType = "number" | "currency";
export type OrdinalSuffix = "st" | "nd" | "rd" | "th";

export function getOrdinalParts(n: number | null | undefined) {
  if (n === null || n === undefined || Number.isNaN(n)) {
    return null;
  }

  const rounded = Math.round(n);
  const absRounded = Math.abs(rounded);
  const lastTwo = absRounded % 100;
  const lastOne = absRounded % 10;

  let suffix: OrdinalSuffix = "th";
  if (lastTwo < 11 || lastTwo > 13) {
    if (lastOne === 1) suffix = "st";
    else if (lastOne === 2) suffix = "nd";
    else if (lastOne === 3) suffix = "rd";
  }

  return {
    value: rounded.toLocaleString(),
    suffix,
  };
}

export function formatNumber(
  n: number | null | undefined,
  mode: NumberFormatMode = "rawInteger",
) {
  if (n === null || n === undefined || Number.isNaN(n)) {
    return "N/A";
  }

  switch (mode) {
    case "rawInteger": {
      return Math.round(n).toLocaleString();
    }

    case "prefixInteger": {
      const abs = Math.abs(n);
      if (abs < 10000) {
        return Math.round(n).toLocaleString();
      }

      const units = ["", "k", "M", "B"];
      let unitIndex = 0;
      let scaled = n;

      while (Math.abs(scaled) >= 1000 && unitIndex < units.length - 1) {
        scaled /= 1000;
        unitIndex += 1;
      }

      const absScaled = Math.abs(scaled);
      const integerDigits =
        absScaled > 0 ? Math.floor(Math.log10(absScaled)) + 1 : 1;
      const decimals = Math.max(0, Math.min(2, 3 - integerDigits));
      const compact = scaled.toFixed(decimals);

      return `${compact}${units[unitIndex]}`;
    }

    case "twoDecimalPlaces": {
      return n.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    }

    case "threeDecimalPlaces": {
      return n.toLocaleString(undefined, {
        minimumFractionDigits: 3,
        maximumFractionDigits: 3,
      });
    }

    case "ordinal": {
      const parts = getOrdinalParts(n);
      if (!parts) {
        return "N/A";
      }
      return `${parts.value}${parts.suffix}`;
    }
  }
}

export function formatCurrency(
  n: number | null | undefined,
  mode: NumberFormatMode = "rawInteger",
) {
  const formatted = formatNumber(n, mode);
  if (formatted === "N/A") {
    return formatted;
  }
  return `$${formatted}`;
}
