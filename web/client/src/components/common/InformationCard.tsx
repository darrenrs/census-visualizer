type FlagRule = {
  bit: number;
  level: "warning" | "error";
  message: string;
};

const flagsLookup = {
  income: [
    {
      bit: 1,
      level: "error",
      message:
        "No data are available because the population or number of households is too low (minimum 250 population or 100 households.)",
    },
    {
      bit: 2,
      level: "warning",
      message:
        "A slightly less accurate formula was used because the number of households that were top coded at over $250,000 was too large to use the standard formula.",
    },
    {
      bit: 4,
      level: "warning",
      message:
        "A significant number of simulations failed due to data uncertainty. Please take caution in interpreting this data.",
    },
    {
      bit: 8,
      level: "error",
      message:
        "The simulations could not be performed because household income percentiles are not available for block groups.",
    },
    {
      bit: 16,
      level: "error",
      message: "The simulations could not be performed due to missing data.",
    },
    {
      bit: 32,
      level: "error",
      message:
        "The simulations could not be performed because all percentile values are bottom coded at $2,499.",
    },
    {
      bit: 64,
      level: "warning",
      message:
        "The simulated P95 was less than the simulated P90 using the standard formula. An alternate formula was applied but the value still may be problematic.",
    },
    {
      bit: 128,
      level: "warning",
      message:
        "The simulated P95 was less than the observed P95 using the standard formula. Please take caution in interpreting this data.",
    },
  ],
  education: [
    {
      bit: 1,
      level: "error",
      message:
        "No data are available because the population or number of households is too low (minimum 250 population, 200 population age 25+, or 100 households.)",
    },
  ],
  diversity: [
    {
      bit: 1,
      level: "error",
      message:
        "No data are available because the population or number of households is too low (minimum 250 population or 100 households.)",
    },
  ],
  occupation: [
    {
      bit: 1,
      level: "error",
      message:
        "No data are available because the population or number of households is too low (minimum 250 population, 200 population age 16+, or 100 households.)",
    },
  ],
} satisfies Record<string, FlagRule[]>;

type InformationSection = keyof typeof flagsLookup;

type InformationCardProps = {
  section: InformationSection;
  flags: number | undefined;
};

export function InformationCard({ section, flags }: InformationCardProps) {
  let matchedRules;

  if (flags === undefined || flags === null) {
    matchedRules = flagsLookup[section].filter((rule) => (1 & rule.bit) !== 0);
  } else {
    if (flags === 0) {
      return null;
    }

    matchedRules = flagsLookup[section].filter(
      (rule) => (flags & rule.bit) !== 0,
    );

    if (matchedRules.length === 0) {
      return null;
    }
  }

  return (
    <div className="information-card-list">
      {matchedRules.map((rule) => (
        <div
          key={`${rule.bit}-${rule.message}`}
          className={`information-card information-card--${rule.level}`}
        >
          <span className="information-card-icon" aria-hidden="true">
            {rule.level === "warning" ? (
              <svg viewBox="0 0 24 24" focusable="false">
                <path d="M12 2L22 20H2L12 2Z" fill="currentColor" />
                <path
                  d="M12 5.4L19.2 17.8H4.8L12 5.4Z"
                  fill="none"
                  stroke="#ffffff"
                  strokeWidth="1.1"
                  strokeLinejoin="round"
                />
                <path
                  d="M12 9.2V13.1"
                  fill="none"
                  stroke="#ffffff"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                />
                <circle cx="12" cy="15.7" r="0.85" fill="#ffffff" />
              </svg>
            ) : (
              <svg viewBox="0 0 24 24" focusable="false">
                <path
                  d="M8 2H16L22 8V16L16 22H8L2 16V8L8 2Z"
                  fill="currentColor"
                />
                <path
                  d="M8.9 4.1H15.1L19.9 8.9V15.1L15.1 19.9H8.9L4.1 15.1V8.9L8.9 4.1Z"
                  fill="none"
                  stroke="#ffffff"
                  strokeWidth="1.4"
                  strokeLinejoin="round"
                />
                <path
                  d="M12 8V13.2"
                  fill="none"
                  stroke="#ffffff"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                />
                <circle cx="12" cy="16.2" r="1.05" fill="#ffffff" />
              </svg>
            )}
          </span>
          <span className="information-card-text">
            {rule.level === "warning"
              ? `Warning: ${rule.message}`
              : `Data unavailable: ${rule.message}`}
          </span>
        </div>
      ))}
    </div>
  );
}
