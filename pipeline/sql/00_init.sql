CREATE SCHEMA IF NOT EXISTS viz;

-- Integer-ish: dollars, counts, thresholds, means
-- Returns BIGINT (so it prints nicely and won’t show 1e+06)
CREATE OR REPLACE FUNCTION viz.clean_int(x real)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN x IS NULL THEN NULL
    -- ACS sentinel codes + float4-rounded neighbors
    WHEN x::double precision IN (
      -666666666, -666666688,
      -888888888, -888888896,
      -999999999, -1000000000
    ) THEN NULL
    WHEN x < 0 THEN NULL
    ELSE round(x::double precision)::bigint
  END;
$$;

-- Decimal-ish: shares (0..100) and gini (0..1) and other ratios
-- Returns DOUBLE PRECISION (good for math; not float4)
CREATE OR REPLACE FUNCTION viz.clean_dec(x real)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN x IS NULL THEN NULL
    WHEN x::double precision IN (
      -666666666, -666666688,
      -888888888, -888888896,
      -999999999, -1000000000
    ) THEN NULL
    WHEN x < 0 THEN NULL
    ELSE x::double precision
  END;
$$;