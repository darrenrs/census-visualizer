import dotenv from "dotenv";
import express from "express";
import path from "path";
import pg from "pg";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 1) Try standard cwd .env (works when running from web/)
dotenv.config();
// 2) Fallback to repo-root .env
if (!process.env.DATABASE_URL) {
  dotenv.config({ path: path.resolve(__dirname, "../../.env") });
}

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL is not set. Add it to web/.env or repo-root .env before starting the server.",
  );
}

const { Pool } = pg;

const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const PORT = Number(process.env.PORT_SERVER || 3000);
const DEFAULT_VINTAGE = process.env.DEFAULT_VINTAGE || "acs2024_5yr";

const QUERY_GEOGRAPHIES = `
WITH all_sumlevels AS (
  SELECT
    g.sumlevel,
    g.label,
    g.description
  FROM api.geographies_v1 g
),
counts AS (
  SELECT
    gb.sumlevel,
    COUNT(*)::integer AS geography_count
  FROM api.geoid_v1 gb
  WHERE gb.vintage = $1
  GROUP BY gb.sumlevel
)
SELECT jsonb_build_object(
  'vintage', $1,
  'geographies',
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'sumlevel', s.sumlevel,
        'label', s.label,
        'description', s.description,
        'geography_count', COALESCE(c.geography_count, 0)
      )
      ORDER BY s.sumlevel
    ),
    '[]'::jsonb
  )
) AS payload
FROM all_sumlevels s
LEFT JOIN counts c
  ON c.sumlevel = s.sumlevel;
`;

const QUERY_GEOGRAPHY = `
WITH target AS (
  SELECT
    g.vintage,
    g.sumlevel,
    g.geoid,
    g.name,
    g.state_code,
    g.total_population,
    g.total_population_lo90,
    g.total_population_hi90,
    g.total_households,
    g.total_households_lo90,
    g.total_households_hi90,
    g.avg_household_size,
    g.avg_household_size_lo90,
    g.avg_household_size_hi90
  FROM api.geoid_v1 g
  WHERE g.geoid = $1
    AND g.vintage = $2
    AND ($3::integer IS NULL OR g.sumlevel = $3)
  LIMIT 1
)
SELECT jsonb_build_object(
  'geography', jsonb_build_object(
    'geoid', t.geoid,
    'name', t.name,
    'vintage', t.vintage,
    'state_code', t.state_code,
    'sumlevel', t.sumlevel
  ),
  'core', jsonb_build_object(
    'total_population', t.total_population,
    'total_population_lo90', t.total_population_lo90,
    'total_population_hi90', t.total_population_hi90,
    'total_households', t.total_households,
    'total_households_lo90', t.total_households_lo90,
    'total_households_hi90', t.total_households_hi90,
    'avg_household_size', t.avg_household_size,
    'avg_household_size_lo90', t.avg_household_size_lo90,
    'avg_household_size_hi90', t.avg_household_size_hi90
  ),
  'income', CASE WHEN i.geoid IS NULL THEN NULL ELSE to_jsonb(i) - 'name' - 'geoid' - 'vintage' - 'state_code' - 'sumlevel' END,
  'education', CASE WHEN e.geoid IS NULL THEN NULL ELSE to_jsonb(e) - 'name' - 'geoid' - 'vintage' - 'state_code' - 'sumlevel' END,
  'diversity', CASE WHEN d.geoid IS NULL THEN NULL ELSE to_jsonb(d) - 'name' - 'geoid' - 'vintage' - 'state_code' - 'sumlevel' END,
  'occupation', CASE WHEN o.geoid IS NULL THEN NULL ELSE to_jsonb(o) - 'name' - 'geoid' - 'vintage' - 'state_code' - 'sumlevel' END
) AS payload
FROM target t
LEFT JOIN api.income_v1 i
  ON i.vintage = t.vintage
 AND i.sumlevel = t.sumlevel
 AND i.geoid = t.geoid
LEFT JOIN api.education_v1 e
  ON e.vintage = t.vintage
 AND e.sumlevel = t.sumlevel
 AND e.geoid = t.geoid
LEFT JOIN api.diversity_v1 d
  ON d.vintage = t.vintage
 AND d.sumlevel = t.sumlevel
 AND d.geoid = t.geoid
LEFT JOIN api.occupation_v1 o
  ON o.vintage = t.vintage
 AND o.sumlevel = t.sumlevel
 AND o.geoid = t.geoid;
`;

app.get("/api/v1/health", async (_req, res, next) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

app.get("/api/v1/geographies", async (req, res, next) => {
  try {
    const vintage = String(req.query.vintage || DEFAULT_VINTAGE);
    const result = await pool.query(QUERY_GEOGRAPHIES, [vintage]);
    res.json(result.rows[0].payload);
  } catch (err) {
    next(err);
  }
});

app.get("/api/v1/geography/:geoid", async (req, res, next) => {
  try {
    const geoid = req.params.geoid;
    const vintage = String(req.query.vintage || DEFAULT_VINTAGE);

    const sumlevel = req.query.sumlevel;

    if (!geoid) {
      res.status(400).json({ error: "GEOID is missing or invalid" });
      return;
    }
    const result = await pool.query(QUERY_GEOGRAPHY, [
      geoid,
      vintage,
      sumlevel,
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: "GEOID not found" });
      return;
    }

    res.json(result.rows[0].payload);
  } catch (err) {
    next(err);
  }
});

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(PORT, () => {
  console.log(`API listening on http://localhost:${PORT}`);
});
