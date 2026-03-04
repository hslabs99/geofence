/**
 * Import geofences from a local KML file into Cloud SQL Postgres (PostGIS).
 *
 * Prerequisites:
 *   - Start Cloud SQL Proxy so Postgres is on localhost:5432:
 *     cloud-sql-proxy.exe cel-geosystem:australia-southeast1:geofence --port 5432
 *   - Set in .env: DATABASE_URL, GEOFENCE_KML_PATH, optional GEOFENCE_TRUNCATE (default true)
 *
 * Run:
 *   npm run geofence:import
 */

import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// Load .env from repo root (parent of scripts/)
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.resolve(__dirname, "..", ".env");
const result = dotenv.config({ path: envPath, override: true });
if (result.error) {
  console.warn("dotenv:", result.error.message);
} else if (result.parsed && !result.parsed.DATABASE_URL) {
  console.warn("dotenv: loaded but DATABASE_URL missing from file. Parsed keys:", Object.keys(result.parsed).join(", "));
}
import { kml } from "@tmcw/togeojson";
import pg from "pg";
import xmldom from "xmldom";
const { DOMParser } = xmldom;

const DATABASE_URL = process.env.DATABASE_URL;
const GEOFENCE_KML_PATH = process.env.GEOFENCE_KML_PATH;
const GEOFENCE_TRUNCATE = process.env.GEOFENCE_TRUNCATE !== "false";

if (!DATABASE_URL || !GEOFENCE_KML_PATH) {
  console.error("Missing env: DATABASE_URL and GEOFENCE_KML_PATH are required.");
  process.exit(1);
}

function getFenceName(feature) {
  const props = feature.properties || {};
  return props.name ?? props.Name ?? "Unnamed Fence";
}

function promotePolygonToMultiPolygon(geometry) {
  if (geometry.type === "Polygon") {
    return { type: "MultiPolygon", coordinates: [geometry.coordinates] };
  }
  if (geometry.type === "MultiPolygon") {
    return geometry;
  }
  return null;
}

function parseKmlToFences(kmlPath) {
  const absPath = path.isAbsolute(kmlPath) ? kmlPath : path.resolve(process.cwd(), kmlPath);
  const xml = fs.readFileSync(absPath, "utf8");
  const doc = new DOMParser().parseFromString(xml, "text/xml");
  const fc = kml(doc);

  const fences = [];
  for (const feature of fc.features || []) {
    if (!feature.geometry) continue;
    const geom = promotePolygonToMultiPolygon(feature.geometry);
    if (!geom) continue;
    fences.push({
      name: getFenceName(feature),
      geom,
    });
  }
  return fences;
}

async function main() {
  const fences = parseKmlToFences(GEOFENCE_KML_PATH);
  if (fences.length === 0) {
    console.log("No Polygon/MultiPolygon placemarks found in KML. Exiting.");
    return;
  }

  const client = new pg.Client({ connectionString: DATABASE_URL });
  await client.connect();

  try {
    await client.query("CREATE EXTENSION IF NOT EXISTS postgis;");

    await client.query(`
      CREATE TABLE IF NOT EXISTS tbl_geofences (
        fence_id bigserial PRIMARY KEY,
        fence_name text NOT NULL,
        geom geometry(MultiPolygon, 4326) NOT NULL,
        created_at timestamptz DEFAULT now()
      );
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS tbl_geofences_geom_idx
      ON tbl_geofences USING GIST (geom);
    `);

    if (GEOFENCE_TRUNCATE) {
      await client.query("TRUNCATE TABLE tbl_geofences RESTART IDENTITY;");
    }

    for (const fence of fences) {
      await client.query(
        `INSERT INTO tbl_geofences (fence_name, geom)
         VALUES ($1, ST_SetSRID(ST_GeomFromGeoJSON($2), 4326))`,
        [fence.name, JSON.stringify(fence.geom)]
      );
    }

    console.log("Imported count:", fences.length);
    const names = fences.slice(0, 5).map((f) => f.name);
    console.log("First 5 fence names:", names);
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
