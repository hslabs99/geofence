/**
 * Simulates Firebase App Hosting runtime env (no .celgps-db-target.local)
 * and verifies Supabase connectivity the same way prod will after secrets are set.
 *
 * Usage (from web/):
 *   node scripts/test-apphosting-supabase-env.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.join(__dirname, '..');
const migrationEnvPath = path.join(webRoot, '.env.local');

function loadKey(filePath, key) {
  if (!fs.existsSync(filePath)) return null;
  for (const raw of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#') || !line.startsWith(`${key}=`)) continue;
    let value = line.slice(key.length + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    return value;
  }
  return null;
}

function withProdSslParams(url) {
  let out = url;
  if (!/[?&]sslmode=/i.test(out)) {
    out += `${out.includes('?') ? '&' : '?'}sslmode=require`;
  }
  if (!/[?&]uselibpqcompat=/i.test(out)) {
    out += `${out.includes('?') ? '&' : '?'}uselibpqcompat=true`;
  }
  return out;
}

function maskUrl(url) {
  try {
    const u = new URL(url);
    if (u.password) u.password = '***';
    return u.toString();
  } catch {
    return '(invalid url)';
  }
}

async function queryWithUrl(label, connectionString, poolOpts = {}) {
  // Match App Hosting / node-pg: connectionString only (no extra ssl object),
  // unless poolOpts.ssl is provided (migration createMigrationPool style).
  const pool = new pg.Pool({
    connectionString,
    max: 1,
    connectionTimeoutMillis: 8000,
    ...poolOpts,
  });
  const started = Date.now();
  try {
    const ping = await pool.query(
      `select current_database() as db,
              current_user as db_user,
              inet_server_addr()::text as server_addr,
              version() as version`
    );
    const tables = await pool.query(
      `select count(*)::int as n from information_schema.tables where table_schema = 'public'`
    );
    let distances = null;
    try {
      const d = await pool.query(`select count(*)::int as n from tbl_distances`);
      distances = d.rows[0]?.n ?? null;
    } catch (e) {
      distances = `error: ${e.message}`;
    }
    return {
      ok: true,
      label,
      ms: Date.now() - started,
      maskedUrl: maskUrl(connectionString),
      db: ping.rows[0]?.db,
      db_user: ping.rows[0]?.db_user,
      server_addr: ping.rows[0]?.server_addr,
      version: String(ping.rows[0]?.version ?? '').slice(0, 80),
      publicTables: tables.rows[0]?.n,
      tbl_distances: distances,
    };
  } catch (e) {
    return {
      ok: false,
      label,
      ms: Date.now() - started,
      maskedUrl: maskUrl(connectionString),
      error: e.message,
      code: e.code,
    };
  } finally {
    await pool.end().catch(() => undefined);
  }
}

async function main() {
  const rawUrl = loadKey(migrationEnvPath, 'CELGPS_DATABASE_URL');
  if (!rawUrl) {
    console.error('FAIL: CELGPS_DATABASE_URL missing in .env.local');
    process.exit(1);
  }
  const url = withProdSslParams(rawUrl);

  // Simulate App Hosting: only these DB-related vars (no PGHOST / Cloud SQL).
  const simulated = {
    DATABASE_URL: url,
    CELGPS_DATABASE_URL: url,
    CELGPS_DB_TARGET: 'supabase',
  };

  console.log('=== App Hosting env simulation ===');
  for (const [k, v] of Object.entries(simulated)) {
    console.log(`${k}=${k.includes('URL') ? maskUrl(v) : v}`);
  }
  console.log(`source_file=${migrationEnvPath}`);
  console.log(`target_file_ignored=yes (prod uses CELGPS_DB_TARGET, not .celgps-db-target.local)`);
  console.log('');

  // A) What we will put in Firebase secret DATABASE_URL (with uselibpqcompat)
  const secretStyle = await queryWithUrl(
    'A) App Hosting secret style (sslmode=require&uselibpqcompat=true)',
    url
  );
  console.log(JSON.stringify(secretStyle, null, 2));
  console.log('');

  // B) Same + migration ssl object (deployed createMigrationPool)
  const migrationStyle = await queryWithUrl(
    'B) + ssl rejectUnauthorized:false (deployed migration pool)',
    url,
    { ssl: { rejectUnauthorized: false } }
  );
  console.log(JSON.stringify(migrationStyle, null, 2));
  console.log('');

  // C) Broken form — prove we must NOT set secret with sslmode=require alone
  const brokenUrl = rawUrl.includes('sslmode=')
    ? rawUrl
    : `${rawUrl}${rawUrl.includes('?') ? '&' : '?'}sslmode=require`;
  const broken = await queryWithUrl('C) sslmode=require ONLY (must fail)', brokenUrl);
  console.log(JSON.stringify(broken, null, 2));
  console.log('');

  if (!secretStyle.ok) {
    console.error('FAIL: Prod secret-style URL cannot reach Supabase — do not deploy.');
    process.exit(1);
  }
  if (!migrationStyle.ok) {
    console.error('FAIL: Migration pool style failed — do not deploy.');
    process.exit(1);
  }
  if (broken.ok) {
    console.log('WARN: bare sslmode=require unexpectedly worked on this pg version.');
  } else {
    console.log('OK: bare sslmode=require fails as expected — secrets must include uselibpqcompat=true.');
  }

  console.log('PASS: App Hosting-style Supabase URL is ready for local Next test.');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
