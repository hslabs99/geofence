import fs from 'node:fs';
import path from 'node:path';
import { Pool, type PoolConfig } from 'pg';

/** Local single source of truth (also documents App Hosting env/secrets). */
const LOCAL_ENV_FILENAME = '.env.local';
const DB_TARGET_FILENAME = '.celgps-db-target.local';

/** Max wait when opening a new pg client (avoids hang when Cloud SQL is stopped). */
export const PG_CONNECT_TIMEOUT_MS = 5000;

/** Supabase Session pooler allows ~15 concurrent clients — keep app pool small. */
export const SUPABASE_SESSION_POOL_MAX = 4;

export function isSupabaseSessionPool(config: CelgpsMigrationConfig): boolean {
  return config.isPoolerHost || config.sslRequired;
}

/** Recommended max clients for the main app pool. */
export function getAppPoolMax(config: CelgpsMigrationConfig): number {
  return isSupabaseSessionPool(config) ? SUPABASE_SESSION_POOL_MAX : 10;
}

export type DbTarget = 'cloudsql' | 'supabase';

/** Supabase session pooler (IPv4) — use on Windows / IPv4-only networks. */
export const CELGPS_SUPABASE_POOLER_HOST = 'aws-1-ap-southeast-2.pooler.supabase.com';
export const CELGPS_SUPABASE_POOLER_PORT = 5432;
export const CELGPS_SUPABASE_PROJECT_REF = 'ticxxuvaajtqmdrdcbml';
export const CELGPS_SUPABASE_POOLER_USER = `postgres.${CELGPS_SUPABASE_PROJECT_REF}`;
export const CELGPS_SUPABASE_DIRECT_HOST = `db.${CELGPS_SUPABASE_PROJECT_REF}.supabase.co`;

export type CelgpsMigrationConfigSource =
  | 'CELGPS_DATABASE_URL'
  | 'CELGPS_CLOUD_SQL_DATABASE_URL'
  | 'SUPABASE_DATABASE_URL'
  | 'DATABASE_URL'
  | 'CELGPS_SUPABASE_PASSWORD'
  | 'PGHOST';

export interface CelgpsMigrationConfig {
  /** Set when connecting via URL; omit when using PGHOST socket fields. */
  connectionString?: string;
  host: string;
  port: number;
  user: string;
  password?: string;
  database: string;
  sslRequired: boolean;
  source: CelgpsMigrationConfigSource;
  isDirectHost: boolean;
  isPoolerHost: boolean;
}

export interface CelgpsMigrationConfigError {
  ok: false;
  error: string;
  hint?: string;
  envFilePath: string;
  envFileExists: boolean;
}

let migrationEnvLoaded = false;

/** Load gitignored `.env.local` into process.env (does not override existing vars). Next.js also loads it. */
export function loadCelgpsMigrationEnvFile(): { path: string; exists: boolean; loadedKeys: string[] } {
  const envPath = path.join(process.cwd(), LOCAL_ENV_FILENAME);
  if (migrationEnvLoaded) {
    return { path: envPath, exists: fs.existsSync(envPath), loadedKeys: [] };
  }
  migrationEnvLoaded = true;
  const loadedKeys: string[] = [];
  if (!fs.existsSync(envPath)) {
    return { path: envPath, exists: false, loadedKeys };
  }
  const text = fs.readFileSync(envPath, 'utf8');
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) {
      process.env[key] = value;
      loadedKeys.push(key);
    }
  }
  return { path: envPath, exists: true, loadedKeys };
}

function parsePostgresUrl(url: string): Omit<CelgpsMigrationConfig, 'source' | 'sslRequired'> | null {
  try {
    const u = new URL(url);
    if (u.protocol !== 'postgresql:' && u.protocol !== 'postgres:') return null;
    const host = u.hostname;
    const port = u.port ? parseInt(u.port, 10) : 5432;
    const database = u.pathname.replace(/^\//, '') || 'postgres';
    const user = decodeURIComponent(u.username || '');
    if (!host || !user) return null;
    return {
      connectionString: url,
      host,
      port,
      user,
      database,
      isDirectHost: host.includes('.supabase.co') && !host.includes('pooler'),
      isPoolerHost: host.includes('pooler.supabase.com'),
    };
  } catch {
    return null;
  }
}

function buildPoolerConnectionString(password: string): string {
  const encodedUser = encodeURIComponent(CELGPS_SUPABASE_POOLER_USER);
  const encodedPassword = encodeURIComponent(password);
  return `postgresql://${encodedUser}:${encodedPassword}@${CELGPS_SUPABASE_POOLER_HOST}:${CELGPS_SUPABASE_POOLER_PORT}/postgres`;
}

function urlLooksLikeSupabase(url: string): boolean {
  return /supabase/i.test(url);
}

function configFromParsedUrl(
  parsed: Omit<CelgpsMigrationConfig, 'source' | 'sslRequired'>,
  source: CelgpsMigrationConfigSource,
  sslRequired: boolean
): { ok: true } & CelgpsMigrationConfig & { envFilePath: string; envFileExists: boolean } {
  const envInfo = loadCelgpsMigrationEnvFile();
  return {
    ok: true,
    ...parsed,
    sslRequired,
    source,
    envFilePath: envInfo.path,
    envFileExists: envInfo.exists,
  };
}

/** Resolve Google Cloud SQL credentials for cross-check / cloudsql app target. */
export function getCloudSqlMigrationConfig():
  | ({ ok: true } & CelgpsMigrationConfig & { envFilePath: string; envFileExists: boolean })
  | CelgpsMigrationConfigError {
  const envInfo = loadCelgpsMigrationEnvFile();

  const explicit = process.env.CELGPS_CLOUD_SQL_DATABASE_URL?.trim();
  if (explicit) {
    const parsed = parsePostgresUrl(explicit);
    if (!parsed) {
      return {
        ok: false,
        error: 'CELGPS_CLOUD_SQL_DATABASE_URL is not a valid PostgreSQL URL.',
        hint: 'Use the Cloud SQL connection string (e.g. geofence@35.197.176.76/geodata).',
        envFilePath: envInfo.path,
        envFileExists: envInfo.exists,
      };
    }
    return configFromParsedUrl(parsed, 'CELGPS_CLOUD_SQL_DATABASE_URL', false);
  }

  const dbUrl = process.env.DATABASE_URL?.trim();
  if (dbUrl && !urlLooksLikeSupabase(dbUrl)) {
    const parsed = parsePostgresUrl(dbUrl);
    if (parsed) {
      return configFromParsedUrl(parsed, 'DATABASE_URL', false);
    }
  }

  const host = process.env.PGHOST?.trim() ?? '';
  if (host && process.env.PGUSER && process.env.PGPASSWORD) {
    const database = (process.env.PGDATABASE ?? 'geodata').trim();
    return {
      ok: true,
      host,
      port: host.startsWith('/') ? 5432 : parseInt(process.env.PGPORT || '5432', 10),
      user: process.env.PGUSER,
      password: process.env.PGPASSWORD,
      database,
      sslRequired: false,
      source: 'PGHOST',
      isDirectHost: false,
      isPoolerHost: false,
      envFilePath: envInfo.path,
      envFileExists: envInfo.exists,
    };
  }

  return {
    ok: false,
    error: 'No Google Cloud SQL credentials configured.',
    hint:
      'Set CELGPS_CLOUD_SQL_DATABASE_URL or DATABASE_URL / PGHOST in .env.local for Cloud SQL (legacy).',
    envFilePath: envInfo.path,
    envFileExists: envInfo.exists,
  };
}

const VALID_DB_TARGETS = new Set<DbTarget>(['cloudsql', 'supabase']);

export function getActiveDbTarget(): DbTarget {
  loadCelgpsMigrationEnvFile();
  const targetPath = path.join(process.cwd(), DB_TARGET_FILENAME);
  if (fs.existsSync(targetPath)) {
    const raw = fs.readFileSync(targetPath, 'utf8').trim().toLowerCase();
    if (VALID_DB_TARGETS.has(raw as DbTarget)) {
      return raw as DbTarget;
    }
  }
  const fromEnv = process.env.CELGPS_DB_TARGET?.trim().toLowerCase();
  if (fromEnv && VALID_DB_TARGETS.has(fromEnv as DbTarget)) {
    return fromEnv as DbTarget;
  }
  // Cloud SQL is gone — prefer Supabase whenever credentials are present.
  // Inline checks (avoid calling getCelgpsMigrationConfig here — defined later).
  if (
    process.env.CELGPS_DATABASE_URL?.trim() ||
    process.env.CELGPS_SUPABASE_PASSWORD?.trim() ||
    process.env.SUPABASE_DATABASE_URL?.trim()
  ) {
    return 'supabase';
  }
  return 'cloudsql';
}

export function setActiveDbTarget(target: DbTarget): { path: string; target: DbTarget } {
  if (!VALID_DB_TARGETS.has(target)) {
    throw new Error(`Invalid db target: ${target}`);
  }
  const targetPath = path.join(process.cwd(), DB_TARGET_FILENAME);
  fs.writeFileSync(targetPath, `${target}\n`, 'utf8');
  return { path: targetPath, target };
}

export function getDbTargetFileInfo(): { path: string; exists: boolean; target: DbTarget } {
  const targetPath = path.join(process.cwd(), DB_TARGET_FILENAME);
  return {
    path: targetPath,
    exists: fs.existsSync(targetPath),
    target: getActiveDbTarget(),
  };
}

/** Which connection string / pool config the main app should use. */
export function resolveActiveAppDbConfig():
  | ({ ok: true } & CelgpsMigrationConfig & { target: DbTarget })
  | (CelgpsMigrationConfigError & { target: DbTarget }) {
  const target = getActiveDbTarget();
  if (target === 'supabase') {
    const cfg = getCelgpsMigrationConfig();
    if (!cfg.ok) return { ...cfg, target };
    return { ...cfg, target };
  }
  const cfg = getCloudSqlMigrationConfig();
  if (!cfg.ok) return { ...cfg, target };
  return { ...cfg, target };
}

/** Supabase + node-pg: sslmode=require alone fails cert verify; libpq-compat fixes it. */
export function normalizeSupabaseDatabaseUrl(url: string): string {
  if (!/supabase\.(co|com)|pooler\.supabase\.com/i.test(url)) return url;
  let out = url;
  if (!/[?&]sslmode=/i.test(out)) {
    out += `${out.includes('?') ? '&' : '?'}sslmode=require`;
  }
  if (!/[?&]uselibpqcompat=/i.test(out)) {
    out += `${out.includes('?') ? '&' : '?'}uselibpqcompat=true`;
  }
  return out;
}

export function createMigrationPool(
  config: CelgpsMigrationConfig,
  max = 1,
  connectionTimeoutMillis = PG_CONNECT_TIMEOUT_MS
): Pool {
  const ssl = config.sslRequired ? { rejectUnauthorized: false } : undefined;
  const poolMax = isSupabaseSessionPool(config) ? Math.min(max, SUPABASE_SESSION_POOL_MAX) : max;
  const common = {
    max: poolMax,
    min: 0,
    ssl,
    connectionTimeoutMillis,
    idleTimeoutMillis: 10000,
    allowExitOnIdle: true,
  };
  if (config.connectionString) {
    return new Pool({
      connectionString: normalizeSupabaseDatabaseUrl(config.connectionString),
      ...common,
    } satisfies PoolConfig);
  }
  return new Pool({
    host: config.host,
    port: config.host.startsWith('/') ? undefined : config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    ...common,
  } satisfies PoolConfig);
}

/** Resolve Supabase target credentials (server-side only). Never return password to clients. */
export function getCelgpsMigrationConfig():
  | ({ ok: true } & CelgpsMigrationConfig & { envFilePath: string; envFileExists: boolean })
  | CelgpsMigrationConfigError {
  const envInfo = loadCelgpsMigrationEnvFile();

  const celgpsUrl = process.env.CELGPS_DATABASE_URL?.trim();
  if (celgpsUrl) {
    const parsed = parsePostgresUrl(celgpsUrl);
    if (!parsed) {
      return {
        ok: false,
        error: 'CELGPS_DATABASE_URL is not a valid PostgreSQL URL.',
        hint: 'Use the Session pooler URI from Supabase Dashboard → Connect.',
        envFilePath: envInfo.path,
        envFileExists: envInfo.exists,
      };
    }
    return configFromParsedUrl(parsed, 'CELGPS_DATABASE_URL', true);
  }

  for (const key of ['SUPABASE_DATABASE_URL', 'DATABASE_URL'] as const) {
    const url = process.env[key]?.trim();
    if (url && urlLooksLikeSupabase(url)) {
      const parsed = parsePostgresUrl(url);
      if (parsed) {
        return configFromParsedUrl(
          parsed,
          key === 'SUPABASE_DATABASE_URL' ? 'SUPABASE_DATABASE_URL' : 'DATABASE_URL',
          true
        );
      }
    }
  }

  const password = process.env.CELGPS_SUPABASE_PASSWORD?.trim();
  if (password) {
    const connectionString = buildPoolerConnectionString(password);
    return configFromParsedUrl(
      {
        connectionString,
        host: CELGPS_SUPABASE_POOLER_HOST,
        port: CELGPS_SUPABASE_POOLER_PORT,
        user: CELGPS_SUPABASE_POOLER_USER,
        database: 'postgres',
        isDirectHost: false,
        isPoolerHost: true,
      },
      'CELGPS_SUPABASE_PASSWORD',
      true
    );
  }

  return {
    ok: false,
    error: 'No Supabase migration credentials configured.',
    hint: `Set CELGPS_DATABASE_URL or DATABASE_URL (Supabase pooler) in ${LOCAL_ENV_FILENAME}, or CELGPS_SUPABASE_PASSWORD.`,
    envFilePath: envInfo.path,
    envFileExists: envInfo.exists,
  };
}

/** Mask password in a connection URL for safe API responses. */
export function maskDatabaseUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = '****';
    return u.toString();
  } catch {
    return '(invalid URL)';
  }
}

export function safeConnectionSummary(config: CelgpsMigrationConfig): {
  label: string;
  host: string;
  port: number;
  user: string;
  database: string;
  source: CelgpsMigrationConfigSource;
  maskedUrl: string;
  isDirectHost: boolean;
  isPoolerHost: boolean;
  isCloudSql: boolean;
  ipv6Warning: string | null;
} {
  const isCloudSql =
    config.source === 'CELGPS_CLOUD_SQL_DATABASE_URL' ||
    config.source === 'PGHOST' ||
    (config.source === 'DATABASE_URL' && !config.isPoolerHost && !config.isDirectHost);
  return {
    label: config.isPoolerHost || config.isDirectHost ? 'Supabase' : 'Google Cloud SQL',
    host: config.host,
    port: config.port,
    user: config.user,
    database: config.database,
    source: config.source,
    maskedUrl: config.connectionString ? maskDatabaseUrl(config.connectionString) : `(PGHOST ${config.host})`,
    isDirectHost: config.isDirectHost,
    isPoolerHost: config.isPoolerHost,
    isCloudSql,
    ipv6Warning: config.isDirectHost
      ? 'Direct db.*.supabase.co host is IPv6-only. On Windows or IPv4-only networks use the Session pooler URL instead.'
      : null,
  };
}
