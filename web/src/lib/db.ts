import { Pool } from 'pg';
import {
  createMigrationPool,
  getActiveDbTarget,
  getAppPoolMax,
  getCelgpsMigrationConfig,
  normalizeSupabaseDatabaseUrl,
  PG_CONNECT_TIMEOUT_MS,
  resolveActiveAppDbConfig,
  setActiveDbTarget,
} from '@/lib/celgps-migration-config';
import { isDbUnreachableError } from '@/lib/celgps-db-verify';

type PgGlobal = typeof globalThis & {
  __geodataPgPool?: Pool | null;
  __geodataPgPoolKey?: string | null;
};

const pgGlobal = globalThis as PgGlobal;

let pool: Pool | null = pgGlobal.__geodataPgPool ?? null;
let poolTargetKey: string | null = pgGlobal.__geodataPgPoolKey ?? null;
let failoverAttempted = false;

function rememberPool(next: Pool, key: string): Pool {
  if (pgGlobal.__geodataPgPool && pgGlobal.__geodataPgPool !== next) {
    pgGlobal.__geodataPgPool.end().catch(() => undefined);
  }
  pgGlobal.__geodataPgPool = next;
  pgGlobal.__geodataPgPoolKey = key;
  pool = next;
  poolTargetKey = key;
  return next;
}

function poolTargetKeyFromEnv(): string {
  const cfg = resolveActiveAppDbConfig();
  if (!cfg.ok) {
    return `error:${cfg.target}`;
  }
  return `${cfg.target}:${cfg.source}:${cfg.host}:${cfg.database}:${cfg.user}`;
}

function getPool(): Pool {
  const key = poolTargetKeyFromEnv();
  if (pool && poolTargetKey === key) return pool;

  const previous = pgGlobal.__geodataPgPool ?? pool;
  if (previous) {
    previous.end().catch(() => undefined);
    pgGlobal.__geodataPgPool = null;
    pool = null;
  }

  const cfg = resolveActiveAppDbConfig();
  if (cfg.ok) {
    return rememberPool(createMigrationPool(cfg, getAppPoolMax(cfg), PG_CONNECT_TIMEOUT_MS), key);
  }

  if (process.env.DATABASE_URL) {
    const url = normalizeSupabaseDatabaseUrl(process.env.DATABASE_URL);
    const isSupabase = /supabase\.(co|com)|pooler\.supabase\.com/i.test(url);
    return rememberPool(new Pool({
      connectionString: url,
      max: isSupabase ? 4 : 10,
      connectionTimeoutMillis: PG_CONNECT_TIMEOUT_MS,
      idleTimeoutMillis: 10000,
      allowExitOnIdle: true,
      ...(isSupabase ? { ssl: { rejectUnauthorized: false } } : {}),
    }), 'legacy:DATABASE_URL');
  }
  const host = process.env.PGHOST?.trim() ?? '';
  if (!host || !process.env.PGUSER || !process.env.PGPASSWORD) {
    throw new Error(
      cfg.error ??
        'Missing DATABASE_URL or PGHOST/PGUSER/PGPASSWORD. Configure Cloud SQL or Supabase in DB Migration settings.'
    );
  }
  return rememberPool(new Pool({
    host,
    port: host.startsWith('/') ? undefined : parseInt(process.env.PGPORT || '5432', 10),
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: (process.env.PGDATABASE ?? 'geodata').trim(),
    max: 10,
    connectionTimeoutMillis: PG_CONNECT_TIMEOUT_MS,
    idleTimeoutMillis: 10000,
    allowExitOnIdle: true,
  }), 'legacy:PGHOST');
}

/** Drop cached pool so the next query uses the updated DB target. */
export function resetDbPool(): void {
  const current = pgGlobal.__geodataPgPool ?? pool;
  if (current) current.end().catch(() => undefined);
  pgGlobal.__geodataPgPool = null;
  pgGlobal.__geodataPgPoolKey = null;
  pool = null;
  poolTargetKey = null;
}

async function maybeFailoverToSupabase(err: unknown): Promise<boolean> {
  if (failoverAttempted) return false;
  if (getActiveDbTarget() !== 'cloudsql') return false;
  if (!getCelgpsMigrationConfig().ok) return false;
  if (!isDbUnreachableError(err)) return false;

  failoverAttempted = true;
  setActiveDbTarget('supabase');
  resetDbPool();
  // eslint-disable-next-line no-console
  console.warn('[db] Cloud SQL unreachable — auto-switched app DB target to Supabase');
  return true;
}

async function runQuery<T>(text: string, params?: unknown[], rowMode: 'rows' | 'rowCount' = 'rows'): Promise<T[] | number> {
  try {
    const res = await getPool().query(text, params);
    if (rowMode === 'rowCount') return res.rowCount ?? 0;
    return (res.rows as T[]) ?? [];
  } catch (err) {
    if (await maybeFailoverToSupabase(err)) {
      const res = await getPool().query(text, params);
      if (rowMode === 'rowCount') return res.rowCount ?? 0;
      return (res.rows as T[]) ?? [];
    }
    const msg = err instanceof Error ? err.message : String(err);
    if (/EMAXCONNSESSION|max clients reached in session mode/i.test(msg)) {
      throw new Error('The database connection limit is full. Wait a few seconds and try again.');
    }
    throw err;
  }
}

function enrichSqlError(err: unknown, text: string, params?: unknown[]): never {
  const e = err as { message?: string; code?: string };
  const msg = e?.message ? String(e.message) : String(err);
  const code = e?.code ? String(e.code) : '';
  const wantSql =
    /inconsistent types deduced for parameter/i.test(msg) ||
    /could not determine data type of parameter/i.test(msg) ||
    code === '42P08' ||
    code === '42P18';
  if (wantSql) {
    const p = params ?? [];
    const fmt = (v: unknown) => {
      if (v === null) return { value: null, typeof: 'null' };
      if (v === undefined) return { value: undefined, typeof: 'undefined' };
      if (Array.isArray(v)) return { value: `[array len=${v.length}]`, typeof: 'array' };
      return { value: typeof v === 'string' ? (v.length > 200 ? `${v.slice(0, 200)}…` : v) : v, typeof: typeof v };
    };
    const paramInfo = p.map((v) => fmt(v));
    throw new Error(`${msg}\n\nSQL:\n${text}\n\nParams:\n${JSON.stringify(paramInfo, null, 2)}`);
  }
  throw err;
}

export async function query<T = unknown>(text: string, params?: unknown[]): Promise<T[]> {
  try {
    return (await runQuery<T>(text, params, 'rows')) as T[];
  } catch (err) {
    enrichSqlError(err, text, params);
  }
}

export async function execute(text: string, params?: unknown[]): Promise<number> {
  try {
    return (await runQuery(text, params, 'rowCount')) as number;
  } catch (err) {
    enrichSqlError(err, text, params);
  }
}

export async function getClient() {
  return getPool().connect();
}

export function resetDbFailoverGuard(): void {
  failoverAttempted = false;
}
