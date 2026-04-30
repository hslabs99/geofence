import { Pool } from 'pg';

let pool: Pool | null = null;

function getPool(): Pool {
  if (pool) return pool;
  if (process.env.DATABASE_URL) {
    pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 10 });
    return pool;
  }
  const host = process.env.PGHOST?.trim() ?? '';
  if (!host || !process.env.PGUSER || !process.env.PGPASSWORD) {
    throw new Error('Missing DATABASE_URL or PGHOST/PGUSER/PGPASSWORD');
  }
  pool = new Pool({
    host,
    port: host.startsWith('/') ? undefined : parseInt(process.env.PGPORT || '5432', 10),
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: (process.env.PGDATABASE ?? 'geodata').trim(),
    max: 10,
  });
  return pool;
}

/** Run a SELECT (or other query that returns rows). Params use $1, $2, ... */
export async function query<T = unknown>(text: string, params?: unknown[]): Promise<T[]> {
  try {
    const res = await getPool().query(text, params);
    return (res.rows as T[]) ?? [];
  } catch (err) {
    const e = err as any;
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
      throw new Error(
        `${msg}\n\nSQL:\n${text}\n\nParams:\n${JSON.stringify(paramInfo, null, 2)}`
      );
    }
    throw err;
  }
}

/** Run INSERT/UPDATE/DELETE. Returns rowCount. */
export async function execute(text: string, params?: unknown[]): Promise<number> {
  try {
    const res = await getPool().query(text, params);
    return res.rowCount ?? 0;
  } catch (err) {
    const e = err as any;
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
      throw new Error(
        `${msg}\n\nSQL:\n${text}\n\nParams:\n${JSON.stringify(paramInfo, null, 2)}`
      );
    }
    throw err;
  }
}

/** Get a client from the pool for transactions or multiple statements. Call client.release() when done. */
export async function getClient() {
  return getPool().connect();
}
