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
  const res = await getPool().query(text, params);
  return (res.rows as T[]) ?? [];
}

/** Run INSERT/UPDATE/DELETE. Returns rowCount. */
export async function execute(text: string, params?: unknown[]): Promise<number> {
  const res = await getPool().query(text, params);
  return res.rowCount ?? 0;
}

/** Get a client from the pool for transactions or multiple statements. Call client.release() when done. */
export async function getClient() {
  return getPool().connect();
}
