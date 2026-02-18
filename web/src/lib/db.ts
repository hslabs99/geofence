import { Pool } from 'pg';

const pool =
  globalThis.pgPool ??
  new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: parseInt(process.env.PGPORT ?? '5432', 10),
    database: process.env.PGDATABASE ?? 'geodata',
    user: process.env.PGUSER ?? 'geofence',
    password: process.env.PGPASSWORD,
  });

if (process.env.NODE_ENV !== 'production') (globalThis as unknown as { pgPool?: Pool }).pgPool = pool;

export function getPool() {
  return pool;
}
