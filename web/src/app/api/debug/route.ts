import { NextResponse } from 'next/server';

/**
 * Parse DATABASE_URL and return safe parts (no password). Returns null if invalid.
 */
function parseDatabaseUrl(url: string): { host: string; port: string; database: string; user: string } | null {
  try {
    const u = new URL(url);
    if (u.protocol !== 'postgresql:' && u.protocol !== 'postgres:') return null;
    return {
      host: u.hostname || '(empty)',
      port: u.port || '5432',
      database: u.pathname ? u.pathname.replace(/^\//, '') : '(empty)',
      user: u.username || '(empty)',
    };
  } catch {
    return null;
  }
}

/**
 * GET /api/debug – safe env and connection check (no secrets). Use to see why DB might fail.
 */
export async function GET() {
  const url = process.env.DATABASE_URL;
  const parsed = url ? parseDatabaseUrl(url) : null;

  const env = {
    DATABASE_URL_set: !!url,
    DATABASE_URL_length: url ? url.length : 0,
    parsed: parsed ?? (url ? 'invalid URL' : 'no URL'),
    PGHOST: process.env.PGHOST ? `${process.env.PGHOST.slice(0, 40)}...` : '(not set)',
    PGPORT: process.env.PGPORT ?? '(not set)',
    PGDATABASE: process.env.PGDATABASE ?? '(not set)',
    PGUSER: process.env.PGUSER ?? '(not set)',
    PGPASSWORD_set: !!process.env.PGPASSWORD,
    NODE_ENV: process.env.NODE_ENV,
  };
  return NextResponse.json(env);
}
