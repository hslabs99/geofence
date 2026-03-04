import { NextResponse } from 'next/server';
import { Client } from 'pg';

/**
 * GET /api/health/db – simple DB check using raw pg. No Prisma.
 * Always returns JSON. Uses PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT.
 */
export async function GET() {
  const json500 = (body: Record<string, unknown>) =>
    NextResponse.json(body, { status: 500 });

  if (
    !process.env.PGHOST ||
    !process.env.PGUSER ||
    !process.env.PGPASSWORD
  ) {
    return json500({
      ok: false,
      error: 'Missing env',
      hint: 'Set PGHOST, PGUSER, PGPASSWORD (and PGDATABASE, PGPORT) in App Hosting.',
    });
  }

  const client = new Client({
    host: process.env.PGHOST,
    port: parseInt(process.env.PGPORT || '5432', 10),
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE || 'geodata',
  });

  try {
    await client.connect();
    const r = await client.query('SELECT 1 AS n');
    await client.end();
    if (r.rows?.[0]?.n === 1) {
      return NextResponse.json({ ok: true });
    }
    return json500({ ok: false, error: 'Unexpected result', hint: 'Check DB.' });
  } catch (err) {
    try {
      await client.end();
    } catch {
      // ignore
    }
    const message = err instanceof Error ? err.message : String(err);
    const code = err instanceof Error ? (err as { code?: string }).code : undefined;
    let hint = 'Check Cloud SQL attached to Cloud Run and service account has Cloud SQL Client.';
    if (message.toLowerCase().includes('econnrefused') || message.toLowerCase().includes('connection refused')) {
      hint = 'Cloud SQL not reachable. Is the instance attached to the Cloud Run service?';
    }
    if (message.toLowerCase().includes('password') || message.toLowerCase().includes('authentication')) {
      hint = 'Wrong user/password or secret PG_PASSWORD not set.';
    }
    return json500({
      ok: false,
      error: message,
      code: code ?? null,
      hint,
    });
  }
}

export const dynamic = 'force-dynamic';
