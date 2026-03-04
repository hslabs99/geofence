import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET /api/health/db – DB check using shared pg pool (same as app).
 * Uses DATABASE_URL locally or PGHOST/PGUSER/PGPASSWORD/PGDATABASE in App Hosting.
 */
export async function GET() {
  const json500 = (body: Record<string, unknown>) =>
    NextResponse.json(body, { status: 500 });

  if (!process.env.DATABASE_URL && (!process.env.PGHOST || !process.env.PGUSER || !process.env.PGPASSWORD)) {
    return json500({
      ok: false,
      error: 'Missing env',
      hint: 'Set DATABASE_URL (local) or PGHOST, PGUSER, PGPASSWORD (and PGDATABASE) in App Hosting.',
    });
  }

  try {
    const rows = await query<{ n: number }>('SELECT 1 AS n');
    if (rows[0]?.n === 1) {
      return NextResponse.json({ ok: true });
    }
    return json500({ ok: false, error: 'Unexpected result', hint: 'Check DB.' });
  } catch (err) {
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
