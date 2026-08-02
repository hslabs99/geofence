import { NextResponse } from 'next/server';
import { runSupabaseGeomTests } from '@/lib/celgps-migration-geom-test';

/**
 * POST /api/celgps-migration/test-geom
 * Read-only PostGIS / store_* checks on Supabase (optional body: { probeDate: "YYYY-MM-DD" }).
 */
export async function POST(request: Request) {
  let probeDate: string | null = null;
  try {
    const body = await request.json().catch(() => ({}));
    const raw = (body as { probeDate?: string }).probeDate?.trim();
    if (raw) probeDate = raw;
  } catch {
    /* empty body ok */
  }

  try {
    const result = await runSupabaseGeomTests(probeDate);
    if (!result.ok && result.checks.length === 0) {
      return NextResponse.json(result, { status: 400 });
    }
    return NextResponse.json(result, { status: result.ok ? 200 : 502 });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message, checks: [] }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';
export const maxDuration = 120;
