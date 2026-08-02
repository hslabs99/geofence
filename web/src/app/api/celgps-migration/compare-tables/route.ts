import { NextResponse } from 'next/server';
import { compareTableRowCounts } from '@/lib/celgps-migration-compare';

/**
 * POST /api/celgps-migration/compare-tables
 * Table-by-table row count cross-check: Google Cloud SQL vs Supabase.
 */
export async function POST() {
  try {
    const result = await compareTableRowCounts();
    if (!result.ok) {
      return NextResponse.json(result, { status: 400 });
    }
    return NextResponse.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message, rows: [] }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';
export const maxDuration = 300;
