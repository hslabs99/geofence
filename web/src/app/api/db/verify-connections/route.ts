import { NextResponse } from 'next/server';
import { verifyBothDbTargets } from '@/lib/celgps-db-verify';

/**
 * GET /api/db/verify-connections — ping Cloud SQL and Supabase (5s timeout each, parallel).
 */
export async function GET() {
  const result = await verifyBothDbTargets();
  return NextResponse.json({
    ok: true,
    ...result,
  });
}

export const dynamic = 'force-dynamic';
