import { NextResponse } from 'next/server';

/**
 * GET /api/health/ping – no DB. Use to confirm the app returns JSON.
 */
export async function GET() {
  return NextResponse.json({ ok: true, message: 'pong' });
}
