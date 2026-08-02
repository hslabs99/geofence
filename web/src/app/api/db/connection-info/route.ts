import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import {
  getActiveDbTarget,
  resolveActiveAppDbConfig,
  safeConnectionSummary,
} from '@/lib/celgps-migration-config';

/**
 * GET /api/db/connection-info — active DB config + live ping via shared app pool (no extra clients).
 */
export async function GET() {
  const target = getActiveDbTarget();
  const active = resolveActiveAppDbConfig();
  if (!active.ok) {
    return NextResponse.json({
      ok: false,
      connected: false,
      error: active.error,
      hint: active.hint ?? null,
      target: active.target,
      database: null,
      host: null,
      label: null,
      connectError: active.error,
      connectMs: null,
    });
  }
  const summary = safeConnectionSummary(active);
  const started = Date.now();
  try {
    await query('SELECT 1 AS n');
    const connectMs = Date.now() - started;
    return NextResponse.json({
      ok: true,
      connected: true,
      target,
      database: summary.database,
      host: summary.host,
      port: summary.port,
      label: summary.label,
      source: summary.source,
      connectError: null,
      connectMs,
      error: null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({
      ok: false,
      connected: false,
      target,
      database: summary.database,
      host: summary.host,
      port: summary.port,
      label: summary.label,
      source: summary.source,
      connectError: message,
      connectMs: Date.now() - started,
      error: message,
    });
  }
}

export const dynamic = 'force-dynamic';
