import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

/**
 * VERBATIM TIMES — position_time_nz is the only derived time.
 * We do NOT alter position_time (that stays exactly as from the API). Here we only run a SQL UPDATE
 * to set position_time_nz = (position_time AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland') where nz is null. Never alter API timestamps.
 */
/** YYYY-MM-DD list from fromDate to toDate inclusive. Uses local date only (no UTC). */
function dateRange(fromDate: string, toDate: string): string[] {
  const from = new Date(fromDate + 'T00:00:00');
  const to = new Date(toDate + 'T00:00:00');
  if (from.getTime() > to.getTime()) return [];
  const out: string[] = [];
  const pad = (n: number) => String(n).padStart(2, '0');
  const cur = new Date(from);
  while (cur.getTime() <= to.getTime()) {
    out.push(`${cur.getFullYear()}-${pad(cur.getMonth() + 1)}-${pad(cur.getDate())}`);
    cur.setDate(cur.getDate() + 1);
  }
  return out;
}

function streamLine(obj: Record<string, unknown>): string {
  return JSON.stringify(obj) + '\n';
}

/**
 * POST: 1) Update position_time_nz once for the range. 2) Run store_fences_for_date_scoped for each day (see fenceScope).
 * Streams NDJSON in real time: { type: 'position_time_nz', updated } then { type: 'day', date, updated, durationMs } per day, then { type: 'done', totalUpdated }.
 * Body: { dateFrom, dateTo, fenceScope?: 'unattempted' | 'all' | 'unattempted_and_missed' }
 *   OR legacy: forceUpdate, reprocessMissedOnly.
 * - unattempted (default): only rows not yet attempted.
 * - all: reprocess every row for each day.
 * - unattempted_and_missed: unattempted pass, then missed-only pass (attempted but not mapped — e.g. new geofences).
 * Legacy reprocessMissedOnly=true: missed-only pass only (no unattempted).
 */
export async function POST(req: Request) {
  try {
    const body = await req.json();
    const dateFrom = typeof body?.dateFrom === 'string' ? body.dateFrom.trim() : '';
    const dateTo = typeof body?.dateTo === 'string' ? body.dateTo.trim() : '';

    type Scope = 'unattempted' | 'all' | 'unattempted_and_missed' | 'missed_only';
    let scope: Scope = 'unattempted';
    const fs = typeof body?.fenceScope === 'string' ? body.fenceScope.trim() : '';
    if (fs === 'all') scope = 'all';
    else if (fs === 'unattempted_and_missed') scope = 'unattempted_and_missed';
    else if (fs === 'unattempted') scope = 'unattempted';
    else if (fs === 'missed_only') scope = 'missed_only';
    else if (body?.forceUpdate === true) scope = 'all';
    else if (body?.reprocessMissedOnly === true) scope = 'missed_only';

    const forceUpdate = scope === 'all';
    const onlyMissed = scope === 'missed_only';
    const runUnattemptedThenMissed = scope === 'unattempted_and_missed';
    if (!dateFrom || !dateTo) {
      return NextResponse.json(
        { ok: false, error: 'dateFrom and dateTo (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }
    const dates = dateRange(dateFrom, dateTo);
    if (dates.length === 0) {
      return NextResponse.json(
        { ok: false, error: 'dateFrom must be ≤ dateTo' },
        { status: 400 }
      );
    }

    const positionTimeNzUpdated = await execute(
      `UPDATE public.tbl_tracking
       SET position_time_nz = (position_time AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland')
       WHERE position_time IS NOT NULL
         AND position_time_nz IS NULL
         AND position_time::date >= $1::date
         AND position_time::date <= $2::date`,
      [dateFrom, dateTo]
    );

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      async start(controller) {
        try {
          controller.enqueue(encoder.encode(streamLine({ type: 'position_time_nz', updated: positionTimeNzUpdated })));
          let totalUpdated = 0;
          for (const date of dates) {
            const startMs = Date.now();
            let updated = 0;
            if (runUnattemptedThenMissed) {
              const r1 = await query<{ n: number }>(
                'SELECT store_fences_for_date_scoped($1::date, $2::boolean, $3::boolean)::int AS n',
                [date, false, false]
              );
              const r2 = await query<{ n: number }>(
                'SELECT store_fences_for_date_scoped($1::date, $2::boolean, $3::boolean)::int AS n',
                [date, false, true]
              );
              updated = (r1[0]?.n ?? 0) + (r2[0]?.n ?? 0);
            } else {
              const rows = await query<{ store_fences_for_date_scoped: number }>(
                'SELECT store_fences_for_date_scoped($1::date, $2::boolean, $3::boolean) AS store_fences_for_date_scoped',
                [date, forceUpdate, onlyMissed]
              );
              updated = rows[0]?.store_fences_for_date_scoped ?? 0;
            }
            const durationMs = Date.now() - startMs;
            totalUpdated += updated;
            controller.enqueue(
              encoder.encode(streamLine({ type: 'day', date, updated, durationMs }))
            );
          }
          controller.enqueue(encoder.encode(streamLine({ type: 'done', totalUpdated })));
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          controller.enqueue(encoder.encode(streamLine({ type: 'error', message })));
        } finally {
          controller.close();
        }
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'application/x-ndjson',
        'Cache-Control': 'no-store',
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/store-fences-for-date-range]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
