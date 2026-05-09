import { NextResponse } from 'next/server';
import { scanCancelledJobs } from '@/lib/cancelled-jobs-scan';

/**
 * GET: Jobs with a vineyard detour before return winery (std ENTER/EXIT tags only).
 *
 * Filters: only query params you pass are applied (via `buildWhereAndParams`). Optional `dateFrom` / `dateTo`
 * (YYYY-MM-DD) on `actual_start_time` — omit both for no date filter (still bounded by `scanCap`).
 * Optional: customer, template, winery, vineyard.
 * Optional: startLessMinutes, endPlusMinutes (Inspect GPS window; defaults match harvest constants).
 * Optional: scanCap (max tbl_vworkjobs rows scanned from filter, default 4000, max 15000).
 *
 * Optional: `stream=1` — NDJSON stream: first line `{type:"meta",...}`, then `{type:"progress",...}` per batch,
 * final line `{type:"done",rows,scanned,scanCap,truncated,totalMatchingJobs}`.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const stream = searchParams.get('stream') === '1' || searchParams.get('stream') === 'true';

    if (stream) {
      const encoder = new TextEncoder();
      const streamOut = new ReadableStream({
        async start(controller) {
          const send = (obj: unknown) => {
            controller.enqueue(encoder.encode(`${JSON.stringify(obj)}\n`));
          };
          try {
            const result = await scanCancelledJobs(searchParams, (ev) => {
              send(ev);
            });
            send({
              type: 'done',
              rows: result.rows,
              scanned: result.scanned,
              scanCap: result.scanCap,
              truncated: result.truncated,
              totalMatchingJobs: result.totalMatchingJobs,
            });
          } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            send({ type: 'error', error: message });
          } finally {
            controller.close();
          }
        },
      });
      return new Response(streamOut, {
        headers: {
          'Content-Type': 'application/x-ndjson; charset=utf-8',
          'Cache-Control': 'no-store',
        },
      });
    }

    const result = await scanCancelledJobs(searchParams, null);
    const dateFrom = searchParams.get('dateFrom')?.trim() || '';
    const dateTo = searchParams.get('dateTo')?.trim() || '';

    return NextResponse.json({
      ok: true,
      dateFrom,
      dateTo,
      scanned: result.scanned,
      scanCap: result.scanCap,
      truncated: result.truncated,
      totalMatchingJobs: result.totalMatchingJobs,
      count: result.rows.length,
      rows: result.rows,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
