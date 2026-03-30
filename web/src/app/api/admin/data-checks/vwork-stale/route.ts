import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type VworkStaleRow = {
  job_id: string;
  worker: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  actual_start_time: string | null;
  steps_fetched: boolean | null;
  steps_fetched_when: string | null;
  /** Human-readable reason for listing this job (fresh = steps run recently but see noStepActuals filter) */
  issue: 'never_fetched' | 'no_timestamp' | 'stale' | 'fresh';
  /** Approximate hours since steps_fetched_when when issue is stale; null if never / unknown */
  hours_since_steps: number | null;
};

/**
 * GET: Jobs that need a GPS steps run — never fetched, missing timestamp, or last run older than staleHours.
 *
 * Query: staleHours (default 48), dateFrom / dateTo (optional YYYY-MM-DD on actual_start_time),
 * limit (default 500, max 2000).
 * noStepActuals=1: only jobs where step_1_actual_time … step_5_actual_time are all null (ignores stale/steps_fetched; use with date range to spot days not stepped).
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const noStepActuals =
      searchParams.get('noStepActuals') === '1' || searchParams.get('noStepActuals')?.toLowerCase() === 'true';
    const staleHoursRaw = searchParams.get('staleHours');
    let staleHours = staleHoursRaw != null ? parseInt(staleHoursRaw, 10) : 48;
    if (Number.isNaN(staleHours) || staleHours < 0) staleHours = 48;
    if (staleHours > 24 * 365) staleHours = 24 * 365;

    let limit = parseInt(searchParams.get('limit') ?? '500', 10);
    if (Number.isNaN(limit) || limit < 1) limit = 500;
    if (limit > 2000) limit = 2000;

    const dateFrom = searchParams.get('dateFrom')?.trim() || null;
    const dateTo = searchParams.get('dateTo')?.trim() || null;
    const re = /^\d{4}-\d{2}-\d{2}$/;
    if (dateFrom && !re.test(dateFrom)) {
      return NextResponse.json({ error: 'dateFrom must be YYYY-MM-DD' }, { status: 400 });
    }
    if (dateTo && !re.test(dateTo)) {
      return NextResponse.json({ error: 'dateTo must be YYYY-MM-DD' }, { status: 400 });
    }

    const params: unknown[] = [staleHours, limit];
    let p = 3;
    let dateCond = '';
    if (dateFrom) {
      dateCond += ` AND v.actual_start_time::date >= $${p}::date`;
      params.push(dateFrom);
      p++;
    }
    if (dateTo) {
      dateCond += ` AND v.actual_start_time::date <= $${p}::date`;
      params.push(dateTo);
      p++;
    }

    const noActualsCond = `
        AND v.step_1_actual_time IS NULL
        AND v.step_2_actual_time IS NULL
        AND v.step_3_actual_time IS NULL
        AND v.step_4_actual_time IS NULL
        AND v.step_5_actual_time IS NULL`;

    const sql = noStepActuals
      ? `
      SELECT
        v.job_id::text AS job_id,
        NULLIF(trim(both from coalesce(v.worker, '')), '') AS worker,
        v.vineyard_name,
        v.delivery_winery,
        to_char(v.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
        v.steps_fetched,
        to_char(v.steps_fetched_when, 'YYYY-MM-DD HH24:MI:SS') AS steps_fetched_when,
        CASE
          WHEN v.steps_fetched IS NOT TRUE THEN 'never_fetched'
          WHEN v.steps_fetched_when IS NULL THEN 'no_timestamp'
          WHEN v.steps_fetched_when < (current_timestamp - ($1::bigint * interval '1 hour')) THEN 'stale'
          ELSE 'fresh'
        END AS issue,
        CASE
          WHEN v.steps_fetched_when IS NULL THEN NULL
          ELSE round(extract(epoch from (current_timestamp - v.steps_fetched_when)) / 3600.0)::numeric
        END AS hours_since_steps
      FROM tbl_vworkjobs v
      WHERE coalesce(trim(both from v.worker), '') <> ''
        AND v.actual_start_time IS NOT NULL
        ${dateCond}
        ${noActualsCond}
      ORDER BY v.actual_start_time DESC NULLS LAST
      LIMIT $2::int
    `
      : `
      SELECT
        v.job_id::text AS job_id,
        NULLIF(trim(both from coalesce(v.worker, '')), '') AS worker,
        v.vineyard_name,
        v.delivery_winery,
        to_char(v.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
        v.steps_fetched,
        to_char(v.steps_fetched_when, 'YYYY-MM-DD HH24:MI:SS') AS steps_fetched_when,
        CASE
          WHEN v.steps_fetched IS NOT TRUE THEN 'never_fetched'
          WHEN v.steps_fetched_when IS NULL THEN 'no_timestamp'
          WHEN v.steps_fetched_when < (current_timestamp - ($1::bigint * interval '1 hour')) THEN 'stale'
          ELSE 'fresh'
        END AS issue,
        CASE
          WHEN v.steps_fetched_when IS NULL THEN NULL
          ELSE round(extract(epoch from (current_timestamp - v.steps_fetched_when)) / 3600.0)::numeric
        END AS hours_since_steps
      FROM tbl_vworkjobs v
      WHERE coalesce(trim(both from v.worker), '') <> ''
        AND v.actual_start_time IS NOT NULL
        ${dateCond}
        AND (
          v.steps_fetched IS NOT TRUE
          OR v.steps_fetched_when IS NULL
          OR v.steps_fetched_when < (current_timestamp - ($1::bigint * interval '1 hour'))
        )
      ORDER BY v.actual_start_time DESC NULLS LAST
      LIMIT $2::int
    `;

    const rows = await query<{
      job_id: string;
      worker: string | null;
      vineyard_name: string | null;
      delivery_winery: string | null;
      actual_start_time: string | null;
      steps_fetched: boolean | null;
      steps_fetched_when: string | null;
      issue: string;
      hours_since_steps: string | null;
    }>(sql, params);

    const out: VworkStaleRow[] = (rows ?? []).map((r) => ({
      job_id: r.job_id,
      worker: r.worker,
      vineyard_name: r.vineyard_name,
      delivery_winery: r.delivery_winery,
      actual_start_time: r.actual_start_time,
      steps_fetched: r.steps_fetched,
      steps_fetched_when: r.steps_fetched_when,
      issue: r.issue as VworkStaleRow['issue'],
      hours_since_steps:
        r.hours_since_steps != null && r.hours_since_steps !== ''
          ? Math.round(parseFloat(String(r.hours_since_steps)))
          : null,
    }));

    return NextResponse.json({
      staleHours,
      noStepActuals,
      dateFrom,
      dateTo,
      count: out.length,
      rows: out,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (/step_\d+_actual/i.test(message)) {
      return NextResponse.json(
        {
          error: message,
          hint: 'Ensure step actual columns exist (e.g. web/sql/add_tbl_vworkjobs_step_actual_via.sql).',
        },
        { status: 500 }
      );
    }
    if (/steps_fetched|column/i.test(message)) {
      return NextResponse.json(
        {
          error: message,
          hint: 'Add columns with web/sql/add_steps_fetched_tbl_vworkjobs.sql if missing.',
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
