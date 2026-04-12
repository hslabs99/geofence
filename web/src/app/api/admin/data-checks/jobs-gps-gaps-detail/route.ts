import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import {
  JOBS_GPS_GAP_THRESHOLD_MINUTES,
  listJobsGpsWindowGaps,
} from '@/lib/jobs-gps-window-report';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
} from '@/lib/gps-harvest-constants';

/**
 * GET: Gap detail for one job (same Inspect window as tab 13 report).
 * Params: jobId (required), startLessMinutes, endPlusMinutes, displayExpandBefore, displayExpandAfter, gapThresholdMinutes.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const jobId = searchParams.get('jobId')?.trim();
    if (!jobId) {
      return NextResponse.json({ error: 'jobId is required' }, { status: 400 });
    }

    const parseMin = (key: string, def: number, min: number, max: number): number => {
      const raw = searchParams.get(key)?.trim();
      const n = raw != null && raw !== '' ? parseInt(raw, 10) : NaN;
      const v = Number.isFinite(n) ? n : def;
      return Math.min(max, Math.max(min, v));
    };
    const startLessMinutes = parseMin('startLessMinutes', DEFAULT_HARVEST_START_LESS_MINUTES, 0, 1440);
    const endPlusMinutes = parseMin('endPlusMinutes', DEFAULT_HARVEST_END_PLUS_MINUTES, 0, 1440);
    const displayExpandBefore = parseMin('displayExpandBefore', 0, 0, 1440);
    const displayExpandAfter = parseMin('displayExpandAfter', 0, 0, 1440);
    const gapThresholdMinutes = parseMin('gapThresholdMinutes', JOBS_GPS_GAP_THRESHOLD_MINUTES, 1, 24 * 60);

    const jobRows = await query<Record<string, unknown>>(
      `SELECT
        t.job_id,
        trim(t.customer) AS customer,
        t.delivery_winery,
        t.vineyard_name,
        to_char(t.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
        to_char(t.actual_end_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_end_time,
        t.worker,
        t.truck_id,
        t.step_1_via, t.step_2_via, t.step_3_via, t.step_4_via, t.step_5_via
      FROM tbl_vworkjobs t
      WHERE trim(t.job_id::text) = trim($1::text)
      LIMIT 1`,
      [jobId]
    );
    const raw = jobRows[0];
    if (!raw) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const result = await listJobsGpsWindowGaps(raw, {
      startLessMinutes,
      endPlusMinutes,
      displayExpandBefore,
      displayExpandAfter,
    }, gapThresholdMinutes);

    if (!result.ok) {
      return NextResponse.json({ error: result.error }, { status: 400 });
    }
    return NextResponse.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
