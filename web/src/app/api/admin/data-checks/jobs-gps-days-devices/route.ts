import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';
import {
  buildJobsGpsReportRowsParallel,
  jobsGpsRowBreachesLimits,
  JOBS_GPS_GAP_THRESHOLD_MINUTES,
} from '@/lib/jobs-gps-window-report';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
} from '@/lib/gps-harvest-constants';

const BATCH_SIZE = 200;
/** Safety cap — if more jobs match, scan stops and returns truncated: true. */
const MAX_JOBS_SCAN = 25_000;

/**
 * GET: Legacy aggregate (day + device) — **superseded** by `jobs-gps-report`, which returns `dayWorkerSummary` in the same scan.
 * Kept for compatibility; prefer the main report endpoint.
 *
 * Same filters as jobs-gps-report; scans all matching jobs (not paginated).
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const dateFrom = searchParams.get('dateFrom')?.trim();
    const dateTo = searchParams.get('dateTo')?.trim();
    if (!dateFrom || !dateTo || !/^\d{4}-\d{2}-\d{2}$/.test(dateFrom) || !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      return NextResponse.json(
        { error: 'dateFrom and dateTo (YYYY-MM-DD) are required for actual_start_time range' },
        { status: 400 }
      );
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

    const { conditions, values, debug } = buildWhereAndParams(searchParams);
    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';

    const windowOpts = {
      startLessMinutes,
      endPlusMinutes,
      displayExpandBefore,
      displayExpandAfter,
    };

    const byKey = new Map<string, { day: string; device: string; job_count: number }>();
    let totalJobsScanned = 0;
    let breachJobCount = 0;
    let totalGapSegmentRows = 0;
    let truncated = false;
    let offset = 0;

    for (;;) {
      if (offset >= MAX_JOBS_SCAN) {
        truncated = true;
        break;
      }

      const jobSql = `
      SELECT
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
      ${whereClause}
      ORDER BY t.actual_start_time ASC NULLS LAST, trim(t.job_id::text) ASC
      LIMIT ${BATCH_SIZE} OFFSET ${offset}
    `;

      const jobRows = await query<Record<string, unknown>>(jobSql, values);
      if (jobRows.length === 0) break;

      totalJobsScanned += jobRows.length;
      const builds = await buildJobsGpsReportRowsParallel(
        jobRows,
        windowOpts,
        gapThresholdMinutes,
        12
      );
      const rows = builds.map((b) => b.row);

      for (const r of rows) {
        totalGapSegmentRows += r.gap_segment_count;
        if (!jobsGpsRowBreachesLimits(r, gapThresholdMinutes)) continue;
        breachJobCount++;
        const ast = r.actual_start_time;
        const day =
          ast != null && typeof ast === 'string' && ast.length >= 10 ? ast.slice(0, 10) : '';
        const dev = r.device_name;
        if (!day || !dev) continue;
        const key = `${day}\t${dev}`;
        const cur = byKey.get(key);
        if (cur) cur.job_count += 1;
        else byKey.set(key, { day, device: dev, job_count: 1 });
      }

      if (jobRows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    const aggregateRows = [...byKey.values()].sort(
      (a, b) => a.day.localeCompare(b.day) || a.device.localeCompare(b.device)
    );

    return NextResponse.json({
      ok: true,
      totalJobsScanned,
      breachJobCount,
      jobsWithoutIssues: Math.max(0, totalJobsScanned - breachJobCount),
      totalGapSegmentRows,
      uniqueDayDeviceCount: aggregateRows.length,
      truncated,
      maxJobsScan: MAX_JOBS_SCAN,
      window: {
        startLessMinutes,
        endPlusMinutes,
        displayExpandBefore,
        displayExpandAfter,
        gapThresholdMinutes,
        dateFrom,
        dateTo,
      },
      rows: aggregateRows,
      debug,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
