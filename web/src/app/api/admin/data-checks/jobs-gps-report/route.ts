import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';
import {
  buildJobsGpsReportRowsParallel,
  jobsGpsRowBreachesLimits,
  JOBS_GPS_GAP_THRESHOLD_MINUTES,
  type JobsGpsReportRow,
} from '@/lib/jobs-gps-window-report';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
} from '@/lib/gps-harvest-constants';

const BATCH_SIZE = 200;
/** Same cap as jobs-gps-days-devices — full scan stops here. */
const MAX_JOBS_SCAN = 25_000;

/**
 * GET: Jobs vs GPS window coverage — same window as Query → Inspect (`buildInspectGpsWindowForJob`).
 * Scans **all** matching jobs in batches (one GPS build pass), returns every row for client-side paging,
 * plus day/worker aggregates for the Days / Worker tab (no second scan).
 *
 * Query params: same filters as /api/vworkjobs (dateFrom, dateTo on actual_start_time, customer, template, winery, vineyard, …).
 * Required: dateFrom + dateTo (YYYY-MM-DD).
 * Optional: startLessMinutes (default 10), endPlusMinutes (default 60), displayExpandBefore, displayExpandAfter (default 0; Inspect display expand),
 * gapThresholdMinutes (default 10). (limit/offset query params are ignored — use client-side paging.)
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

    const countSql = `SELECT COUNT(*)::text AS c FROM tbl_vworkjobs t${whereClause}`;
    const countRows = await query<{ c: string }>(countSql, values);
    const totalMatchingJobs = parseInt(countRows[0]?.c ?? '0', 10) || 0;

    const windowOpts = {
      startLessMinutes,
      endPlusMinutes,
      displayExpandBefore,
      displayExpandAfter,
    };

    const byDayWorker = new Map<
      string,
      { day: string; worker: string; total_jobs: number; bad_jobs: number }
    >();
    const gapProbabilityByDay = new Map<
      string,
      {
        day: string;
        gap_from_nz: string | null;
        gap_to_nz: string | null;
        issue_gap_segment_count: number;
        job_ids: Set<string>;
      }
    >();
    const allRows: JobsGpsReportRow[] = [];
    let totalGapSegmentRows = 0;
    let breachJobCount = 0;
    let truncated = false;
    let sqlOffset = 0;

    for (;;) {
      if (sqlOffset >= MAX_JOBS_SCAN) {
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
      LIMIT ${BATCH_SIZE} OFFSET ${sqlOffset}
    `;

      const jobRows = await query<Record<string, unknown>>(jobSql, values);
      if (jobRows.length === 0) break;

      const batchBuilds = await buildJobsGpsReportRowsParallel(
        jobRows,
        windowOpts,
        gapThresholdMinutes,
        12
      );

      for (const b of batchBuilds) {
        const r = b.row;
        allRows.push(r);
        totalGapSegmentRows += r.gap_segment_count;
        const ast = r.actual_start_time;
        const day =
          ast != null && typeof ast === 'string' && ast.length >= 10 ? ast.slice(0, 10) : '—';
        const workerLabel =
          r.worker != null && String(r.worker).trim() !== ''
            ? String(r.worker).trim()
            : '(no worker)';
        const key = `${day}\t${workerLabel}`;
        const isBad = jobsGpsRowBreachesLimits(r, gapThresholdMinutes);
        if (isBad) breachJobCount++;
        const cur = byDayWorker.get(key);
        if (cur) {
          cur.total_jobs += 1;
          if (isBad) cur.bad_jobs += 1;
        } else {
          byDayWorker.set(key, {
            day,
            worker: workerLabel,
            total_jobs: 1,
            bad_jobs: isBad ? 1 : 0,
          });
        }

        if (b.gapIssueNZSpans.length > 0) {
          const dKey = b.jobCalendarDay;
          let gp = gapProbabilityByDay.get(dKey);
          if (!gp) {
            gp = {
              day: dKey,
              gap_from_nz: null,
              gap_to_nz: null,
              issue_gap_segment_count: 0,
              job_ids: new Set<string>(),
            };
            gapProbabilityByDay.set(dKey, gp);
          }
          gp.issue_gap_segment_count += b.gapIssueNZSpans.length;
          gp.job_ids.add(r.job_id);
          for (const sp of b.gapIssueNZSpans) {
            if (gp.gap_from_nz == null || sp.from < gp.gap_from_nz) gp.gap_from_nz = sp.from;
            if (gp.gap_to_nz == null || sp.to > gp.gap_to_nz) gp.gap_to_nz = sp.to;
          }
        }
      }

      if (jobRows.length < BATCH_SIZE) break;
      sqlOffset += BATCH_SIZE;
    }

    const totalJobsScanned = allRows.length;
    const dayWorkerSummary = [...byDayWorker.values()]
      .filter((x) => x.bad_jobs > 0)
      .sort((a, b) => a.day.localeCompare(b.day) || a.worker.localeCompare(b.worker));

    const gapProbabilityByDayRows = [...gapProbabilityByDay.values()]
      .map((x) => ({
        day: x.day,
        gap_from_nz: x.gap_from_nz,
        gap_to_nz: x.gap_to_nz,
        issue_gap_segments: x.issue_gap_segment_count,
        jobs_with_issue_gaps: x.job_ids.size,
      }))
      .sort((a, b) => a.day.localeCompare(b.day));

    return NextResponse.json({
      ok: true,
      /** Row count returned (same as `rows.length`). Use for paging — capped by `maxJobsScan`. */
      total: totalJobsScanned,
      totalMatchingJobs,
      limit: totalJobsScanned,
      offset: 0,
      totalJobsScanned,
      breachJobCount,
      jobsWithoutIssues: Math.max(0, totalJobsScanned - breachJobCount),
      totalGapSegmentRows,
      uniqueDayWorkerWithIssues: dayWorkerSummary.length,
      truncated,
      maxJobsScan: MAX_JOBS_SCAN,
      dayWorkerSummary,
      gapProbabilityByDay: gapProbabilityByDayRows,
      window: {
        startLessMinutes,
        endPlusMinutes,
        displayExpandBefore,
        displayExpandAfter,
        gapThresholdMinutes,
        dateFrom,
        dateTo,
      },
      rows: allRows,
      debug,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
