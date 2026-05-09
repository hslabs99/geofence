import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
} from '@/lib/gps-harvest-constants';
import {
  excludeDetourHitBecauseNextTruckJobSameVineyard,
  jobHasDetourVineyardBeforeWinery,
  loadAllMappedVineyardFenceIds,
} from '@/lib/detour-vineyard-jobs';
import type { InspectGpsWindowOptions } from '@/lib/inspect-gps-window';

export const CANCELLED_JOBS_BATCH = 100;
export const CANCELLED_JOBS_DEFAULT_SCAN_CAP = 4000;
export const CANCELLED_JOBS_MAX_SCAN_CAP = 15000;

export type CancelledJobListRow = {
  job_id: string;
  actual_start_time: string | null;
  worker: string | null;
  delivery_winery: string | null;
  vineyard_name: string | null;
  customer: string | null;
  template: string | null;
  truck_id: string | null;
};

export type CancelledJobsScanProgress =
  | { type: 'meta'; totalMatchingJobs: number; scanCap: number }
  | { type: 'progress'; scanned: number; scanCap: number; totalMatchingJobs: number; identified: number; truncated: boolean };

function rowToListRow(row: Record<string, unknown>): CancelledJobListRow | null {
  const jobId = String(row.job_id ?? row.Job_ID ?? '').trim();
  if (!jobId) return null;
  const ast =
    row.actual_start_time != null
      ? String(row.actual_start_time).trim().slice(0, 19)
      : row.Actual_Start_Time != null
        ? String(row.Actual_Start_Time).trim().slice(0, 19)
        : null;
  return {
    job_id: jobId,
    actual_start_time: ast,
    worker: row.worker != null ? String(row.worker).trim() : row.Worker != null ? String(row.Worker).trim() : null,
    delivery_winery:
      row.delivery_winery != null
        ? String(row.delivery_winery).trim()
        : row.Delivery_Winery != null
          ? String(row.Delivery_Winery).trim()
          : null,
    vineyard_name:
      row.vineyard_name != null
        ? String(row.vineyard_name).trim()
        : row.Vineyard_Name != null
          ? String(row.Vineyard_Name).trim()
          : null,
    customer: row.customer != null ? String(row.customer).trim() : row.Customer != null ? String(row.Customer).trim() : null,
    template: row.template != null ? String(row.template).trim() : row.Template != null ? String(row.Template).trim() : null,
    truck_id: row.truck_id != null ? String(row.truck_id).trim() : row.Truck_ID != null ? String(row.Truck_ID).trim() : null,
  };
}

/**
 * Scan tbl_vworkjobs with optional filters; identify diverted jobs (GPS shows another vineyard
 * between leaving the job block and winery ENTER), excluding cases where the next truck job
 * is still the same vineyard (unlikely real diversion).
 * @param emit — when set, called with meta once, progress after each batch, then caller should use returned rows (done event optional duplicate).
 */
export async function scanCancelledJobs(
  searchParams: URLSearchParams,
  emit: ((p: CancelledJobsScanProgress) => void) | null
): Promise<{
  rows: CancelledJobListRow[];
  scanned: number;
  scanCap: number;
  truncated: boolean;
  totalMatchingJobs: number;
}> {
  const dateRe = /^\d{4}-\d{2}-\d{2}$/;
  const dateFrom = searchParams.get('dateFrom')?.trim() || '';
  const dateTo = searchParams.get('dateTo')?.trim() || '';
  if (dateFrom && !dateRe.test(dateFrom)) {
    throw new Error('dateFrom must be YYYY-MM-DD when provided');
  }
  if (dateTo && !dateRe.test(dateTo)) {
    throw new Error('dateTo must be YYYY-MM-DD when provided');
  }
  if (dateFrom && dateTo && new Date(dateFrom + 'T00:00:00Z').getTime() > new Date(dateTo + 'T00:00:00Z').getTime()) {
    throw new Error('dateFrom must be <= dateTo');
  }

  let scanCap = parseInt(searchParams.get('scanCap') ?? String(CANCELLED_JOBS_DEFAULT_SCAN_CAP), 10);
  if (!Number.isFinite(scanCap) || scanCap < 1) scanCap = CANCELLED_JOBS_DEFAULT_SCAN_CAP;
  scanCap = Math.min(CANCELLED_JOBS_MAX_SCAN_CAP, Math.max(1, scanCap));

  const startLess = Math.min(
    1440,
    Math.max(
      0,
      parseInt(searchParams.get('startLessMinutes') ?? String(DEFAULT_HARVEST_START_LESS_MINUTES), 10) ||
        DEFAULT_HARVEST_START_LESS_MINUTES,
    ),
  );
  const endPlus = Math.min(
    1440,
    Math.max(
      0,
      parseInt(searchParams.get('endPlusMinutes') ?? String(DEFAULT_HARVEST_END_PLUS_MINUTES), 10) || DEFAULT_HARVEST_END_PLUS_MINUTES,
    ),
  );
  const windowOpts: InspectGpsWindowOptions = {
    startLessMinutes: startLess,
    endPlusMinutes: endPlus,
    displayExpandBefore: 0,
    displayExpandAfter: 0,
  };

  const { conditions, values } = buildWhereAndParams(searchParams);
  const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';

  const countSql = `SELECT COUNT(*)::text AS c FROM tbl_vworkjobs t${whereClause}`;
  const countRows = await query<{ c: string }>(countSql, values);
  const totalMatchingJobs = parseInt(countRows[0]?.c ?? '0', 10) || 0;
  const scanTarget = Math.min(scanCap, totalMatchingJobs);

  emit?.({ type: 'meta', totalMatchingJobs, scanCap });

  const allVineyardFenceIds = await loadAllMappedVineyardFenceIds();
  const matches: CancelledJobListRow[] = [];
  let scanned = 0;
  let truncated = false;

  for (let offset = 0; offset < scanCap; offset += CANCELLED_JOBS_BATCH) {
    const limit = Math.min(CANCELLED_JOBS_BATCH, scanCap - offset);
    const jobSql = `
        SELECT t.*
        FROM tbl_vworkjobs t
        ${whereClause}
        ORDER BY t.actual_start_time ASC NULLS LAST, trim(t.job_id::text) ASC
        LIMIT ${limit} OFFSET ${offset}
      `;
    const jobRows = await query<Record<string, unknown>>(jobSql, values);
    if (jobRows.length === 0) break;

    const chunkResults = await Promise.all(
      jobRows.map(async (row) => ({
        row,
        hit: await jobHasDetourVineyardBeforeWinery(row, windowOpts, allVineyardFenceIds),
      })),
    );

    const kept = await Promise.all(
      chunkResults.map(async ({ row, hit }) => {
        if (!hit) return { row, keep: false };
        const exclude = await excludeDetourHitBecauseNextTruckJobSameVineyard(row);
        return { row, keep: !exclude };
      }),
    );

    for (const { row, keep } of kept) {
      if (!keep) continue;
      const mapped = rowToListRow(row);
      if (mapped) matches.push(mapped);
    }

    scanned += jobRows.length;
    const hitCap = scanned >= scanCap && totalMatchingJobs > scanned;
    emit?.({
      type: 'progress',
      scanned,
      scanCap,
      totalMatchingJobs,
      identified: matches.length,
      truncated: hitCap,
    });

    if (jobRows.length < limit) break;
    if (scanned >= scanCap) {
      truncated = totalMatchingJobs > scanned;
      break;
    }
  }

  return {
    rows: matches,
    scanned,
    scanCap,
    truncated,
    totalMatchingJobs,
  };
}
