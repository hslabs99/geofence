import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { pairDistanceContextForJobRow, type PairDistanceContext } from '@/lib/vworkjob-pair-distance-baseline';
import { buildInspectStyleStepsFromJobRow } from '@/lib/vworkjob-inspect-step-display';

/**
 * GET ?jobId=: one job row fields for Data Checks → Diverted Jobs detail pane (steps 1–5 + notes).
 * Step columns match Query → Inspect (VWork / GPS / Manual / Final = oride else actual else VWork tap).
 */
export async function GET(request: Request) {
  try {
    const jobId = new URL(request.url).searchParams.get('jobId')?.trim() ?? '';
    if (!jobId) {
      return NextResponse.json({ error: 'jobId is required' }, { status: 400 });
    }

    const rows = await query<Record<string, unknown>>(
      `SELECT *
       FROM tbl_vworkjobs
       WHERE trim(job_id::text) = trim($1::text)
       LIMIT 1`,
      [jobId],
    );
    const row = rows[0];
    if (!row) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const pick = (...keys: string[]): unknown => {
      for (const k of keys) {
        const v = row[k];
        if (v !== undefined) return v;
      }
      return null;
    };

    const steps = buildInspectStyleStepsFromJobRow(row);

    const deliveryWineryVal = pick('delivery_winery', 'Delivery_Winery');
    const vineyardNameVal = pick('vineyard_name', 'Vineyard_Name');
    const distanceContext = await pairDistanceContextForJobRow(
      row,
      deliveryWineryVal != null ? String(deliveryWineryVal) : null,
      vineyardNameVal != null ? String(vineyardNameVal) : null
    );

    const excludedRaw = pick('excluded', 'Excluded');
    const excluded =
      excludedRaw === true || excludedRaw === 1 || String(excludedRaw).trim() === '1'
        ? 'X'
        : excludedRaw === false || excludedRaw === 0 || String(excludedRaw).trim() === '0'
          ? ''
          : excludedRaw != null && String(excludedRaw).trim() !== ''
            ? String(excludedRaw).trim()
            : null;

    const excludednotes = pick('excludednotes', 'ExcludedNotes', 'excluded_notes');
    const calcnotes = pick('calcnotes', 'CalcNotes');

    const worker = pick('worker', 'Worker');
    const actualStartTime = pick('actual_start_time', 'Actual_Start_Time');
    let nextJob: {
      job_id: string;
      actual_start_time: string | null;
      delivery_winery: string | null;
      vineyard_name: string | null;
      steps: ReturnType<typeof buildInspectStyleStepsFromJobRow>;
      distance_context: PairDistanceContext | null;
    } | null = null;

    const workerStr = worker != null ? String(worker).trim() : '';
    if (workerStr && actualStartTime != null) {
      const nextRows = await query<{
        job_id: string;
        actual_start_time: string | null;
        delivery_winery: string | null;
        vineyard_name: string | null;
      }>(
        `
          SELECT
            trim(t.job_id::text) AS job_id,
            to_char(t.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
            t.delivery_winery,
            t.vineyard_name
          FROM tbl_vworkjobs t
          WHERE t.worker = $1
            AND t.actual_start_time IS NOT NULL
            AND t.actual_start_time > $2
          ORDER BY t.actual_start_time ASC, trim(t.job_id::text) ASC
          LIMIT 1
        `,
        [workerStr, actualStartTime],
      );
      if (nextRows[0]?.job_id) {
        const nextJobId = String(nextRows[0].job_id).trim();
        const nextFullRows = await query<Record<string, unknown>>(
          `SELECT * FROM tbl_vworkjobs WHERE trim(job_id::text) = trim($1::text) LIMIT 1`,
          [nextJobId],
        );
        const nextRow = nextFullRows[0];
        const nextSteps = nextRow ? buildInspectStyleStepsFromJobRow(nextRow) : [];
        const nextDw = nextRows[0].delivery_winery != null ? String(nextRows[0].delivery_winery).trim() : null;
        const nextVn = nextRows[0].vineyard_name != null ? String(nextRows[0].vineyard_name).trim() : null;
        const nextDistanceContext = nextRow
          ? await pairDistanceContextForJobRow(nextRow, nextDw, nextVn)
          : null;
        nextJob = {
          job_id: nextJobId,
          actual_start_time: nextRows[0].actual_start_time != null ? String(nextRows[0].actual_start_time).trim().slice(0, 19) : null,
          delivery_winery: nextDw,
          vineyard_name: nextVn,
          steps: nextSteps,
          distance_context: nextDistanceContext,
        };
      }
    }

    return NextResponse.json({
      ok: true,
      job_id: String(pick('job_id', 'Job_ID') ?? jobId),
      customer: pick('customer', 'Customer'),
      template: pick('template', 'Template'),
      worker,
      delivery_winery: pick('delivery_winery', 'Delivery_Winery'),
      vineyard_name: pick('vineyard_name', 'Vineyard_Name'),
      truck_id: pick('truck_id', 'Truck_ID'),
      steps,
      distance_context: distanceContext,
      excluded,
      excludednotes: excludednotes != null ? String(excludednotes) : null,
      calcnotes: calcnotes != null ? String(calcnotes) : null,
      next_job: nextJob,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
