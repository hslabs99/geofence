/**
 * One-shot Steps+ (buffered vineyard) for a job window — same logic as
 * `/api/tracking/derived-steps` VineFence+ prep. Inspect and tagging share this path.
 * Returns diagnostics even when no merged stay exists, so the route never runs `runStepsPlusQuery` twice.
 */
import { query } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import {
  aggregateStepsPlusBufferedSegments,
  getVineyardFenceIdsForVworkName,
  normalizeTimestampString,
  type StepsPlusBufferedSegment,
} from '@/lib/derived-steps';
import { runStepsPlusQuery, type StepsPlusSegmentRow } from '@/lib/steps-plus-query';
import { getStepsPlusSettings } from '@/lib/steps-plus-settings';

export type StepsPlusMergeSnapshot = {
  fenceNames: string[];
  bufferMeters: number;
  minDurationSeconds: number;
  stepsPlusEnd: string;
  stepsPlusRows: StepsPlusSegmentRow[];
  stays: StepsPlusSegmentRow[];
  staysInJob: StepsPlusSegmentRow[];
  rawSegmentDurationsSeconds: number[];
  maxRawSegmentDurationSeconds: number | null;
  vworkEnd: string | null;
  exitCeilForStayFilter: string | null;
  mergedEnter: string;
  mergedExit: string;
  usedGpsStarMerge: boolean;
};

export type StepsPlusSnapshotAttempt =
  | { ok: true; snapshot: StepsPlusMergeSnapshot }
  | {
      ok: false;
      fenceNames: string[];
      bufferMeters: number;
      minDurationSeconds: number;
      stepsPlusEnd: string;
      stepsPlusRows: StepsPlusSegmentRow[];
      stays: StepsPlusSegmentRow[];
      staysInJob: StepsPlusSegmentRow[];
      rawSegmentDurationsSeconds: number[];
      maxRawSegmentDurationSeconds: number | null;
      vworkEnd: string | null;
      exitCeilForStayFilter: string | null;
      fail: 'no_rows' | 'no_stays_in_job' | 'merge_invalid';
    };

/**
 * Runs Steps+ query + min-duration filter + job-window stay filter + `aggregateStepsPlusBufferedSegments`.
 * Returns an attempt when fence names resolve; `null` only when vineyard/device/after are unusable.
 */
export async function tryStepsPlusSnapshotForVineyardJob(args: {
  device: string;
  positionAfter: string;
  positionBefore: string | null;
  vineyardName: string;
  jobEndCeilingBufferMinutes: number;
  vworkEnd: string | null;
}): Promise<StepsPlusSnapshotAttempt | null> {
  const vineyardName = String(args.vineyardName).trim();
  if (!vineyardName || !args.device || !String(args.positionAfter).trim()) return null;

  const mappings = await query<{ vwname: string | null; gpsname: string | null }>(
    `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard'
     AND (
       LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1::text))
       OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1::text))
     )`,
    [vineyardName]
  );
  const fenceNames: string[] = [vineyardName];
  for (const m of mappings) {
    const gps = (m.gpsname ?? '').trim();
    if (gps && !fenceNames.includes(gps)) fenceNames.push(gps);
  }
  if (fenceNames.length === 0) return null;

  const { bufferMeters: stepsPlusBufferM, minDurationSeconds: stepsPlusMinSec } = await getStepsPlusSettings();
  const stepsPlusEnd =
    args.positionBefore != null && String(args.positionBefore).trim() !== ''
      ? String(args.positionBefore).trim()
      : addMinutesToTimestampAsNZ(args.positionAfter, 24 * 60);

  const stepsPlusRows = await runStepsPlusQuery(
    args.device,
    args.positionAfter,
    stepsPlusEnd,
    fenceNames,
    stepsPlusBufferM
  );

  const commonHead = {
    fenceNames,
    bufferMeters: stepsPlusBufferM,
    minDurationSeconds: stepsPlusMinSec,
    stepsPlusEnd,
    stepsPlusRows,
    stays: [] as StepsPlusSegmentRow[],
    staysInJob: [] as StepsPlusSegmentRow[],
    rawSegmentDurationsSeconds: [] as number[],
    maxRawSegmentDurationSeconds: null as number | null,
    vworkEnd: args.vworkEnd,
    exitCeilForStayFilter: null as string | null,
  };

  if (stepsPlusRows.length === 0) {
    return { ok: false, ...commonHead, fail: 'no_rows' };
  }

  const stays = stepsPlusRows.filter((r) => Number(r.duration_seconds) >= stepsPlusMinSec);
  const rawDurNums = stepsPlusRows
    .map((r) => Number(r.duration_seconds))
    .filter((n) => Number.isFinite(n));
  const maxRawSegmentDurationSeconds =
    rawDurNums.length === 0 ? null : Math.max(...rawDurNums);

  const vworkEnd = args.vworkEnd;
  const exitCeil =
    vworkEnd != null && args.jobEndCeilingBufferMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkEnd, args.jobEndCeilingBufferMinutes)) ?? vworkEnd
      : vworkEnd;

  const staysInJob =
    vworkEnd == null
      ? stays
      : stays.filter((r) => {
          const ent = normalizeTimestampString(r.enter_time);
          const ext = normalizeTimestampString(r.exit_time);
          return (
            ent != null &&
            ext != null &&
            exitCeil != null &&
            ent < vworkEnd &&
            ext < exitCeil
          );
        });

  const common = {
    fenceNames,
    bufferMeters: stepsPlusBufferM,
    minDurationSeconds: stepsPlusMinSec,
    stepsPlusEnd,
    stepsPlusRows,
    stays,
    staysInJob,
    rawSegmentDurationsSeconds: rawDurNums,
    maxRawSegmentDurationSeconds,
    vworkEnd,
    exitCeilForStayFilter: exitCeil,
  };

  if (staysInJob.length < 1) {
    return { ok: false, ...common, fail: 'no_stays_in_job' };
  }

  const vineyardFenceIds = await getVineyardFenceIdsForVworkName(vineyardName);
  const segments: StepsPlusBufferedSegment[] = staysInJob.map((r) => ({
    enter_time: String(r.enter_time),
    exit_time: String(r.exit_time),
  }));
  const merged = await aggregateStepsPlusBufferedSegments(
    segments,
    args.device,
    args.positionAfter,
    args.positionBefore ?? null,
    vineyardFenceIds
  );
  const mergedEnter = normalizeTimestampString(merged.enter) ?? String(merged.enter).trim().slice(0, 19);
  const mergedExit = normalizeTimestampString(merged.exit) ?? String(merged.exit).trim().slice(0, 19);
  if (!mergedEnter || !mergedExit) {
    return { ok: false, ...common, fail: 'merge_invalid' };
  }

  return {
    ok: true,
    snapshot: {
      ...common,
      mergedEnter,
      mergedExit,
      usedGpsStarMerge: merged.usedGpsStarMerge,
    },
  };
}
