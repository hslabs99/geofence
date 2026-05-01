/**
 * Concise headlines + expandable detail for Inspect → Steps debug → Explanation tab.
 */

import type { CleanupRulesReport } from '@/lib/derived-steps';

export type InspectExplanationItem = {
  id: string;
  /** One line to scan first */
  headline: string;
  /** Shown when expanded (omit or empty = headline-only row, no chevron) */
  detail?: string[];
};

function str(v: unknown): string {
  if (v == null) return '';
  return String(v).trim();
}

/** Inspect → Steps debug → Explanation: first technical block after Run — Step1(lastJobEnd) acid test. */
function buildAcidTestStep1Item(api: Record<string, unknown>): InspectExplanationItem | null {
  const raw = api.step1LastJobEnd;
  if (raw == null || typeof raw !== 'object') {
    return {
      id: 'acid-step1-missing',
      headline: 'Acid Test Step1 · no payload (refetch with write-back or newer API)',
      detail: ['Server did not return step1LastJobEnd on this response.'],
    };
  }
  const r = raw as Record<string, unknown>;
  const applied = r.applied === true;
  const reason = str(r.reason);
  const path = str(r.path);
  const dbg = r.debug && typeof r.debug === 'object' ? (r.debug as Record<string, unknown>) : null;
  const prevId = str(dbg?.previousJobId) || str(r.previousJobId);
  const prevS5 = str(dbg?.previousJobStep5At).slice(0, 19);
  const actualStart = str(dbg?.thisJobActualStartAt).slice(0, 19);
  const lim = dbg?.limitMinutes;
  const limStr = lim != null && String(lim).trim() !== '' ? String(lim) : '—';

  const yn = (v: unknown) => (v === true ? 'yes' : v === false ? 'no' : '—');

  const detail: string[] = [];
  if (dbg) {
    detail.push(`This job actual_start: ${actualStart || '—'}.`);
    detail.push(
      `Outbound polygon snapshot: vineyard ENTER ${yn(dbg.vineyardEnterFound)} · vineyard EXIT ${yn(dbg.vineyardExitFound)} · morning winery EXIT ${yn(dbg.morningWineryExitFound)}.`
    );
    detail.push(`Missing both vineyard Step 2 & 3 (polygon): ${yn(dbg.missingVineyardStep23)} — when yes, Step1(lastJobEnd) may move start toward previous job Step 5 in either time direction (within limit).`);
    detail.push(`Step 1 from previous Job Limit: ${limStr} minutes.`);
    if (path) detail.push(`Branch evaluated: ${path}.`);
    if (prevId) detail.push(`Previous job id (chain): ${prevId}.`);
    if (prevS5) detail.push(`Previous job Step 5 candidate (actual preferred, else GPS column): ${prevS5}.`);
  }

  const why = acidTestStep1WhyLine(reason, applied, path);
  detail.push(`Why ${applied ? 'imposed' : 'not imposed'}: ${why}`);

  const headline = applied
    ? `Acid Test Step1 · imposed · previous job ${prevId || '—'} · Step 5 at ${prevS5 || '—'}`
    : `Acid Test Step1 · not imposed · ${reason || 'unknown'}`;

  return { id: 'acid-step1', headline, detail };
}

/** Part 3b cleanup audit (`cleanupRulesReport` from derived-steps). */
function pushCleanupRulesAudit(out: InspectExplanationItem[], api: Record<string, unknown>): void {
  const raw = api.cleanupRulesReport;
  if (raw == null || typeof raw !== 'object') {
    out.push({
      id: 'cleanup-audit-missing',
      headline: 'Cleanup Part 3b · no audit payload',
      detail: [
        'Refetch steps after deploy — the API should return cleanupRulesReport (step 1 travel/start, Step3windback, step4_order, step4_mid_35).',
      ],
    });
    return;
  }
  const cr = raw as CleanupRulesReport;

  if (cr.step1.applied && cr.step1.rule) {
    const ruleLabel = cr.step1.rule === 'cleanup_start' ? 'cleanup_start (step 1 missing, VWork start after step 2)' : 'travel (VWork start after step 2)';
    out.push({
      id: 'cleanup-step1',
      headline: `Cleanup · step 1 · ${cr.step1.rule}`,
      detail: [
        ruleLabel,
        `Before: ${str(cr.step1.step1Before).slice(0, 19) || '—'}`,
        `After: ${str(cr.step1.step1After).slice(0, 19) || '—'}`,
      ],
    });
  }

  if (cr.step3Windback) {
    const w = cr.step3Windback;
    out.push({
      id: 'cleanup-step3windback',
      headline: `Step3windback · ${w.path} · step 3 actual adjusted`,
      detail: [
        'GPS step 4 (winery ENTER) is trusted; merged step 3 (VWork, no step_3 GPS) was after step 4 — impossible order.',
        `Merged step 3 before: ${w.mergedStep3Before.slice(0, 19)}`,
        `GPS step 4: ${w.step4Gps.slice(0, 19)}`,
        `Outbound minutes used (step 2 − step 1, capped at 24h): ${w.outboundMinutes}`,
        w.path === 'wind'
          ? 'Applied: step 4 minus outbound (wind back); result stayed strictly after step 2.'
          : 'Applied: midpoint between step 2 and step 4 (wind back would not stay after step 2).',
        `Step 3 after: ${w.step3After.slice(0, 19)}`,
        'CalcNotes: Step3windback: (on write-back). step_3_via = Step3windback.',
      ],
    });
  }

  if (cr.step4Order) {
    const o = cr.step4Order;
    out.push({
      id: 'cleanup-step4-order',
      headline: 'Cleanup · step4_order · step 4 actual adjusted forward',
      detail: [
        'Step 3 is GPS (or Step3windback did not apply); merged step 4 was still before step 3 — step 4 pushed to step 3 plus morning outbound.',
        `Merged step 4 before: ${o.mergedStep4Before.slice(0, 19)}`,
        `Step 3 at: ${o.step3At.slice(0, 19)}`,
        `Outbound minutes (step 2 − step 1, capped): ${o.outboundMinutes}`,
        `Step 4 after: ${o.step4After.slice(0, 19)}`,
        'step_4_via = RULE when not overridden.',
      ],
    });
  }

  if (cr.step4Mid35) {
    const m = cr.step4Mid35;
    out.push({
      id: 'cleanup-step4-mid35',
      headline: 'Cleanup · step4_mid_35 · step 4 midpoint (step 3 ↔ step 5)',
      detail: [
        'Merged step 4 was still missing after GPS/VWork; step 3 and step 5 both present — set step 4 to the time halfway between them.',
        `Step 3 at: ${m.step3At.slice(0, 19)}`,
        `Step 5 at: ${m.step5At.slice(0, 19)}`,
        `Step 4 after: ${m.step4After.slice(0, 19)}`,
        'step_4_via = RULE when not overridden.',
      ],
    });
  }

  if (!cr.step1.applied && cr.step3Windback == null && cr.step4Order == null && cr.step4Mid35 == null) {
    out.push({
      id: 'cleanup-none',
      headline: 'Cleanup Part 3b · no rules applied',
      detail: [
        'Merged GPS∨VWork times did not trigger cleanup_start, travel, Step3windback, step4_order, or step4_mid_35.',
      ],
    });
  }
}

function acidTestStep1WhyLine(reason: string, applied: boolean, path: string): string {
  if (applied) {
    return 'step_1_completed_at set from previous job Step 5; original imported Step 1 stored in step_1_safe (once).';
  }
  switch (reason) {
    case 'write_back_disabled':
      return 'Derived-steps ran without write-back — Step1(lastJobEnd) only runs when GPS write-back is on.';
    case 'step1_oride':
      return 'step1oride is set — manual Step 1 override blocks this automation.';
    case 'step_1_safe_set':
      return 'step_1_safe already populated — original import preserved; rule does not re-run.';
    case 'no_worker':
      return 'No worker / device on job — cannot resolve driver chain.';
    case 'no_delivery_winery':
      return 'No delivery_winery — cannot match previous job winery.';
    case 'no_actual_start':
      return 'No actual_start_time — cannot anchor this job in the day chain.';
    case 'winery_exit_found':
      return 'Legacy path: a morning winery EXIT exists before vineyard ENTER — rule expects no such exit on that path (use missing Step 2+3 branch instead if both vineyard steps absent).';
    case 'no_previous_job':
      return 'No earlier job the same day for same worker + delivery_winery — nothing to inherit Step 5 from.';
    case 'previous_no_step5_gps':
      return 'Previous job row has no step_5_gps_completed_at — cannot confirm winery exit GPS for that job.';
    case 'no_step5_time':
      return 'Could not read a usable Step 5 timestamp from the previous job row.';
    case 'previous_end_not_before_current_start':
      return 'Legacy path: previous Step 5 is not strictly before this actual_start — timeline does not fit legacy ordering (see missing Step 2+3 branch for forward fixes).';
    case 'previous_job_gap_exceeds_limit':
      return `Gap between previous Step 5 and this actual_start exceeds "Step 1 from previous Job Limit" (${path === 'missing_vineyard_step23' ? 'symmetric' : 'one-way'}) minutes.`;
    case 'gap_minutes_unresolved':
      return 'Could not compute minutes between timestamps — check formats.';
    case 'no_time_change':
      return 'Previous Step 5 candidate equals this actual_start — no change needed.';
    case 'db_update_skipped':
      return 'UPDATE matched 0 rows (e.g. job_id mismatch or step_1_safe already set under race).';
    default:
      return reason ? `Code: ${reason}.` : 'Unknown reason.';
  }
}

function has(v: unknown): boolean {
  return v != null && String(v).trim() !== '';
}

function viaLabel(v: unknown): string {
  const s = str(v);
  return s || '—';
}

function fmtMinSec(sec: unknown): string {
  const n = typeof sec === 'number' ? sec : parseInt(String(sec ?? ''), 10);
  if (!Number.isFinite(n) || n <= 0) return '—';
  if (n % 60 === 0) return `${n / 60} min`;
  if (n > 120) return `${(n / 60).toFixed(1)} min (${n}s)`;
  return `${n}s`;
}

/** Human-readable duration for a segment length (may be fractional seconds from SQL). */
function formatSegmentDuration(sec: number): string {
  if (!Number.isFinite(sec) || sec < 0) return '—';
  const rounded = Math.round(sec * 10) / 10;
  if (rounded >= 60) {
    const m = Math.floor(rounded / 60);
    const s = Math.round(rounded % 60);
    return s > 0 ? `${m}m ${s}s (${Math.round(rounded)}s)` : `${m} min (${Math.round(rounded)}s)`;
  }
  return `${rounded}s`;
}

export function buildInspectDerivedStepsExplanation(api: Record<string, unknown>): InspectExplanationItem[] {
  const out: InspectExplanationItem[] = [];

  const err = str(api.error);
  if (err) {
    out.push({
      id: 'error',
      headline: `Error · ${err.length > 80 ? `${err.slice(0, 77)}…` : err}`,
      detail: err.length > 80 ? [err] : undefined,
    });
  }

  const jobId =
    str(api.debug && typeof api.debug === 'object' ? (api.debug as { jobId?: unknown }).jobId : undefined) ||
    str((api as { jobId?: unknown }).jobId);

  out.push({
    id: 'intro',
    headline: `Run${jobId ? ` · job ${jobId}` : ''}`,
    detail: [
      'Uses tbl_tracking for the Inspect window (start less / end plus), vineyard & winery fence mappings, guardrails, then optional Steps+ (buffer around vineyard polygons).',
      'Headlines are the quick read; open a row for why.',
    ],
  });

  const acid = buildAcidTestStep1Item(api);
  if (acid) out.push(acid);
  pushCleanupRulesAudit(out, api);

  const initial = api.initialPass as Record<string, unknown> | undefined;
  const sPlus = api.stepsPlusReport as Record<string, unknown> | undefined;

  const poly2 = initial?.step2Gps;
  const poly3 = initial?.step3Gps;
  const vName = str(sPlus?.vineyardName) || 'mapped vineyard';

  if (initial) {
    const step2Fence = str(initial.step2PolygonFenceName);
    const step2Gid = initial.step2MatchedGeofenceId;
    const step2SearchList = Array.isArray(initial.step2PolygonSearchFenceNames)
      ? (initial.step2PolygonSearchFenceNames as unknown[]).map((x) => str(x)).filter(Boolean)
      : [];
    const step2SearchLine =
      step2SearchList.length > 0
        ? `Mapped vineyard search set (VWork + tbl_gpsmappings → tbl_geofences): ${step2SearchList.join('; ')}.`
        : 'Mapped vineyard search set: (not in snapshot — refetch with newer API for names).';

    if (has(poly2)) {
      const fenceBit = step2Fence ? ` · ${step2Fence}` : '';
      const gidDetail =
        step2Gid != null && (typeof step2Gid === 'number' || typeof step2Gid === 'string')
          ? `tbl_tracking.geofence_id on winning row: ${String(step2Gid)}.`
          : null;
      out.push({
        id: 'poly2-ok',
        headline: `Step 2 · Polygon OK${fenceBit} · arrive ${str(poly2).slice(0, 19)}`,
        detail: [
          step2Fence
            ? `Winning ENTER: geofence “${step2Fence}” (one of several mapped names for this vineyard).`
            : 'Winning ENTER: fence name not in snapshot (older API).',
          ...(gidDetail ? [gidDetail] : []),
          step2SearchLine,
          `Via: ${viaLabel(initial.step2Via)}.`,
        ],
      });
    } else {
      out.push({
        id: 'poly2-fail',
        headline: `Step 2 · Polygon failed · no vineyard ENTER for “${vName}”`,
        detail: [
          'No tbl_tracking ENTER row whose geofence_id maps to this job’s vineyard fences.',
          step2SearchLine,
          `Via after first pass: ${viaLabel(initial.step2Via)}.`,
        ],
      });
    }

    if (has(poly3)) {
      out.push({
        id: 'poly3-ok',
        headline: `Step 3 · Polygon OK · depart ${str(poly3).slice(0, 19)}`,
        detail: [`Via: ${viaLabel(initial.step3Via)}. EXIT on mapped vineyard fences.`],
      });
    } else {
      out.push({
        id: 'poly3-fail',
        headline: `Step 3 · Polygon failed · no vineyard EXIT for “${vName}”`,
        detail: [
          'No tbl_tracking EXIT on mapped vineyard fence ids after the arrive time (or none in window).',
          `Via: ${viaLabel(initial.step3Via)}.`,
        ],
      });
    }
  } else {
    out.push({
      id: 'poly-snapshot-missing',
      headline: 'Step 2–3 · Polygon snapshot missing (older API)',
      detail: ['Refetch after deploy to get initialPass, or read Step_N_GPS on the job row.'],
    });
  }

  if (!sPlus) {
    out.push({
      id: 'vine-missing-report',
      headline: 'Vine+ · Report missing (older API)',
      detail: ['Server did not return stepsPlusReport; VineFence+ / VineFenceV+ may still be in Via.'],
    });
    return finishFinal(out, api);
  }

  if (sPlus.eligible === false) {
    const r = str(sPlus.reason);
    const short =
      r === 'no_vineyard_name_on_job'
        ? 'Vine+ · Skipped · job has no vineyard name'
        : r === 'write_back_disabled'
          ? 'Vine+ · Skipped · write-back off (Inspect refetch uses write-back)'
          : `Vine+ · Skipped · ${r || 'not eligible'}`;
    out.push({
      id: 'vine-ineligible',
      headline: short,
      detail:
        r === 'no_vineyard_name_on_job'
          ? ['Steps+ needs a vineyard on the job row to load fence names and buffer geometry.']
          : r === 'write_back_disabled'
            ? ['Call derived-steps with writeBack so Steps+ runs.']
            : undefined,
    });
    return finishFinal(out, api);
  }

  const minSec = sPlus.minDurationSeconds;
  const minLabel = fmtMinSec(minSec);
  const fn = sPlus.fenceNames;
  const fenceLine =
    Array.isArray(fn) && fn.length > 0 ? `Fences for vineyard: ${fn.join(', ')}.` : 'Fence names from tbl_gpsmappings + tbl_geofences.';

  const oc = str(sPlus.outcome);
  const raw = sPlus.rawSegmentCount;
  const afterDur = sPlus.afterMinDurationCount;
  const inJob = sPlus.staysInJobCount;

  if (oc === 'no_buffered_stays_found') {
    out.push({
      id: 'vine-fail-none',
      headline: `Vine+ · Failed · no contiguous time inside buffered fences (${String(raw ?? 0)} segment${Number(raw) === 1 ? '' : 's'})`,
      detail: [
        fenceLine,
        `Buffer ${str(sPlus.bufferMeters)} m; window ends ${str(sPlus.stepsPlusEnd) || '—'}.`,
        'Typical causes: no lat/lon in window, points never inside ST_Buffer(geom), or fence_name mismatch vs tbl_geofences.',
      ],
    });
  } else if (oc === 'all_segments_below_min_duration') {
    const maxS = sPlus.maxRawSegmentDurationSeconds;
    const maxHead =
      typeof maxS === 'number' && Number.isFinite(maxS)
        ? ` · longest ${formatSegmentDuration(maxS)}`
        : '';
    const rawArr = Array.isArray(sPlus.rawSegmentDurationsSeconds)
      ? (sPlus.rawSegmentDurationsSeconds as unknown[])
          .map((x) => Number(x))
          .filter((n) => Number.isFinite(n))
      : [];
    const sortedDesc = [...rawArr].sort((a, b) => b - a);
    const detailLines: string[] = [
      `${fenceLine} Raw segments before duration filter: ${String(raw ?? '—')}; after min-duration: ${String(afterDur ?? 0)}.`,
    ];
    if (typeof maxS === 'number' && Number.isFinite(maxS)) {
      detailLines.push(
        `Maximum segment duration: ${formatSegmentDuration(maxS)} — still below the ${minLabel} (${str(sPlus.minDurationSeconds)}s) minimum.`
      );
    }
    if (sortedDesc.length > 0) {
      detailLines.push(
        `All segment lengths (longest first): ${sortedDesc.map((d) => formatSegmentDuration(d)).join('; ')}.`
      );
    }
    detailLines.push(`Every run was shorter than ${minLabel} — not used as a stay.`);
    out.push({
      id: 'vine-fail-short',
      headline: `Vine+ · Failed · inside buffer but under min stay (${minLabel} required)${maxHead}`,
      detail: detailLines,
    });
  } else if (oc === 'no_stay_in_job_window') {
    out.push({
      id: 'vine-fail-window',
      headline: `Vine+ · Failed · stay not in job window (${String(inJob ?? 0)} in-window after filters)`,
      detail: [
        `${fenceLine}`,
        `Job end (VWork): ${str(sPlus.vworkEnd) || '—'}; exit must be before ${str(sPlus.exitCeilForStayFilter) || 'ceiling'}.`,
        'Enter must be before job completion; exit before job end + job-end ceiling buffer. Stays crossing that window are dropped.',
      ],
    });
  } else if (oc === 'buffer_skipped_polygon_complete_vinefencev_not_wider') {
    out.push({
      id: 'vine-skip-vplus',
      headline: 'Vine+ · Skipped · polygon had step 2 & 3; VineFenceV+ did not widen enter',
      detail: [
        'Buffered enter was not clearly earlier than polygon enter by enough minutes (see VineFenceV+ rules / queue threshold).',
        str(sPlus.detail) || '',
      ].filter(Boolean),
    });
  } else if (oc === 'buffer_pipeline_guardrails_failed') {
    out.push({
      id: 'vine-revert',
      headline: 'Vine+ · Reverted · guardrails dropped GPS step 2 or 3 after buffer merge',
      detail: [
        str(sPlus.detail) || 'Re-derived winery/vineyard steps failed ordering, job end, or duplicate tracking id checks — kept pre-buffer polygon pass.',
      ],
    });
  } else if (oc === 'applied_vinefence_plus' || oc === 'applied_vinefence_v_plus') {
    const isV = oc === 'applied_vinefence_v_plus';
    out.push({
      id: 'vine-ok',
      headline: isV ? 'Vine+ · OK · VineFenceV+ (buffered enter + polygon exit)' : 'Vine+ · OK · VineFence+ (buffered stay used)',
      detail: [
        has(sPlus.mergedEnter) && has(sPlus.mergedExit)
          ? `Merged enter ${str(sPlus.mergedEnter).slice(0, 19)} → exit ${str(sPlus.mergedExit).slice(0, 19)}.`
          : '',
        sPlus.usedGpsStarMerge === true ? 'Multiple buffer segments merged (GPS* style).' : '',
        `Buffer ${str(sPlus.bufferMeters)} m; min stay ${minLabel}.`,
      ].filter(Boolean),
    });
  } else if (oc) {
    out.push({
      id: 'vine-other',
      headline: `Vine+ · ${oc.replace(/_/g, ' ')}`,
      detail: has(sPlus.detail) ? [str(sPlus.detail)] : undefined,
    });
  }

  if (sPlus.eligible === true && !oc && has(sPlus.vineyardName)) {
    out.push({
      id: 'vine-partial',
      headline: `Vine+ · Ran · ${str(sPlus.vineyardName)}`,
      detail: [fenceLine, `Segments raw ${String(raw)} · ≥min ${String(afterDur)} · in job ${String(inJob)}.`].filter(Boolean),
    });
  }

  return finishFinal(out, api);
}

/** Inspect → Steps debug → Explanation: morning winery EXIT (step 1) bounds + re-enter NOT EXISTS rule. */
function pushStep1MorningExitAudit(out: InspectExplanationItem[], api: Record<string, unknown>): void {
  const dbg = api.debug && typeof api.debug === 'object' ? (api.debug as Record<string, unknown>) : null;
  const winery = dbg?.winery && typeof dbg.winery === 'object' ? (dbg.winery as Record<string, unknown>) : null;
  const s1 = winery?.step1MorningExitSearch;
  if (s1 == null || typeof s1 !== 'object') {
    out.push({
      id: 'step1-morning-audit-missing',
      headline: 'Step 1 · Morning winery EXIT audit · not in response',
      detail: [
        'Refetch steps after deploy — the API should return debug.winery.step1MorningExitSearch.',
      ],
    });
    return;
  }
  const w = s1 as Record<string, unknown>;
  const lower = str(w.lowerExclusive).slice(0, 19) || '—';
  const upper = str(w.upperExclusive).slice(0, 19) || '—';
  const oc = str(w.morningExitOutcome).replace(/_/g, ' ');
  const headline =
    oc === 'main query found exit'
      ? `Step 1 · Morning winery EXIT · found · lower>${lower} · upper<${upper}`
      : `Step 1 · Morning winery EXIT · ${oc} · lower>${lower} · upper<${upper}`;

  const naive = w.naiveFirstWineryExit && typeof w.naiveFirstWineryExit === 'object' ? (w.naiveFirstWineryExit as Record<string, unknown>) : null;
  // Legacy field (winery ENTER blocker) — rule no longer uses it; keep null-safe in case older payloads include it.
  const blk =
    w.blockingWineryEnterBeforeNaiveExit && typeof w.blockingWineryEnterBeforeNaiveExit === 'object'
      ? (w.blockingWineryEnterBeforeNaiveExit as Record<string, unknown>)
      : null;

  const detail: string[] = [];
  // Keep this block ruthlessly task-focused: show the two bounds and the two witness rows.
  detail.push(`Lower bound (strict): ${lower} (EXIT must be after this).`);
  detail.push(`Upper bound (strict): ${upper} (EXIT must be before this).`);
  const fenceIds = Array.isArray(w.wineryFenceIdsUsed)
    ? (w.wineryFenceIdsUsed as unknown[]).map((n) => String(n)).filter((s) => s !== '')
    : [];
  const notExists = str(w.notExistsCheckPlain);
  if (notExists) {
    detail.push(`Re-enter check (why an EXIT can be rejected): ${notExists}`);
  }
  if (naive && (str(naive.asGridRow) || str(naive.positionTimeNz) || str((naive as { time?: unknown }).time))) {
    detail.push('Naive first winery EXIT in the bounds (this is the EXIT being discussed):');
    detail.push(str(naive.asGridRow) || '—');
  }
  // Do not print blk anymore; current rule ignores winery ENTERs.
  if (fenceIds.length > 0) detail.push(`Mapped winery geofence_id list (for both rows): [${fenceIds.join(', ')}].`);
  // Keep summaryLine last; it’s the human one-liner.
  if (str(w.summaryLine)) detail.push(str(w.summaryLine));
  out.push({ id: 'step1-morning-exit', headline, detail });
}

/** Inspect → Steps debug → Explanation: Part 1 (X,Y) window + Part 2 accept for GPS step 5. */
function pushStep5GpsWindowAudit(out: InspectExplanationItem[], api: Record<string, unknown>): void {
  const dbg = api.debug && typeof api.debug === 'object' ? (api.debug as Record<string, unknown>) : null;
  const winery = dbg?.winery && typeof dbg.winery === 'object' ? (dbg.winery as Record<string, unknown>) : null;
  const sw = winery?.step5SearchWindow;
  const dec = dbg?.step5Decide && typeof dbg.step5Decide === 'object' ? (dbg.step5Decide as Record<string, unknown>) : null;
  if (sw == null || typeof sw !== 'object') {
    out.push({
      id: 'step5-audit-missing',
      headline: 'Step 5 · GPS window audit · not in response',
      detail: [
        'Refetch steps after deploy — the API should return debug.winery.step5SearchWindow and debug.step5Decide.',
      ],
    });
    return;
  }
  const w = sw as Record<string, unknown>;
  const x = str(w.lowerExclusive).slice(0, 19) || '—';
  const y = str(w.upperExclusive).slice(0, 19) || '—';
  const skip = str(w.fetchSkippedReason);
  const headline =
    skip !== '' && skip !== 'null'
      ? `Step 5 · Part 1 · fetch skipped (${skip.replace(/_/g, ' ')})`
      : `Step 5 · Part 1 · winery EXIT must satisfy X < t < Y (strict) · X=${x} · Y=${y}`;

  const detail: string[] = [];
  detail.push(str(w.summaryLine));
  detail.push(`X lowerExclusive (GPS step 4 winery ENTER, strict lower bound): ${x}.`);
  detail.push(`Y upperExclusive (strict — EXIT must be before this instant): ${y}.`);
  detail.push(`Job end for step-5 cap (step_5_completed_at ?? actual_end_time): ${str(w.jobEndForStep5Rule).slice(0, 19) || '—'}.`);
  detail.push(`positionBefore from derived-steps options: ${str(w.positionBeforeFromOptions).slice(0, 19) || '—'}.`);
  detail.push(`Step5ExtendWineryExit (minutes): ${str(w.step5ExtendWineryExitMinutes)}.`);
  detail.push(`jobEnd + extend (input to min): ${str(w.jobEndPlusExtend).slice(0, 19) || '—'}.`);
  detail.push(`upperExclusiveSource: ${str(w.upperExclusiveSource).replace(/_/g, ' ') || '—'}.`);
  detail.push(`step5ExitQueryRan: ${w.step5ExitQueryRan === true ? 'yes' : 'no'}.`);
  if (dec) {
    detail.push('— Part 2 (decideFinalSteps) —');
    detail.push(str(dec.summaryLine));
    detail.push(`Fetch candidate after guardrails: ${str(dec.fetchCandidateTime).slice(0, 19) || '—'} (tracking id ${str(dec.fetchCandidateTrackingId) || '—'}).`);
    detail.push(`VWork job end: ${str(dec.vworkStep5).slice(0, 19) || '—'}.`);
    detail.push(`Accept-after-job-end exclusive upper (job end + extend): ${str(dec.acceptAfterJobEndExclusiveUpper).slice(0, 19) || '—'}.`);
    detail.push(`step5GpsAccepted: ${dec.step5GpsAccepted === true ? 'yes' : 'no'} · outcome: ${str(dec.outcome).replace(/_/g, ' ') || '—'}.`);
  }
  out.push({ id: 'step5-gps-window', headline, detail });
}

function finishFinal(out: InspectExplanationItem[], api: Record<string, unknown>): InspectExplanationItem[] {
  pushStep1MorningExitAudit(out, api);
  pushStep5GpsWindowAudit(out, api);
  const lines: string[] = [];
  for (let n = 1; n <= 5; n++) {
    const g = api[`step${n}Gps` as keyof typeof api];
    const v = api[`step${n}Via` as keyof typeof api];
    const a = api[`step${n}` as keyof typeof api];
    const src = has(g) ? `GPS ${str(g).slice(0, 19)}` : has(a) ? `final ${str(a).slice(0, 19)}` : '—';
    lines.push(`Step ${n}: ${src} · via ${viaLabel(v)}`);
  }
  out.push({
    id: 'final',
    headline: 'Final · steps 1–5 (after buffer pass)',
    detail: lines,
  });
  return out;
}
