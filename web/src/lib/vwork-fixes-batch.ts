import { query } from '@/lib/db';
import {
  runWineMappFixes,
  runVineMappFixes,
  runUpdateVineyardGroup,
  runDriverMappFixes,
  runSetTrailerType,
  runSetTrailermodeFromLoadsize,
  runStep4to5NormalAllEligible,
} from '@/lib/run-vwork-data-fixes';

const LOGTYPE = 'AutoRun';
const LOGCAT1 = 'vwork-fixes-batch';

export type VworkFixBatchStepOut = {
  step: string;
  logcat2: string;
  ok: boolean;
  result?: Record<string, unknown>;
  error?: string;
  logid?: string;
  logdatetime?: string;
};

type StepDef = {
  logcat2: string;
  label: string;
  run: () => Promise<Record<string, unknown>>;
};

export const VWORK_FIX_BATCH_STEPS: StepDef[] = [
  { logcat2: 'winery-name-fixes', label: 'Winery name fixes', run: () => runWineMappFixes() as Promise<Record<string, unknown>> },
  { logcat2: 'vineyard-name-fixes', label: 'Vineyard name fixes', run: () => runVineMappFixes() as Promise<Record<string, unknown>> },
  {
    logcat2: 'vineyard-group-mapping',
    label: 'Vineyard mappings run',
    run: () => runUpdateVineyardGroup() as Promise<Record<string, unknown>>,
  },
  { logcat2: 'driver-name-fixes', label: 'Driver name fixes', run: () => runDriverMappFixes() as Promise<Record<string, unknown>> },
  { logcat2: 'set-trailer-type', label: 'Set trailer type', run: () => runSetTrailerType() as Promise<Record<string, unknown>> },
  {
    logcat2: 'set-trailermode-from-loadsize',
    label: 'Trailermode from load size',
    run: () => runSetTrailermodeFromLoadsize() as Promise<Record<string, unknown>>,
  },
  {
    logcat2: 'step4to5-normal-all',
    label: 'Step4→5 fix (all eligible jobs)',
    run: () => runStep4to5NormalAllEligible() as Promise<Record<string, unknown>>,
  },
];

export const VWORK_FIX_BATCH_STEP_COUNT = VWORK_FIX_BATCH_STEPS.length;

async function insertStepLog(
  logcat2: string,
  payload: Record<string, unknown>
): Promise<{ logid: string; logdatetime: string }> {
  const rows = await query<{ logid: string; logdatetime: string }>(
    `INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4) RETURNING logid, logdatetime`,
    [LOGTYPE, LOGCAT1, logcat2, JSON.stringify(payload)]
  );
  const row = rows[0];
  if (!row) throw new Error('tbl_logs insert returned no row');
  return { logid: String(row.logid), logdatetime: String(row.logdatetime) };
}

/**
 * Run one batch step by index (0 .. VWORK_FIX_BATCH_STEP_COUNT - 1). Writes tbl_logs like the full batch.
 */
export async function runVworkFixBatchStep(stepIndex: number): Promise<VworkFixBatchStepOut> {
  if (!Number.isInteger(stepIndex) || stepIndex < 0 || stepIndex >= VWORK_FIX_BATCH_STEPS.length) {
    throw new Error(`Invalid step index: ${stepIndex}`);
  }
  const def = VWORK_FIX_BATCH_STEPS[stepIndex]!;
  try {
    const result = await def.run();
    const { logid, logdatetime } = await insertStepLog(def.logcat2, { ok: true, ...result });
    return {
      step: def.label,
      logcat2: def.logcat2,
      ok: true,
      result,
      logid,
      logdatetime,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    let logid: string | undefined;
    let logdatetime: string | undefined;
    try {
      const row = await insertStepLog(def.logcat2, { ok: false, error: msg });
      logid = row.logid;
      logdatetime = row.logdatetime;
    } catch (logErr) {
      console.error('[vwork-fixes-batch] Failed to log step failure:', logErr);
    }
    return {
      step: def.label,
      logcat2: def.logcat2,
      ok: false,
      error: msg,
      logid,
      logdatetime,
    };
  }
}

export async function runAllVworkFixesBatch(): Promise<{ ok: boolean; steps: VworkFixBatchStepOut[] }> {
  const steps: VworkFixBatchStepOut[] = [];
  for (let i = 0; i < VWORK_FIX_BATCH_STEPS.length; i++) {
    steps.push(await runVworkFixBatchStep(i));
  }
  return { ok: steps.every((s) => s.ok), steps };
}
