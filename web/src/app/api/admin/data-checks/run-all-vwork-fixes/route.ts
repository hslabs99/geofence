import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import {
  runWineMappFixes,
  runVineMappFixes,
  runUpdateVineyardGroup,
  runDriverMappFixes,
  runSetTrailerType,
} from '@/lib/run-vwork-data-fixes';

const LOGTYPE = 'AutoRun';
const LOGCAT1 = 'vwork-fixes-batch';

type StepDef = {
  logcat2: string;
  label: string;
  run: () => Promise<Record<string, unknown>>;
};

const STEPS: StepDef[] = [
  { logcat2: 'winery-name-fixes', label: 'Winery name fixes', run: () => runWineMappFixes() as Promise<Record<string, unknown>> },
  { logcat2: 'vineyard-name-fixes', label: 'Vineyard name fixes', run: () => runVineMappFixes() as Promise<Record<string, unknown>> },
  {
    logcat2: 'vineyard-group-mapping',
    label: 'Vineyard mappings run',
    run: () => runUpdateVineyardGroup() as Promise<Record<string, unknown>>,
  },
  { logcat2: 'driver-name-fixes', label: 'Driver name fixes', run: () => runDriverMappFixes() as Promise<Record<string, unknown>> },
  { logcat2: 'set-trailer-type', label: 'Set trailer type', run: () => runSetTrailerType() as Promise<Record<string, unknown>> },
];

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
 * POST: Run all five tbl_vworkjobs data-check fixes in order.
 * Writes one tbl_logs row per step (logtype=AutoRun, logcat1=vwork-fixes-batch, logcat2=step id).
 */
export async function POST() {
  const stepsOut: {
    step: string;
    logcat2: string;
    ok: boolean;
    result?: Record<string, unknown>;
    error?: string;
    logid?: string;
    logdatetime?: string;
  }[] = [];

  for (const def of STEPS) {
    try {
      const result = await def.run();
      const { logid, logdatetime } = await insertStepLog(def.logcat2, { ok: true, ...result });
      stepsOut.push({
        step: def.label,
        logcat2: def.logcat2,
        ok: true,
        result,
        logid,
        logdatetime,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      let logid: string | undefined;
      let logdatetime: string | undefined;
      try {
        const row = await insertStepLog(def.logcat2, { ok: false, error: msg });
        logid = row.logid;
        logdatetime = row.logdatetime;
      } catch (logErr) {
        console.error('[run-all-vwork-fixes] Failed to log step failure:', logErr);
      }
      stepsOut.push({
        step: def.label,
        logcat2: def.logcat2,
        ok: false,
        error: msg,
        logid,
        logdatetime,
      });
    }
  }

  const allOk = stepsOut.every((s) => s.ok);
  return NextResponse.json({
    ok: allOk,
    steps: stepsOut,
  });
}
