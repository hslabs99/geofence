import { execute, query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import { completeVworkJobStep, normalizeVworkAccount, vworkCompletedAt } from '@/lib/vwork-api';

const STEP_COLUMNS = [
  'vwork_step_1_name',
  'vwork_step_2_name',
  'vwork_step_3_name',
  'vwork_step_4_name',
  'vwork_step_5_name',
] as const;

/** Driver gate, then the VWork step names that gate should complete. */
const GATE_STEP_NAMES: Record<number, string[]> = {
  1: ['start job'],
  2: ['arrive vineyard'],
  3: ['depart vineyard'],
  4: ['arrive winery'],
  5: ['end job', 'job completed', 'complete job', 'job complete'],
};

function normStepName(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

/**
 * Picks the VWork step name for a driver gate.
 * A four-step template keeps "Job Completed" in slot 4. That belongs to End job,
 * and Arrive winery is left alone because that template has no winery step.
 */
export function vworkStepNameForGate(step: number, names: Array<string | null | undefined>): string {
  const wanted = GATE_STEP_NAMES[step];
  if (!wanted) return '';
  const cleaned = names.map((name) => name?.trim() ?? '');
  for (const alias of wanted) {
    const found = cleaned.find((name) => name !== '' && normStepName(name) === alias);
    if (found) return found;
  }
  const slot = cleaned[step - 1] ?? '';
  if (!slot) return '';
  const slotKey = normStepName(slot);
  const claimedByOther = Object.entries(GATE_STEP_NAMES).some(
    ([other, aliases]) => Number(other) !== step && aliases.includes(slotKey),
  );
  return claimedByOther ? '' : slot;
}

/** Fills the VWork job id, account, and step names on queue rows that came from the feed. */
export async function linkQueuedJobsToVwork(): Promise<void> {
  await ensureTblDriverJobsSchema();
  await ensureVworkJobFeedSchema();
  await execute(
    `UPDATE tbl_driver_jobs j
     SET vwork_account = f.account,
         company_name = NULLIF(btrim(f.customer), ''),
         group_name = NULLIF(btrim(f.group_name), ''),
         vwork_job_id = f.job_id,
         vwork_step_1_name = f.step_1_name,
         vwork_step_2_name = f.step_2_name,
         vwork_step_3_name = f.step_3_name,
         vwork_step_4_name = f.step_4_name,
         vwork_step_5_name = f.step_5_name
     FROM (
       SELECT DISTINCT ON (job_id)
         job_id, account, customer, group_name, step_1_name, step_2_name, step_3_name, step_4_name, step_5_name
       FROM vwork_job_feed
       ORDER BY job_id, id
     ) f
     WHERE j.job_id = f.job_id
       AND j.vwork_job_id IS NULL`,
  );
  await execute(
    `UPDATE tbl_driver_jobs j
     SET company_name = COALESCE(NULLIF(btrim(j.company_name), ''), NULLIF(btrim(f.customer), '')),
         group_name = COALESCE(NULLIF(btrim(j.group_name), ''), NULLIF(btrim(f.group_name), '')),
         vwork_account = COALESCE(NULLIF(btrim(j.vwork_account), ''), f.account)
     FROM (
       SELECT DISTINCT ON (job_id) job_id, account, customer, group_name
       FROM vwork_job_feed
       ORDER BY job_id, id
     ) f
     WHERE j.job_id = f.job_id
       AND (
         j.company_name IS NULL OR btrim(j.company_name) = ''
         OR j.group_name IS NULL OR btrim(j.group_name) = ''
         OR j.vwork_account IS NULL OR btrim(j.vwork_account) = ''
       )`,
  );
}

/**
 * Writes one queued gate to the VWork job.
 * The driver tap only enqueues. This runs later, from the gate-update service.
 * Jobs with no VWork hook are left alone. A VWork failure does not undo the gate.
 */
export async function pushDriverGateToVwork(
  driverJobId: number,
  step: number,
  at: string,
  queuedAccount?: string | null,
): Promise<{ sent: boolean; error?: string }> {
  if (!GATE_STEP_NAMES[step]) return { sent: false };
  await linkQueuedJobsToVwork();
  const rows = await query<
    { vwork_account: string | null; vwork_job_id: string | null } & Record<(typeof STEP_COLUMNS)[number], string | null>
  >(
    `SELECT vwork_account, vwork_job_id, ${STEP_COLUMNS.join(', ')}
     FROM tbl_driver_jobs
     WHERE id = $1`,
    [driverJobId],
  );
  const row = rows[0];
  const account = normalizeVworkAccount(queuedAccount) ?? normalizeVworkAccount(row?.vwork_account);
  const vworkJobId = row?.vwork_job_id?.trim() ?? '';
  const stepName = vworkStepNameForGate(
    step,
    STEP_COLUMNS.map((column) => row?.[column]),
  );
  if (!vworkJobId || !stepName) return { sent: false };
  if (!account) return { sent: false, error: 'This job has no VWork account, so the stage was not sent' };
  try {
    await completeVworkJobStep({
      account,
      vworkJobId,
      stepName,
      completedAt: vworkCompletedAt(at),
    });
    return { sent: true };
  } catch (err) {
    return { sent: false, error: err instanceof Error ? err.message : String(err) };
  }
}
