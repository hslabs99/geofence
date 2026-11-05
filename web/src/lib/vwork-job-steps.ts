import { execute, query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import { completeVworkJobStep, vworkCompletedAt, type VworkAccountId } from '@/lib/vwork-api';

const STEP_COLUMN = {
  1: 'vwork_step_1_name',
  2: 'vwork_step_2_name',
  3: 'vwork_step_3_name',
  4: 'vwork_step_4_name',
  5: 'vwork_step_5_name',
} as const;

/** Fills the VWork job id, account, and step names on queue rows that came from the feed. */
export async function linkQueuedJobsToVwork(): Promise<void> {
  await ensureTblDriverJobsSchema();
  await ensureVworkJobFeedSchema();
  await execute(
    `UPDATE tbl_driver_jobs j
     SET vwork_account = f.account,
         vwork_job_id = f.job_id,
         vwork_step_1_name = f.step_1_name,
         vwork_step_2_name = f.step_2_name,
         vwork_step_3_name = f.step_3_name,
         vwork_step_4_name = f.step_4_name,
         vwork_step_5_name = f.step_5_name
     FROM (
       SELECT DISTINCT ON (job_id)
         job_id, account, step_1_name, step_2_name, step_3_name, step_4_name, step_5_name
       FROM vwork_job_feed
       ORDER BY job_id, id DESC
     ) f
     WHERE j.job_id = f.job_id
       AND j.vwork_job_id IS NULL`,
  );
}

/**
 * Writes the passed gate back to the VWork job.
 * Jobs with no VWork hook are left alone. A VWork failure does not undo the gate.
 */
export async function pushDriverGateToVwork(
  driverJobId: number,
  step: number,
  at: string,
): Promise<{ sent: boolean; error?: string }> {
  const column = STEP_COLUMN[step as keyof typeof STEP_COLUMN];
  if (!column) return { sent: false };
  await linkQueuedJobsToVwork();
  const rows = await query<{ vwork_account: string | null; vwork_job_id: string | null; step_name: string | null }>(
    `SELECT vwork_account, vwork_job_id, ${column} AS step_name
     FROM tbl_driver_jobs
     WHERE id = $1`,
    [driverJobId],
  );
  const row = rows[0];
  const account = row?.vwork_account?.trim() as VworkAccountId | undefined;
  const vworkJobId = row?.vwork_job_id?.trim() ?? '';
  const stepName = row?.step_name?.trim() ?? '';
  if (!account || !vworkJobId || !stepName) return { sent: false };
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
