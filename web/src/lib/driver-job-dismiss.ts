import { execute } from '@/lib/db';
import type { PoolClient } from 'pg';

let ensured = false;

/** Jobs removed from the queue. The VWork feed must not copy these job ids back in. */
export async function ensureDriverJobDismissedSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_job_dismissed (
      job_id text PRIMARY KEY,
      dismissed_at timestamp without time zone NOT NULL DEFAULT clock_timestamp()
    )`);
  ensured = true;
}

/** Remember a deleted job id only when that id is already in the VWork feed. A hand-typed queue job is not. */
export async function dismissDriverJob(client: PoolClient, jobId: string): Promise<void> {
  const key = jobId.trim();
  if (!key) return;
  await client.query(
    `INSERT INTO tbl_driver_job_dismissed (job_id)
     SELECT $1
     WHERE EXISTS (
       SELECT 1 FROM vwork_job_feed f WHERE btrim(f.job_id) = $1
     )
     ON CONFLICT (job_id) DO UPDATE SET dismissed_at = clock_timestamp()`,
    [key],
  );
}

export async function clearDismissedDriverJob(client: PoolClient, jobId: string): Promise<void> {
  const key = jobId.trim();
  if (!key) return;
  await client.query(`DELETE FROM tbl_driver_job_dismissed WHERE btrim(job_id) = $1`, [key]);
}

/** Feed row is still waiting to be copied: not already queued, and not removed by hand. */
export function feedJobNotQueued(jobIdExpr: string): string {
  return `NOT EXISTS (
    SELECT 1 FROM tbl_driver_jobs queued
    WHERE btrim(queued.job_id) = btrim(${jobIdExpr})
  )
  AND NOT EXISTS (
    SELECT 1 FROM tbl_driver_job_dismissed dismissed
    WHERE btrim(dismissed.job_id) = btrim(${jobIdExpr})
  )`;
}
