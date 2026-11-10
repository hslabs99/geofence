import { execute } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';

let ensured = false;

/** Queue of driver stage taps waiting to be written to the VWork job. */
export async function ensureVworkGateUpdateSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS vwork_gate_updates (
      id              bigserial PRIMARY KEY,
      driver_job_id   integer NOT NULL,
      job_id          varchar,
      step            integer NOT NULL,
      step_title      varchar NOT NULL,
      stage_at        text NOT NULL,
      received_at     text NOT NULL,
      status          varchar NOT NULL DEFAULT 'queued',
      executed_at     text,
      error           text,
      attempts        integer NOT NULL DEFAULT 0,
      claimed_at      text
    )`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_gate_updates_job_step
      ON vwork_gate_updates (driver_job_id, step)`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_vwork_gate_updates_status_id
      ON vwork_gate_updates (status, id)`);
  await execute(`ALTER TABLE vwork_gate_updates ADD COLUMN IF NOT EXISTS vwork_account varchar`);
  await ensureTblDriverJobsSchema();
  await execute(`
    UPDATE vwork_gate_updates g
    SET vwork_account = j.vwork_account
    FROM tbl_driver_jobs j
    WHERE g.driver_job_id = j.id
      AND (g.vwork_account IS NULL OR btrim(g.vwork_account) = '')
      AND j.vwork_account IS NOT NULL
      AND btrim(j.vwork_account) <> ''`);
  ensured = true;
}
