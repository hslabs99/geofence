import { execute } from '@/lib/db';

let ensured = false;

/** VWork import fence choices and the open issues waiting on the job controller. */
export async function ensureVworkFenceIssueSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS vwork_import_fence_map (
      id           serial PRIMARY KEY,
      kind         text NOT NULL,
      vwork_name   text NOT NULL,
      fence_name   text NOT NULL,
      created_at   text NOT NULL
    )`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_import_fence_map
      ON vwork_import_fence_map (kind, lower(btrim(vwork_name)))`);
  await execute(`
    CREATE TABLE IF NOT EXISTS vwork_job_queue_issues (
      id                serial PRIMARY KEY,
      kind              text NOT NULL,
      vwork_name        text NOT NULL,
      candidate_fences  text,
      status            text NOT NULL DEFAULT 'open',
      resolved_fence    text,
      created_at        text NOT NULL,
      resolved_at       text
    )`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_queue_issue_open
      ON vwork_job_queue_issues (kind, lower(btrim(vwork_name)))
      WHERE status = 'open'`);
  ensured = true;
}
