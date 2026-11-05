import { execute, query } from '@/lib/db';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import {
  isoLocal,
  listVworkJobsUpdatedSince,
  readyVworkAccounts,
  vworkAccountStatus,
  vworkApiConfigured,
  vworkApiKeyState,
  type VworkAccountId,
  type VworkAccountStatus,
  type VworkFeedRow,
} from '@/lib/vwork-api';

const INSERT_COLUMNS = [
  'job_id',
  'customer',
  'template',
  'worker',
  'status',
  'third_party_id',
  'planned_start_time',
  'planned_duration',
  'actual_start_time',
  'worker_duration_mins',
  'proof_of_delivery',
  'number_of_steps',
  'pod_signed_by',
  'pod_signed_at',
  'asset_id',
  'asset_name',
  'customer_third_party_id',
  'groups',
  'create_date',
  'source_updated_at',
  'step_1_name',
  'step_1_completed_at',
  'step_1_address',
  'step_2_name',
  'step_2_completed_at',
  'step_2_address',
  'step_3_name',
  'step_3_completed_at',
  'step_3_address',
  'step_4_name',
  'step_4_completed_at',
  'step_4_address',
  'step_5_name',
  'step_5_completed_at',
  'step_5_address',
  'truck_id',
  'truck_rego',
  'trailer_rego',
  'driver_name',
  'vineyard_name',
  'pickup_location_map',
  'field_docket_number',
  'delivery_winery',
  'delivery_location_map',
  'weighbridge_weight',
  'driver_notes',
  'comments',
  'field_notes',
  'contains_mog',
  'number_of_loads',
  'loadsize',
  'custom_fields',
  'fetched_at',
] as const;

export type VworkFeedAccountResult = {
  id: VworkAccountId;
  label: string;
  fetched: number;
  pages: number;
  truncated: boolean;
  inserted: number;
  alreadyInVworkJobs: number;
  alreadyInFeed: number;
  error: string | null;
};

export type VworkFeedListRow = {
  account: string | null;
  job_id: string;
  customer: string | null;
  template: string | null;
  worker: string | null;
  status: string | null;
  planned_start_time: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  truck_id: string | null;
  truck_rego: string | null;
  fetched_at: string | null;
};

function localClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

async function writeLog(details: Record<string, unknown>): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      ['vwork_job_feed', 'all', 'check', JSON.stringify(details)],
    );
  } catch (err) {
    console.error('[vwork-job-feed] log failed', err);
  }
}

async function existingJobIds(ids: string[]): Promise<Set<string>> {
  if (ids.length === 0) return new Set();
  const rows = await query<{ job_id: string }>(
    `SELECT job_id::text AS job_id FROM tbl_vworkjobs WHERE job_id::text = ANY($1::text[])`,
    [ids],
  );
  return new Set(rows.map((row) => row.job_id));
}

async function existingFeedIds(account: VworkAccountId, ids: string[]): Promise<Set<string>> {
  if (ids.length === 0) return new Set();
  const rows = await query<{ job_id: string }>(
    `SELECT job_id::text AS job_id FROM vwork_job_feed WHERE account = $1 AND job_id::text = ANY($2::text[])`,
    [account, ids],
  );
  return new Set(rows.map((row) => row.job_id));
}

async function insertFeedRow(row: VworkFeedRow, fetchedAt: string, account: VworkAccountId): Promise<void> {
  const columns = ['account', ...INSERT_COLUMNS];
  const values = columns.map((column) => {
    if (column === 'account') return account;
    if (column === 'fetched_at') return fetchedAt;
    return row[column as keyof VworkFeedRow];
  });
  const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
  await execute(
    `INSERT INTO vwork_job_feed (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT (account, job_id) DO NOTHING`,
    values,
  );
}

export async function checkVworkJobFeed(lookbackDays: number): Promise<{
  configured: boolean;
  lookbackDays: number;
  updatedSince: string;
  fetched: number;
  pages: number;
  truncated: boolean;
  alreadyInVworkJobs: number;
  alreadyInFeed: number;
  inserted: number;
  checkedAt: string;
  accounts: VworkFeedAccountResult[];
}> {
  await ensureVworkJobFeedSchema();
  const days = Math.min(90, Math.max(1, Math.round(lookbackDays)));
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const updatedSince = isoLocal(since);
  const fetchedAt = localClock();
  const accounts: VworkFeedAccountResult[] = [];
  for (const account of readyVworkAccounts()) {
    try {
      const listed = await listVworkJobsUpdatedSince(updatedSince, account.apiKey);
      const ids = [...new Set(listed.jobs.map((job) => job.job_id))];
      const [inJobs, inFeed] = await Promise.all([existingJobIds(ids), existingFeedIds(account.id, ids)]);
      let inserted = 0;
      for (const job of listed.jobs) {
        if (inJobs.has(job.job_id) || inFeed.has(job.job_id)) continue;
        await insertFeedRow(job, fetchedAt, account.id);
        inFeed.add(job.job_id);
        inserted += 1;
      }
      accounts.push({
        id: account.id,
        label: account.label,
        fetched: listed.jobs.length,
        pages: listed.pages,
        truncated: listed.truncated,
        inserted,
        alreadyInVworkJobs: listed.jobs.filter((job) => inJobs.has(job.job_id)).length,
        alreadyInFeed: listed.jobs.filter((job) => inFeed.has(job.job_id)).length - inserted,
        error: null,
      });
    } catch (err) {
      accounts.push({
        id: account.id,
        label: account.label,
        fetched: 0,
        pages: 0,
        truncated: false,
        inserted: 0,
        alreadyInVworkJobs: 0,
        alreadyInFeed: 0,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
  const result = {
    configured: accounts.length > 0,
    lookbackDays: days,
    updatedSince,
    fetched: accounts.reduce((sum, account) => sum + account.fetched, 0),
    pages: accounts.reduce((sum, account) => sum + account.pages, 0),
    truncated: accounts.some((account) => account.truncated),
    alreadyInVworkJobs: accounts.reduce((sum, account) => sum + account.alreadyInVworkJobs, 0),
    alreadyInFeed: accounts.reduce((sum, account) => sum + account.alreadyInFeed, 0),
    inserted: accounts.reduce((sum, account) => sum + account.inserted, 0),
    checkedAt: fetchedAt,
    accounts,
  };
  await writeLog(result);
  return result;
}

export async function insertVworkFeedTestRows(): Promise<{ inserted: number; jobIds: string[] }> {
  await ensureVworkJobFeedSchema();
  const fetchedAt = localClock();
  const day = fetchedAt.slice(0, 10);
  const stamp = fetchedAt.replace(/\D/g, '');
  const jobIds: string[] = [];
  for (let i = 1; i <= 5; i++) {
    const jobId = `TEST${stamp}${i}`;
    const hour = String(7 + i).padStart(2, '0');
    await execute(
      `INSERT INTO vwork_job_feed (
         account, job_id, customer, template, worker, driver_name, status, truck_rego,
         planned_start_time, vineyard_name, delivery_winery, fetched_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      ['delegat', jobId, 'Test', 'Cartage', 'Mike', 'Mike', 'assigned', 'ABC123', `${day} ${hour}:00:00`, 'Test vineyard', 'Test winery', fetchedAt],
    );
    jobIds.push(jobId);
  }
  await writeLog({ at: fetchedAt, inserted: jobIds.length, test: true, worker: 'Mike', truck_rego: 'ABC123' });
  return { inserted: jobIds.length, jobIds };
}

export async function listVworkJobFeed(limit = 80): Promise<{
  configured: boolean;
  keyState: 'missing' | 'placeholder' | 'ready';
  accounts: VworkAccountStatus[];
  total: number;
  rows: VworkFeedListRow[];
}> {
  await ensureVworkJobFeedSchema();
  const cap = Math.min(200, Math.max(1, limit));
  const [countRows, rows] = await Promise.all([
    query<{ n: number }>('SELECT count(*)::int AS n FROM vwork_job_feed'),
    query<VworkFeedListRow>(
      `SELECT account, job_id, customer, template, worker, status, planned_start_time, vineyard_name, delivery_winery, truck_id, truck_rego, fetched_at
       FROM vwork_job_feed
       ORDER BY id DESC
       LIMIT $1`,
      [cap],
    ),
  ]);
  return {
    configured: vworkApiConfigured(),
    keyState: vworkApiKeyState(),
    accounts: vworkAccountStatus(),
    total: countRows[0]?.n ?? 0,
    rows,
  };
}
