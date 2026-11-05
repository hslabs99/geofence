import { execute, query } from '@/lib/db';
import { ensureDriverUserSchema } from '@/lib/ensure-driver-user';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import { linkQueuedJobsToVwork } from '@/lib/vwork-job-steps';
import { applyVworkDriverResolution, refreshDriverImeisFromTracksolid } from '@/lib/driver-imei-sync';
import { applyVworkFenceResolution } from '@/lib/vwork-fence-resolve';

const CLOSED_STATUS = new Set(['completed', 'complete', 'cancelled', 'canceled', 'archived', 'deleted', 'invoiced']);
const BATCH = 200;

type FeedJob = {
  account: string | null;
  job_id: string;
  worker: string | null;
  driver_name: string | null;
  status: string | null;
  planned_start_time: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  truck_id: string | null;
  truck_rego: string | null;
  trailer_rego: string | null;
  step_1_name: string | null;
  step_2_name: string | null;
  step_3_name: string | null;
  step_4_name: string | null;
  step_5_name: string | null;
};

type DeviceHit = { name: string; imei: string | null; key: string; compact: string };

export type VworkFeedQueueResult = {
  considered: number;
  queued: number;
  skippedClosed: number;
  remaining: number;
};

/** Clock digits from an API datetime, with any offset left off. Not converted. */
export function verbatimClock(raw: string | null | undefined): string | null {
  if (raw == null) return null;
  const match = raw.trim().match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?/);
  if (!match) return null;
  return `${match[1]} ${match[2]}:${match[3] ?? '00'}`;
}

function closedStatus(status: string | null): boolean {
  return CLOSED_STATUS.has((status ?? '').trim().toLowerCase());
}

function compactKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, '');
}

type DriverImeiHit = { userid: string; label: string; imei: string | null };

/** One driver with an IMEI, or nothing when the assigned name matches none or several. */
export function resolveAssignedDriverImei(
  driverName: string | null,
  worker: string | null,
  byKey: Map<string, DriverImeiHit | null>,
): { driver: string; imei: string } | null {
  const keys = [driverName, worker]
    .map((value) => (value ?? '').trim().toLowerCase())
    .filter((value, index, all) => value !== '' && all.indexOf(value) === index);
  const hits = new Map<string, DriverImeiHit>();
  for (const key of keys) {
    const hit = byKey.get(key);
    if (hit) hits.set(hit.userid, hit);
  }
  if (hits.size !== 1) return null;
  const hit = [...hits.values()][0];
  if (!hit.imei) return null;
  return { driver: hit.label, imei: hit.imei };
}

async function driverImeiIndex(): Promise<Map<string, DriverImeiHit | null>> {
  await ensureDriverUserSchema();
  const rows = await query<{ userid: string; email: string | null; firstname: string | null; imei: string | null }>(
    `SELECT userid::text AS userid, email, firstname, NULLIF(btrim(imei), '') AS imei
     FROM tbl_users
     WHERE LOWER(btrim(COALESCE(user_type, ''))) = 'driver'`,
  );
  const byKey = new Map<string, DriverImeiHit | null>();
  for (const row of rows) {
    const label = (row.firstname ?? '').trim() || (row.email ?? '').trim();
    if (!label) continue;
    const hit: DriverImeiHit = { userid: row.userid, label, imei: row.imei?.trim() || null };
    for (const key of [(row.email ?? '').trim().toLowerCase(), (row.firstname ?? '').trim().toLowerCase()]) {
      if (!key) continue;
      const previous = byKey.get(key);
      if (previous === undefined) byKey.set(key, hit);
      else if (previous && previous.userid !== hit.userid) byKey.set(key, null);
    }
  }
  return byKey;
}

function deviceFor(rego: string | null, truckId: string | null, devices: DeviceHit[]): { truckId: string | null; imei: string | null } {
  const raws = [rego, truckId].map((value) => (value ?? '').trim()).filter(Boolean);
  for (const raw of raws) {
    const key = raw.toLowerCase();
    const compact = compactKey(raw);
    const hit = devices.find((device) => device.key === key || (compact !== '' && device.compact === compact));
    if (hit) return { truckId: hit.name, imei: hit.imei };
  }
  return { truckId: raws[0] ?? null, imei: null };
}

/** VWork name is stored as typed. The fence is chosen afterwards, only when it maps to one geo fence. */
function fenceFor(name: string | null): { label: string | null; fence: null } {
  const label = name?.trim() || null;
  return { label, fence: null };
}

async function settleImportedJobs(): Promise<void> {
  try {
    await refreshDriverImeisFromTracksolid(false);
  } catch (err) {
    console.error('[vwork-import] TrackSolid driver IMEI refresh failed', err);
  }
  await applyVworkFenceResolution();
  await applyVworkDriverResolution();
}

/** Copies open VWork feed jobs that are not already in the driver job queue. */
export async function enqueueVworkFeedJobs(): Promise<VworkFeedQueueResult> {
  await ensureVworkJobFeedSchema();
  await ensureTblDriverJobsSchema();
  await ensureTblDevicesSchema();
  await ensureDriverUserSchema();

  await linkQueuedJobsToVwork();
  const pending = await query<FeedJob>(
    `SELECT f.account, f.job_id, f.worker, f.driver_name, f.status, f.planned_start_time,
            f.vineyard_name, f.delivery_winery, f.truck_id, f.truck_rego, f.trailer_rego,
            f.step_1_name, f.step_2_name, f.step_3_name, f.step_4_name, f.step_5_name
     FROM vwork_job_feed f
     WHERE NOT EXISTS (
       SELECT 1 FROM tbl_driver_jobs j WHERE j.job_id = f.job_id
     )
     ORDER BY f.id
     LIMIT $1`,
    [BATCH + 1],
  );
  const remaining = pending.length > BATCH ? pending.length - BATCH : 0;
  const batch = pending.slice(0, BATCH);
  const open = batch.filter((job) => !closedStatus(job.status));
  const skippedClosed = batch.length - open.length;
  if (open.length === 0) {
    await settleImportedJobs();
    return { considered: batch.length, queued: 0, skippedClosed, remaining };
  }

  const [devices, drivers] = await Promise.all([
    query<{ device_name: string | null; imei: string | null }>(
      `SELECT btrim(device_name) AS device_name, NULLIF(btrim(imei), '') AS imei
       FROM tbl_devices
       WHERE device_name IS NOT NULL AND btrim(device_name) <> ''`,
    ),
    driverImeiIndex(),
  ]);

  const deviceHits: DeviceHit[] = devices
    .map((row) => {
      const name = row.device_name?.trim() ?? '';
      return { name, imei: row.imei?.trim() || null, key: name.toLowerCase(), compact: compactKey(name) };
    })
    .filter((row) => row.name !== '');

  let queued = 0;
  for (const job of open) {
    const truck = deviceFor(job.truck_rego, job.truck_id, deviceHits);
    const assigned = resolveAssignedDriverImei(job.driver_name, job.worker, drivers);
    const driverDevice = assigned
      ? deviceHits.find((device) => device.imei === assigned.imei && device.key === assigned.driver.toLowerCase()) ??
        deviceHits.find((device) => device.imei === assigned.imei)
      : undefined;
    const driver = assigned?.driver ?? ((job.driver_name ?? job.worker ?? '').trim() || null);
    const imei = assigned?.imei ?? null;
    const truckId = driverDevice?.name ?? truck.truckId;
    const winery = fenceFor(job.delivery_winery);
    const vineyard = fenceFor(job.vineyard_name);
    const planned = verbatimClock(job.planned_start_time) ?? '';
    const inserted = await execute(
      `INSERT INTO tbl_driver_jobs (
         job_id, truck_id, imei, driver, planned_start_time, winery_name, vineyard_name,
         winery_geo_fence, vineyard_geo_fence, truck_rego, trailer_rego, vwork_account, vwork_job_id,
         vwork_step_1_name, vwork_step_2_name, vwork_step_3_name, vwork_step_4_name, vwork_step_5_name
       )
       SELECT $1, $2, $3, $4, CAST(NULLIF($5, '') AS timestamp without time zone), $6, $7, $8, $9,
              $10, $11, $12, $13, $14, $15, $16, $17, $18
       WHERE NOT EXISTS (SELECT 1 FROM tbl_driver_jobs WHERE job_id = $1)`,
      [
        job.job_id,
        truckId,
        imei,
        driver,
        planned,
        winery.label,
        vineyard.label,
        winery.fence,
        vineyard.fence,
        job.truck_rego?.trim() || null,
        job.trailer_rego?.trim() || null,
        job.account,
        job.job_id,
        job.step_1_name,
        job.step_2_name,
        job.step_3_name,
        job.step_4_name,
        job.step_5_name,
      ],
    );
    queued += inserted;
  }

  await settleImportedJobs();
  return { considered: batch.length, queued, skippedClosed, remaining };
}
