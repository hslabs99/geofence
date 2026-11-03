import { execute, query } from '@/lib/db';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import { expandedFenceNameList, mappingsByVworkName, type GpsMappingRow } from '@/lib/gps-mappings-by-vwork-name';

const CLOSED_STATUS = new Set(['completed', 'complete', 'cancelled', 'canceled', 'archived', 'deleted', 'invoiced']);
const BATCH = 200;

type FeedJob = {
  job_id: string;
  worker: string | null;
  driver_name: string | null;
  status: string | null;
  planned_start_time: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  truck_id: string | null;
  truck_rego: string | null;
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

function fenceFor(
  name: string | null,
  maps: GpsMappingRow[],
  fences: Map<string, string>,
): { label: string | null; fence: string | null } {
  const label = name?.trim() || null;
  if (!label) return { label: null, fence: null };
  for (const candidate of expandedFenceNameList(label, maps)) {
    const hit = fences.get(candidate.trim().toLowerCase());
    if (hit) return { label, fence: hit };
  }
  return { label, fence: null };
}

/** Copies open VWork feed jobs that are not already in the driver job queue. */
export async function enqueueVworkFeedJobs(): Promise<VworkFeedQueueResult> {
  await ensureVworkJobFeedSchema();
  await ensureTblDriverJobsSchema();
  await ensureTblDevicesSchema();

  const pending = await query<FeedJob>(
    `SELECT f.job_id, f.worker, f.driver_name, f.status, f.planned_start_time,
            f.vineyard_name, f.delivery_winery, f.truck_id, f.truck_rego
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
    return { considered: batch.length, queued: 0, skippedClosed, remaining };
  }

  const [devices, fenceRows, wineryMaps, vineyardMaps] = await Promise.all([
    query<{ device_name: string | null; imei: string | null }>(
      `SELECT btrim(device_name) AS device_name, NULLIF(btrim(imei), '') AS imei
       FROM tbl_devices
       WHERE device_name IS NOT NULL AND btrim(device_name) <> ''`,
    ),
    query<{ fence_name: string }>(
      `SELECT DISTINCT ON (lower(btrim(fence_name))) btrim(fence_name) AS fence_name
       FROM tbl_geofences
       WHERE fence_name IS NOT NULL AND btrim(fence_name) <> ''
       ORDER BY lower(btrim(fence_name)), btrim(fence_name)`,
    ),
    mappingsByVworkName(
      'Winery',
      open.map((job) => job.delivery_winery ?? ''),
    ),
    mappingsByVworkName(
      'Vineyard',
      open.map((job) => job.vineyard_name ?? ''),
    ),
  ]);

  const deviceHits: DeviceHit[] = devices
    .map((row) => {
      const name = row.device_name?.trim() ?? '';
      return { name, imei: row.imei?.trim() || null, key: name.toLowerCase(), compact: compactKey(name) };
    })
    .filter((row) => row.name !== '');
  const fences = new Map<string, string>();
  for (const row of fenceRows) {
    const name = row.fence_name.trim();
    if (name) fences.set(name.toLowerCase(), name);
  }

  let queued = 0;
  for (const job of open) {
    const device = deviceFor(job.truck_rego, job.truck_id, deviceHits);
    const driver = (job.driver_name ?? job.worker ?? '').trim() || null;
    const winery = fenceFor(job.delivery_winery, wineryMaps[job.delivery_winery?.trim() ?? ''] ?? [], fences);
    const vineyard = fenceFor(job.vineyard_name, vineyardMaps[job.vineyard_name?.trim() ?? ''] ?? [], fences);
    const planned = verbatimClock(job.planned_start_time) ?? '';
    const inserted = await execute(
      `INSERT INTO tbl_driver_jobs (
         job_id, truck_id, imei, driver, planned_start_time, winery_name, vineyard_name,
         winery_geo_fence, vineyard_geo_fence
       )
       SELECT $1, $2, $3, $4, CAST(NULLIF($5, '') AS timestamp without time zone), $6, $7, $8, $9
       WHERE NOT EXISTS (SELECT 1 FROM tbl_driver_jobs WHERE job_id = $1)`,
      [job.job_id, device.truckId, device.imei, driver, planned, winery.label, vineyard.label, winery.fence, vineyard.fence],
    );
    queued += inserted;
  }

  return { considered: batch.length, queued, skippedClosed, remaining };
}
