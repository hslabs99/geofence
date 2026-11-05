import { execute, query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { ensureVworkJobFeedSchema } from '@/lib/ensure-vwork-job-feed-schema';
import type { DriverJob } from '@/lib/driver-jobs';

export type TruckPlates = {
  truckRego: string;
  trailerRego: string;
};

/** Drop feed placeholders that are not a registration. */
export function cleanPlate(value: unknown): string {
  const text = typeof value === 'string' ? value.trim() : '';
  const lower = text.toLowerCase();
  if (!text) return '';
  if (
    lower === 'no trailer required' ||
    lower === 'n/a' ||
    lower === 'na' ||
    lower === 'none' ||
    lower === 'nil' ||
    lower === '-'
  ) {
    return '';
  }
  return text;
}

/** Last plates a driver confirmed for this truck. Null when this truck has never been confirmed. */
export async function platesForTruck(truckId: string | null | undefined): Promise<TruckPlates | null> {
  const id = truckId?.trim() ?? '';
  if (!id) return null;
  await ensureTblDriverJobsSchema();
  const rows = await query<{ truck_rego: string | null; trailer_rego: string | null }>(
    `SELECT truck_rego, trailer_rego
     FROM tbl_truck_plates
     WHERE lower(btrim(truck_id)) = lower(btrim($1))
     LIMIT 1`,
    [id],
  );
  const row = rows[0];
  if (!row) return null;
  return { truckRego: cleanPlate(row.truck_rego), trailerRego: cleanPlate(row.trailer_rego) };
}

/** Plates to show before step 0. A remembered truck wins over the registration that arrived with this job. */
export async function platesToConfirm(job: DriverJob): Promise<TruckPlates> {
  const remembered = await platesForTruck(job.truck_id);
  if (remembered) return remembered;
  const onJob = { truckRego: cleanPlate(job.truck_rego), trailerRego: cleanPlate(job.trailer_rego) };
  if (onJob.truckRego || onJob.trailerRego) return onJob;
  await ensureVworkJobFeedSchema();
  const feed = await query<{ truck_rego: string | null; trailer_rego: string | null }>(
    `SELECT truck_rego, trailer_rego
     FROM vwork_job_feed
     WHERE job_id = $1
     ORDER BY id DESC
     LIMIT 1`,
    [job.job_id],
  );
  const row = feed[0];
  return { truckRego: cleanPlate(row?.truck_rego), trailerRego: cleanPlate(row?.trailer_rego) };
}

/** Remember the plates against the truck so the next job only asks the driver to confirm them. */
export async function rememberTruckPlates(truckId: string | null | undefined, plates: TruckPlates): Promise<void> {
  const id = truckId?.trim() ?? '';
  if (!id) return;
  await ensureTblDriverJobsSchema();
  await execute(`DELETE FROM tbl_truck_plates WHERE lower(btrim(truck_id)) = lower(btrim($1))`, [id]);
  await execute(
    `INSERT INTO tbl_truck_plates (truck_id, truck_rego, trailer_rego) VALUES ($1, $2, $3)`,
    [id, plates.truckRego, plates.trailerRego || null],
  );
}
