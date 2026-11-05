import { execute, query } from '@/lib/db';
import { ensureDriverUserSchema } from '@/lib/ensure-driver-user';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';
import { ensureVworkFenceIssueSchema } from '@/lib/ensure-vwork-fence-issue-schema';
import { getAccessToken, listDevices } from '@/lib/tracksolid';

const TRACKSOLID_KEYS = ['global', 'hk', 'eu', 'us'] as const;

export type DriverLookupRow = {
  userid: string;
  email: string;
  firstname: string | null;
  surname: string | null;
  phone: string | null;
  userType: string;
  customer: string | null;
  truckRego: string | null;
  imei: string | null;
};

export type FillDriverImeiResult = {
  filled: { driverId: string; imei: string }[];
  stillMissing: string[];
  tracksolidDevices: number;
  jobsUpdated: number;
};

function tracksolidEndpoint(): string {
  const key = process.env.TRACKSOLID_ENDPOINT?.trim().toLowerCase() ?? '';
  if ((TRACKSOLID_KEYS as readonly string[]).includes(key)) return key;
  return 'hk';
}

/** Drivers only, with the TrackSolid IMEI stored against that driver ID. */
export async function listDriverLookups(): Promise<{ rows: DriverLookupRow[]; missing: number }> {
  await ensureDriverUserSchema();
  const rows = await query<{
    userid: string;
    email: string | null;
    firstname: string | null;
    surname: string | null;
    phone: string | null;
    user_type: string | null;
    customer: string | null;
    truck_rego: string | null;
    imei: string | null;
  }>(
    `SELECT userid::text AS userid, email, firstname, surname, phone, user_type, customer, truck_rego,
            NULLIF(btrim(imei), '') AS imei
     FROM tbl_users
     WHERE LOWER(btrim(COALESCE(user_type, ''))) = 'driver'
     ORDER BY LOWER(btrim(email)), userid`,
  );
  const mapped = rows.map((row) => ({
    userid: row.userid,
    email: (row.email ?? '').trim(),
    firstname: row.firstname?.trim() || null,
    surname: row.surname?.trim() || null,
    phone: row.phone?.trim() || null,
    userType: 'Driver',
    customer: row.customer?.trim() || null,
    truckRego: row.truck_rego?.trim() || null,
    imei: row.imei?.trim() || null,
  }));
  return { rows: mapped, missing: mapped.filter((row) => !row.imei).length };
}

let tracksolidRefreshedAt = 0;
const TRACKSOLID_REFRESH_MS = 2 * 60 * 1000;

function localClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

/**
 * Asks TrackSolid for the account device list and keeps each driver ID's IMEI
 * aligned with that list. A driver missing from the live list keeps the IMEI
 * already stored, or takes one from the last TrackSolid device harvest.
 */
export async function fillMissingDriverImeis(): Promise<FillDriverImeiResult> {
  const current = await listDriverLookups();
  const endpoint = tracksolidEndpoint();
  const token = await getAccessToken(endpoint, { skipCache: false });
  const listed = await listDevices(token.token, endpoint);
  const byName = new Map<string, string | null>();
  for (const device of listed.devices) {
    const name = (device.deviceName ?? '').trim().toLowerCase();
    const imei = (device.imei ?? '').trim();
    if (!name || !imei) continue;
    const previous = byName.get(name);
    if (previous === undefined) byName.set(name, imei);
    else if (previous !== imei) byName.set(name, null);
  }
  await ensureTblDevicesSchema();
  const stored = await query<{ device_name: string | null; imei: string | null }>(
    `SELECT btrim(device_name) AS device_name, NULLIF(btrim(imei), '') AS imei
     FROM tbl_devices
     WHERE device_name IS NOT NULL AND btrim(device_name) <> ''`,
  );
  for (const row of stored) {
    const name = (row.device_name ?? '').trim().toLowerCase();
    const imei = (row.imei ?? '').trim();
    if (!name || !imei || byName.has(name)) continue;
    byName.set(name, imei);
  }

  const liveNames = new Set<string>();
  for (const device of listed.devices) {
    const name = (device.deviceName ?? '').trim().toLowerCase();
    if (name) liveNames.add(name);
  }
  const filled: { driverId: string; imei: string }[] = [];
  const stillMissing: string[] = [];
  let jobsUpdated = 0;
  for (const driver of current.rows) {
    const driverId = driver.email || driver.firstname || driver.userid;
    const keys = [driver.email, driver.firstname ?? ''].map((value) => value.trim().toLowerCase()).filter(Boolean);
    const liveKey = keys.find((key) => liveNames.has(key) && typeof byName.get(key) === 'string' && byName.get(key) !== '');
    const storedKey = keys.find((key) => typeof byName.get(key) === 'string' && byName.get(key) !== '');
    const matchedKey = liveKey ?? (!driver.imei ? storedKey : undefined);
    const match = matchedKey ? byName.get(matchedKey) : null;
    if (!matchedKey || !match) {
      if (!driver.imei) stillMissing.push(driverId);
      continue;
    }
    if (driver.imei === match) continue;
    const updated = await execute(`UPDATE tbl_users SET imei = $2 WHERE userid::text = $1`, [driver.userid, match]);
    if (updated === 0) {
      if (!driver.imei) stillMissing.push(driverId);
      continue;
    }
    jobsUpdated += await execute(
      `UPDATE tbl_driver_jobs
       SET imei = $2
       WHERE lower(btrim(coalesce(driver, ''))) = $1
         AND (imei IS NULL OR btrim(imei) = '')`,
      [matchedKey, match],
    );
    filled.push({ driverId, imei: match });
  }
  return { filled, stillMissing, tracksolidDevices: listed.devices.length, jobsUpdated };
}

/** Refreshes driver IMEIs from TrackSolid, at most once every two minutes unless forced. */
export async function refreshDriverImeisFromTracksolid(force = false): Promise<FillDriverImeiResult | null> {
  const now = Date.now();
  if (!force && now - tracksolidRefreshedAt < TRACKSOLID_REFRESH_MS) return null;
  const result = await fillMissingDriverImeis();
  tracksolidRefreshedAt = Date.now();
  return result;
}

type DriverHit = { userid: string; label: string; imei: string | null };

async function driverHitIndex(): Promise<{ byKey: Map<string, DriverHit | null>; withImei: DriverHit[] }> {
  await ensureDriverUserSchema();
  const rows = await query<{ userid: string; email: string | null; firstname: string | null; imei: string | null }>(
    `SELECT userid::text AS userid, email, firstname, NULLIF(btrim(imei), '') AS imei
     FROM tbl_users
     WHERE LOWER(btrim(COALESCE(user_type, ''))) = 'driver'`,
  );
  const byKey = new Map<string, DriverHit | null>();
  const withImei: DriverHit[] = [];
  for (const row of rows) {
    const label = (row.firstname ?? '').trim() || (row.email ?? '').trim();
    if (!label) continue;
    const hit: DriverHit = { userid: row.userid, label, imei: row.imei?.trim() || null };
    if (hit.imei) withImei.push(hit);
    for (const key of [(row.email ?? '').trim().toLowerCase(), (row.firstname ?? '').trim().toLowerCase()]) {
      if (!key) continue;
      const previous = byKey.get(key);
      if (previous === undefined) byKey.set(key, hit);
      else if (previous && previous.userid !== hit.userid) byKey.set(key, null);
    }
  }
  withImei.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: 'base' }));
  return { byKey, withImei };
}

function driverForName(name: string, byKey: Map<string, DriverHit | null>, savedLabel: string | null): DriverHit | null {
  if (savedLabel) {
    const saved = byKey.get(savedLabel.trim().toLowerCase());
    if (saved?.imei) return saved;
    return null;
  }
  const hit = byKey.get(name.trim().toLowerCase());
  if (!hit?.imei) return null;
  return hit;
}

async function deviceNameFor(imei: string, driverLabel: string): Promise<string | null> {
  await ensureTblDevicesSchema();
  const rows = await query<{ device_name: string }>(
    `SELECT btrim(device_name) AS device_name
     FROM tbl_devices
     WHERE btrim(imei) = $1 AND device_name IS NOT NULL AND btrim(device_name) <> ''
     ORDER BY CASE WHEN lower(btrim(device_name)) = lower($2) THEN 0 ELSE 1 END, btrim(device_name)
     LIMIT 1`,
    [imei, driverLabel],
  );
  return rows[0]?.device_name?.trim() || null;
}

/** Fills a job IMEI from the driver table, or opens an exception when that driver name does not resolve. */
export async function applyVworkDriverResolution(): Promise<void> {
  await ensureVworkFenceIssueSchema();
  const { byKey } = await driverHitIndex();
  const savedRows = await query<{ vwork_name: string; fence_name: string }>(
    `SELECT vwork_name, fence_name FROM vwork_import_fence_map WHERE kind = 'Driver'`,
  );
  const saved = new Map<string, string>();
  for (const row of savedRows) saved.set(row.vwork_name.trim().toLowerCase(), row.fence_name);
  const jobs = await query<{ id: number; driver: string | null; imei: string | null }>(
    `SELECT id, driver, NULLIF(btrim(imei), '') AS imei
     FROM tbl_driver_jobs
     WHERE vwork_job_id IS NOT NULL AND driver IS NOT NULL AND btrim(driver) <> ''`,
  );
  const pending = new Map<string, { name: string; candidates: string[] }>();
  for (const job of jobs) {
    const name = (job.driver ?? '').trim();
    if (!name) continue;
    const key = name.toLowerCase();
    const resolved = driverForName(name, byKey, saved.get(key) ?? null);
    if (resolved?.imei) {
      if ((job.driver ?? '').trim() === resolved.label && (job.imei ?? '') === resolved.imei) continue;
      const truck = await deviceNameFor(resolved.imei, resolved.label);
      await execute(
        `UPDATE tbl_driver_jobs
         SET driver = $2, imei = $3, truck_id = COALESCE($4, truck_id)
         WHERE id = $1`,
        [job.id, resolved.label, resolved.imei, truck],
      );
      continue;
    }
    const exact = byKey.get(key);
    const candidates = exact?.imei ? [exact.label] : [];
    pending.set(key, { name, candidates });
  }
  const now = localClock();
  for (const item of pending.values()) {
    await execute(
      `INSERT INTO vwork_job_queue_issues (kind, vwork_name, candidate_fences, status, created_at)
       VALUES ('Driver', $1, $2, 'open', $3)
       ON CONFLICT (kind, lower(btrim(vwork_name))) WHERE status = 'open'
       DO UPDATE SET candidate_fences = EXCLUDED.candidate_fences`,
      [item.name, JSON.stringify(item.candidates), now],
    );
  }
  await execute(
    `UPDATE vwork_job_queue_issues i
     SET status = 'resolved', resolved_at = $1
     WHERE i.status = 'open'
       AND i.kind = 'Driver'
       AND NOT EXISTS (
         SELECT 1 FROM tbl_driver_jobs j
         WHERE j.vwork_job_id IS NOT NULL
           AND lower(btrim(coalesce(j.driver, ''))) = lower(btrim(i.vwork_name))
           AND (
             j.imei IS NULL OR btrim(j.imei) = ''
             OR NOT EXISTS (
               SELECT 1 FROM vwork_import_fence_map m
               WHERE m.kind = 'Driver' AND lower(btrim(m.vwork_name)) = lower(btrim(i.vwork_name))
             )
           )
       )`,
    [now],
  );
}

export async function driverChoices(): Promise<{ label: string; imei: string }[]> {
  const { withImei } = await driverHitIndex();
  return withImei.flatMap((driver) => (driver.imei ? [{ label: driver.label, imei: driver.imei }] : []));
}
