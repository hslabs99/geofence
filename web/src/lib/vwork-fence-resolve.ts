import { execute, query } from '@/lib/db';
import { applyVworkDriverResolution, driverChoices, refreshDriverImeisFromTracksolid } from '@/lib/driver-imei-sync';
import { ensureVworkFenceIssueSchema } from '@/lib/ensure-vwork-fence-issue-schema';

export type FenceKind = 'Winery' | 'Vineyard';

export type VworkFenceDecision = {
  fence: string | null;
  candidates: string[];
};

function localClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

async function fenceSet(): Promise<Map<string, string>> {
  const rows = await query<{ fence_name: string }>(
    `SELECT DISTINCT ON (lower(btrim(fence_name))) btrim(fence_name) AS fence_name
     FROM tbl_geofences
     WHERE fence_name IS NOT NULL AND btrim(fence_name) <> ''
     ORDER BY lower(btrim(fence_name)), btrim(fence_name)`,
  );
  const fences = new Map<string, string>();
  for (const row of rows) {
    const name = row.fence_name.trim();
    if (name) fences.set(name.toLowerCase(), name);
  }
  return fences;
}

type FenceIndex = {
  fences: Map<string, string>;
  saved: Map<string, string>;
  mapped: Map<string, string[]>;
};

function keyOf(kind: FenceKind, name: string): string {
  return `${kind}\n${name.trim().toLowerCase()}`;
}

async function loadFenceIndex(): Promise<FenceIndex> {
  await ensureVworkFenceIssueSchema();
  const [fences, savedRows, mappedRows] = await Promise.all([
    fenceSet(),
    query<{ kind: string; vwork_name: string; fence_name: string }>(
      `SELECT kind, vwork_name, fence_name FROM vwork_import_fence_map`,
    ),
    query<{ type: string | null; vwname: string | null; gpsname: string | null }>(
      `SELECT type, vwname, gpsname FROM tbl_gpsmappings WHERE type IN ('Winery', 'Vineyard')`,
    ),
  ]);
  const saved = new Map<string, string>();
  for (const row of savedRows) {
    if (row.kind === 'Winery' || row.kind === 'Vineyard') {
      saved.set(keyOf(row.kind, row.vwork_name), row.fence_name);
    }
  }
  const mapped = new Map<string, string[]>();
  for (const row of mappedRows) {
    if (row.type !== 'Winery' && row.type !== 'Vineyard') continue;
    const name = (row.vwname ?? '').trim();
    const gps = (row.gpsname ?? '').trim();
    if (!name || !gps) continue;
    const key = keyOf(row.type, name);
    const list = mapped.get(key) ?? [];
    list.push(gps);
    mapped.set(key, list);
  }
  return { fences, saved, mapped };
}

/**
 * One fence, or the list the controller has to choose from.
 * The VWork import map wins. Otherwise a name is precise only when the raw name
 * and tbl_gpsmappings together point at a single existing fence.
 */
function decideFromIndex(index: FenceIndex, kind: FenceKind, vworkName: string): VworkFenceDecision {
  const name = vworkName.trim();
  if (!name) return { fence: null, candidates: [] };
  const savedFence = index.saved.get(keyOf(kind, name))?.trim();
  if (savedFence && index.fences.has(savedFence.toLowerCase())) {
    return { fence: index.fences.get(savedFence.toLowerCase()) ?? savedFence, candidates: [] };
  }
  const found = new Set<string>();
  const exact = index.fences.get(name.toLowerCase());
  if (exact) found.add(exact);
  for (const gps of index.mapped.get(keyOf(kind, name)) ?? []) {
    const hit = index.fences.get(gps.toLowerCase());
    if (hit) found.add(hit);
  }
  const candidates = [...found].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  if (candidates.length === 1) return { fence: candidates[0], candidates: [] };
  return { fence: null, candidates };
}

type NamedJob = { id: number; job_id: string; name: string | null; fence: string | null };

/** Fills a unique fence and opens an issue when a VWork name still matches zero or several fences. */
export async function applyVworkFenceResolution(): Promise<{ open: number }> {
  const index = await loadFenceIndex();
  const jobs = await query<NamedJob & { vineyard_name: string | null; vineyard_geo_fence: string | null }>(
    `SELECT id, job_id, winery_name AS name, winery_geo_fence AS fence, vineyard_name, vineyard_geo_fence
     FROM tbl_driver_jobs
     WHERE vwork_job_id IS NOT NULL`,
  );
  const pending = new Map<string, { kind: FenceKind; name: string; candidates: string[] }>();
  for (const job of jobs) {
    await consider(index, job.id, 'Winery', job.name, job.fence, 'winery_geo_fence', pending);
    await consider(index, job.id, 'Vineyard', job.vineyard_name, job.vineyard_geo_fence, 'vineyard_geo_fence', pending);
  }
  const now = localClock();
  for (const item of pending.values()) {
    await execute(
      `INSERT INTO vwork_job_queue_issues (kind, vwork_name, candidate_fences, status, created_at)
       VALUES ($1, $2, $3, 'open', $4)
       ON CONFLICT (kind, lower(btrim(vwork_name))) WHERE status = 'open'
       DO UPDATE SET candidate_fences = EXCLUDED.candidate_fences`,
      [item.kind, item.name, JSON.stringify(item.candidates), now],
    );
  }
  await execute(
    `UPDATE vwork_job_queue_issues i
     SET status = 'resolved', resolved_at = $1
     WHERE i.status = 'open'
       AND i.kind IN ('Winery', 'Vineyard')
       AND NOT EXISTS (
         SELECT 1 FROM tbl_driver_jobs j
         WHERE j.vwork_job_id IS NOT NULL
           AND (
             (i.kind = 'Winery'
               AND lower(btrim(coalesce(j.winery_name, ''))) = lower(btrim(i.vwork_name))
               AND (j.winery_geo_fence IS NULL OR btrim(j.winery_geo_fence) = ''))
             OR
             (i.kind = 'Vineyard'
               AND lower(btrim(coalesce(j.vineyard_name, ''))) = lower(btrim(i.vwork_name))
               AND (j.vineyard_geo_fence IS NULL OR btrim(j.vineyard_geo_fence) = ''))
           )
       )`,
    [now],
  );
  const open = await query<{ n: number }>(`SELECT count(*)::int AS n FROM vwork_job_queue_issues WHERE status = 'open'`);
  return { open: open[0]?.n ?? 0 };
}

async function consider(
  index: FenceIndex,
  jobId: number,
  kind: FenceKind,
  rawName: string | null,
  currentFence: string | null,
  column: 'winery_geo_fence' | 'vineyard_geo_fence',
  pending: Map<string, { kind: FenceKind; name: string; candidates: string[] }>,
): Promise<void> {
  const name = (rawName ?? '').trim();
  if (!name || (currentFence ?? '').trim()) return;
  const decision = decideFromIndex(index, kind, name);
  if (decision.fence) {
    await execute(`UPDATE tbl_driver_jobs SET ${column} = $2 WHERE id = $1 AND (${column} IS NULL OR btrim(${column}) = '')`, [
      jobId,
      decision.fence,
    ]);
    return;
  }
  pending.set(`${kind}\n${name.toLowerCase()}`, { kind, name, candidates: decision.candidates });
}

export async function listOpenVworkFenceIssues(): Promise<{
  open: number;
  fences: string[];
  drivers: { label: string; imei: string }[];
  rows: {
    id: number;
    kind: 'Winery' | 'Vineyard' | 'Driver';
    vworkName: string;
    candidates: string[];
    createdAt: string;
    jobs: { id: number; jobId: string; driver: string | null; plannedStart: string | null }[];
  }[];
  resolved: {
    id: number;
    kind: 'Winery' | 'Vineyard' | 'Driver';
    vworkName: string;
    mappedTo: string;
    resolvedAt: string;
  }[];
}> {
  try {
    await refreshDriverImeisFromTracksolid(false);
  } catch (err) {
    console.error('[vwork-import] TrackSolid driver IMEI refresh failed', err);
  }
  const applied = await applyVworkFenceResolution();
  await applyVworkDriverResolution();
  const fences = [...(await fenceSet()).values()].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  const drivers = await driverChoices();
  const issues = await query<{ id: number; kind: 'Winery' | 'Vineyard' | 'Driver'; vwork_name: string; candidate_fences: string | null; created_at: string }>(
    `SELECT id, kind, vwork_name, candidate_fences, created_at
     FROM vwork_job_queue_issues
     WHERE status = 'open'
     ORDER BY created_at, id`,
  );
  const rows = [];
  for (const issue of issues) {
    const jobs = await query<{ id: number; job_id: string; driver: string | null; planned_start_time: string | null }>(
      issue.kind === 'Driver'
        ? `SELECT id, job_id, driver, to_char(planned_start_time, 'YYYY-MM-DD HH24:MI:SS') AS planned_start_time
           FROM tbl_driver_jobs
           WHERE vwork_job_id IS NOT NULL
             AND lower(btrim(coalesce(driver, ''))) = lower($1)
             AND (
               imei IS NULL OR btrim(imei) = ''
               OR NOT EXISTS (
                 SELECT 1 FROM vwork_import_fence_map m
                 WHERE m.kind = 'Driver' AND lower(btrim(m.vwork_name)) = lower($1)
               )
             )
           ORDER BY planned_start_time NULLS LAST, job_id`
        : `SELECT id, job_id, driver, to_char(planned_start_time, 'YYYY-MM-DD HH24:MI:SS') AS planned_start_time
           FROM tbl_driver_jobs
           WHERE vwork_job_id IS NOT NULL
             AND lower(btrim(coalesce(${issue.kind === 'Winery' ? 'winery_name' : 'vineyard_name'}, ''))) = lower($1)
             AND (${issue.kind === 'Winery' ? 'winery_geo_fence' : 'vineyard_geo_fence'} IS NULL OR btrim(${issue.kind === 'Winery' ? 'winery_geo_fence' : 'vineyard_geo_fence'}) = '')
           ORDER BY planned_start_time NULLS LAST, job_id`,
      [issue.vwork_name],
    );
    let candidates: string[] = [];
    try {
      const parsed = JSON.parse(issue.candidate_fences ?? '[]') as unknown;
      candidates = Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
    } catch {
      candidates = [];
    }
    rows.push({
      id: issue.id,
      kind: issue.kind,
      vworkName: issue.vwork_name,
      candidates,
      createdAt: issue.created_at,
      jobs: jobs.map((job) => ({
        id: job.id,
        jobId: job.job_id,
        driver: job.driver,
        plannedStart: job.planned_start_time,
      })),
    });
  }
  const openRows = await query<{ n: number }>(`SELECT count(*)::int AS n FROM vwork_job_queue_issues WHERE status = 'open'`);
  const resolvedRows = await query<{
    id: number;
    kind: 'Winery' | 'Vineyard' | 'Driver';
    vwork_name: string;
    resolved_fence: string | null;
    resolved_at: string | null;
  }>(
    `SELECT id, kind, vwork_name, resolved_fence, resolved_at
     FROM vwork_job_queue_issues
     WHERE status = 'resolved' AND resolved_at IS NOT NULL
     ORDER BY resolved_at DESC, id DESC
     LIMIT 40`,
  );
  return {
    open: openRows[0]?.n ?? applied.open,
    fences,
    drivers,
    rows,
    resolved: resolvedRows.map((row) => ({
      id: row.id,
      kind: row.kind,
      vworkName: row.vwork_name,
      mappedTo: (row.resolved_fence ?? '').trim(),
      resolvedAt: row.resolved_at ?? '',
    })),
  };
}

async function resolveDriverIssue(issueId: number, vworkName: string, driverLabel: string): Promise<void> {
  const choices = await driverChoices();
  const chosen = choices.find((driver) => driver.label.toLowerCase() === driverLabel.toLowerCase());
  if (!chosen) throw new Error('Pick a driver that has a TrackSolid IMEI');
  const now = localClock();
  await execute(
    `INSERT INTO vwork_import_fence_map (kind, vwork_name, fence_name, created_at)
     VALUES ('Driver', $1, $2, $3)
     ON CONFLICT (kind, lower(btrim(vwork_name)))
     DO UPDATE SET fence_name = EXCLUDED.fence_name`,
    [vworkName.trim(), chosen.label, now],
  );
  const trucks = await query<{ device_name: string }>(
    `SELECT btrim(device_name) AS device_name
     FROM tbl_devices
     WHERE btrim(imei) = $1 AND device_name IS NOT NULL AND btrim(device_name) <> ''
     ORDER BY CASE WHEN lower(btrim(device_name)) = lower($2) THEN 0 ELSE 1 END, btrim(device_name)
     LIMIT 1`,
    [chosen.imei, chosen.label],
  );
  await execute(
    `UPDATE tbl_driver_jobs
     SET driver = $1, imei = $2, truck_id = COALESCE($3, truck_id)
     WHERE vwork_job_id IS NOT NULL
       AND lower(btrim(coalesce(driver, ''))) = lower($4)`,
    [chosen.label, chosen.imei, trucks[0]?.device_name?.trim() || null, vworkName.trim()],
  );
  await execute(
    `UPDATE vwork_job_queue_issues
     SET status = 'resolved', resolved_fence = $2, resolved_at = $3
     WHERE id = $1`,
    [issueId, chosen.label, now],
  );
}

/** Saves the controller's fence choice for this VWork import and fills every waiting job. */
export async function resolveVworkFenceIssue(issueId: number, fenceName: string): Promise<void> {
  await ensureVworkFenceIssueSchema();
  const fence = fenceName.trim();
  const issues = await query<{ id: number; kind: 'Winery' | 'Vineyard' | 'Driver'; vwork_name: string }>(
    `SELECT id, kind, vwork_name FROM vwork_job_queue_issues WHERE id = $1 AND status = 'open'`,
    [issueId],
  );
  const issue = issues[0];
  if (!issue) throw new Error('That issue is no longer open');
  if (issue.kind === 'Driver') {
    await resolveDriverIssue(issue.id, issue.vwork_name, fence);
    return;
  }
  const fences = await fenceSet();
  const canonical = fences.get(fence.toLowerCase());
  if (!canonical) throw new Error('Pick an existing geo fence');
  const now = localClock();
  await execute(
    `INSERT INTO vwork_import_fence_map (kind, vwork_name, fence_name, created_at)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (kind, lower(btrim(vwork_name)))
     DO UPDATE SET fence_name = EXCLUDED.fence_name`,
    [issue.kind, issue.vwork_name.trim(), canonical, now],
  );
  const column = issue.kind === 'Winery' ? 'winery_geo_fence' : 'vineyard_geo_fence';
  const nameColumn = issue.kind === 'Winery' ? 'winery_name' : 'vineyard_name';
  await execute(
    `UPDATE tbl_driver_jobs
     SET ${column} = $1
     WHERE vwork_job_id IS NOT NULL
       AND lower(btrim(coalesce(${nameColumn}, ''))) = lower($2)
       AND (${column} IS NULL OR btrim(${column}) = '')`,
    [canonical, issue.vwork_name.trim()],
  );
  await execute(
    `UPDATE vwork_job_queue_issues
     SET status = 'resolved', resolved_fence = $2, resolved_at = $3
     WHERE id = $1`,
    [issue.id, canonical, now],
  );
}
