import type { Client, PoolClient } from 'pg';

/** pg Client or PoolClient; both support query() and transactions. */
type DbClient = Client | PoolClient;
import { listPlatformFences, type TracksolidPlatformFence } from './tracksolid';

const SOURCE_NAME = 'tracksolid';

export interface StagingRow {
  source_name: string;
  source_fence_id: string;
  fence_name: string;
  fence_type: string;
  color: string | null;
  description: string | null;
  geom_wkt: string;
  radius_meters: number | null;
  /** Platform last-modified time when API provides it (ISO string). Used to skip overwriting with older data. */
  source_updated_at: string | null;
}

/** Convert API fence to staging row. Polygon: geom "lat,lng#lat,lng#..." → POLYGON((lng lat, ...)). Circle: "lat,lng" → POINT(lng lat); radius stored for SQL buffer. */
export function tracksolidFenceToStaging(f: TracksolidPlatformFence): StagingRow {
  let geom_wkt: string;
  if (f.type === 'circle') {
    const parts = f.geom.trim().split(',');
    const lat = parseFloat(parts[0]);
    const lng = parseFloat(parts[1]);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      geom_wkt = 'POINT(0 0)';
    } else {
      geom_wkt = `POINT(${lng} ${lat})`;
    }
  } else {
    const points = f.geom.split('#').map((pair) => {
      const [latStr, lngStr] = pair.trim().split(',');
      const lat = parseFloat(latStr ?? '0');
      const lng = parseFloat(lngStr ?? '0');
      return { lat, lng };
    }).filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng));
    if (points.length < 3) {
      geom_wkt = 'POLYGON((0 0, 1 0, 1 1, 0 0))';
    } else {
      const first = points[0];
      const last = points[points.length - 1];
      const closed = last.lat === first.lat && last.lng === first.lng
        ? points
        : [...points, first];
      const coords = closed.map((p) => `${p.lng} ${p.lat}`).join(', ');
      geom_wkt = `POLYGON((${coords}))`;
    }
  }
  return {
    source_name: SOURCE_NAME,
    source_fence_id: f.fenceId,
    fence_name: f.name,
    fence_type: f.type,
    color: f.color ?? null,
    description: f.description ?? null,
    geom_wkt,
    radius_meters: f.type === 'circle' && f.radius != null ? f.radius : null,
    source_updated_at: f.lastModified ?? null,
  };
}

const STAGING_BATCH = 50;

export type SyncProgressEvent = {
  stage: string;
  message?: string;
  current?: number;
  total?: number;
  fetched?: number;
  inserted?: number;
  updated?: number;
  deleted?: number;
  error?: string;
  /** Per-fence: 'inserted' | 'updated' */
  action?: 'inserted' | 'updated';
  fence_name?: string;
};

/** TRUNCATE staging then bulk INSERT. Staging table must have radius_meters column (nullable). Calls onProgress after each batch. */
export async function loadTracksolidFencesToStaging(
  client: DbClient,
  rows: StagingRow[],
  onProgress?: (e: SyncProgressEvent) => void
): Promise<void> {
  await client.query('TRUNCATE TABLE public.stg_tracksolid_geofences');
  if (rows.length === 0) return;
  const totalBatches = Math.ceil(rows.length / STAGING_BATCH);
  for (let i = 0; i < rows.length; i += STAGING_BATCH) {
    const batchIndex = Math.floor(i / STAGING_BATCH) + 1;
    const batch = rows.slice(i, i + STAGING_BATCH);
    const placeholders = batch.map((_, b) => {
      const o = b * 9;
      return `($${o + 1}, $${o + 2}, $${o + 3}, $${o + 4}, $${o + 5}, $${o + 6}, $${o + 7}, $${o + 8}, $${o + 9})`;
    }).join(', ');
    const params = batch.flatMap((r) => [
      r.source_name, r.source_fence_id, r.fence_name, r.fence_type,
      r.color, r.description, r.geom_wkt, r.radius_meters,
      (r.source_updated_at?.trim()) ? r.source_updated_at : null,
    ]);
    await client.query(
      `INSERT INTO public.stg_tracksolid_geofences
       (source_name, source_fence_id, fence_name, fence_type, color, description, geom_wkt, radius_meters, source_updated_at)
       VALUES ${placeholders}`,
      params
    );
    const rowsSoFar = Math.min(i + STAGING_BATCH, rows.length);
    onProgress?.({
      stage: 'staging',
      message: `Staging: wrote ${rowsSoFar}/${rows.length} rows (batch ${batchIndex}/${totalBatches})`,
      current: batchIndex,
      total: totalBatches,
    });
  }
}

/** One row per fence_name from staging; keeps the last by source_fence_id so geom matches the most recent. Fence key is fence_name only. */
const STAGING_DEDUP_CTE = `
  staging_dedup AS (
    SELECT DISTINCT ON (fence_name)
      source_name, fence_name, fence_type, geom_wkt, radius_meters, source_updated_at
    FROM stg_tracksolid_geofences
    ORDER BY fence_name, source_fence_id DESC
  )
`;

/** One row per fence_name from staging (for fence-by-fence merge). */
const STAGING_DEDUP_SELECT = `
  SELECT source_name, fence_name, fence_type, geom_wkt, radius_meters, source_updated_at
  FROM (
    SELECT DISTINCT ON (fence_name)
      source_name, fence_name, fence_type, geom_wkt, radius_meters, source_updated_at
    FROM stg_tracksolid_geofences
    ORDER BY fence_name, source_fence_id DESC
  ) s
`;

/** Merge staging into tbl_geofences by fence_name only (fence_id is table PK, not from API). Insert new, update geom for existing, delete tracksolid-only fences no longer in staging. Same fence from different sources is one row. */
export async function mergeStagingIntoTblGeofences(client: DbClient): Promise<{ inserted: number; updated: number; deleted: number }> {
  const insertResult = await client.query(`
    WITH ${STAGING_DEDUP_CTE}
    INSERT INTO tbl_geofences (fence_name, source_name, geom, source_updated_at)
    SELECT s.fence_name, s.source_name,
      CASE
        WHEN s.fence_type = 'circle' AND s.radius_meters IS NOT NULL THEN
          ST_Multi(ST_Buffer(ST_GeomFromText(s.geom_wkt, 4326)::geography, s.radius_meters)::geometry)
        ELSE
          ST_Multi(ST_GeomFromText(s.geom_wkt, 4326))
      END,
      CASE WHEN NULLIF(TRIM(s.source_updated_at), '') IS NOT NULL THEN NULLIF(TRIM(s.source_updated_at), '')::timestamptz ELSE NULL END
    FROM staging_dedup s
    LEFT JOIN tbl_geofences t ON t.fence_name = s.fence_name
    WHERE t.fence_id IS NULL
  `);
  const inserted = insertResult.rowCount ?? 0;

  const updateResult = await client.query(`
    WITH ${STAGING_DEDUP_CTE}
    UPDATE tbl_geofences t
    SET geom = CASE
        WHEN s.fence_type = 'circle' AND s.radius_meters IS NOT NULL THEN
          ST_Multi(ST_Buffer(ST_GeomFromText(s.geom_wkt, 4326)::geography, s.radius_meters)::geometry)
        ELSE
          ST_Multi(ST_GeomFromText(s.geom_wkt, 4326))
      END,
      source_name = s.source_name,
      source_updated_at = CASE WHEN NULLIF(TRIM(s.source_updated_at), '') IS NOT NULL THEN NULLIF(TRIM(s.source_updated_at), '')::timestamptz ELSE t.source_updated_at END,
      updated_at = now()
    FROM staging_dedup s
    WHERE t.fence_name = s.fence_name
  `);
  const updated = updateResult.rowCount ?? 0;

  const deleteResult = await client.query(`
    DELETE FROM tbl_geofences t
    WHERE t.source_name = $1
      AND NOT EXISTS (
        SELECT 1 FROM stg_tracksolid_geofences s
        WHERE s.fence_name = t.fence_name
      )
  `, [SOURCE_NAME]);
  const deleted = deleteResult.rowCount ?? 0;

  return { inserted, updated, deleted };
}

type StagingDedupRow = {
  source_name: string;
  fence_name: string;
  fence_type: string;
  geom_wkt: string;
  radius_meters: number | null;
  source_updated_at: string | null | Date;
};

/** Merge staging into tbl_geofences fence-by-fence, calling onProgress for each inserted/updated row. */
export async function mergeStagingIntoTblGeofencesWithProgress(
  client: DbClient,
  onProgress?: (e: SyncProgressEvent) => void
): Promise<{ inserted: number; updated: number; deleted: number }> {
  const { rows: dedupRows } = await client.query<StagingDedupRow>(STAGING_DEDUP_SELECT);
  let inserted = 0;
  let updated = 0;

  for (const s of dedupRows) {
    const raw = s.source_updated_at;
    const sourceUpdatedAt =
      raw == null
        ? null
        : raw instanceof Date
          ? raw.toISOString()
          : (String(raw).trim() || null);

    const existing = await client.query<{ fence_id: number }>(
      'SELECT fence_id FROM tbl_geofences WHERE fence_name = $1 LIMIT 1',
      [s.fence_name]
    );

    if (existing.rows.length === 0) {
      await client.query(
        `INSERT INTO tbl_geofences (fence_name, source_name, geom, source_updated_at)
         VALUES ($1, $2,
           CASE
             WHEN $4 = 'circle' AND $5 IS NOT NULL THEN
               ST_Multi(ST_Buffer(ST_GeomFromText($3, 4326)::geography, $5)::geometry)
             ELSE
               ST_Multi(ST_GeomFromText($3, 4326))
           END,
           CASE WHEN $6 IS NOT NULL AND $6 <> '' THEN $6::timestamptz ELSE NULL END
         )`,
        [s.fence_name, s.source_name, s.geom_wkt, s.fence_type, s.radius_meters, sourceUpdatedAt]
      );
      inserted += 1;
      onProgress?.({ stage: 'fence', action: 'inserted', fence_name: s.fence_name, message: `Inserted: ${s.fence_name}` });
    } else {
      await client.query(
        `UPDATE tbl_geofences
         SET geom = CASE
               WHEN $4 = 'circle' AND $5 IS NOT NULL THEN
                 ST_Multi(ST_Buffer(ST_GeomFromText($3, 4326)::geography, $5)::geometry)
               ELSE
                 ST_Multi(ST_GeomFromText($3, 4326))
             END,
             source_name = $2,
             source_updated_at = CASE WHEN $6 IS NOT NULL AND $6 <> '' THEN $6::timestamptz ELSE source_updated_at END,
             updated_at = now()
         WHERE fence_name = $1`,
        [s.fence_name, s.source_name, s.geom_wkt, s.fence_type, s.radius_meters, sourceUpdatedAt]
      );
      updated += 1;
      onProgress?.({ stage: 'fence', action: 'updated', fence_name: s.fence_name, message: `Updated: ${s.fence_name}` });
    }
  }

  const deleteResult = await client.query(
    `DELETE FROM tbl_geofences t
     WHERE t.source_name = $1
       AND NOT EXISTS (
         SELECT 1 FROM stg_tracksolid_geofences s
         WHERE s.fence_name = t.fence_name
       )`,
    [SOURCE_NAME]
  );
  const deleted = deleteResult.rowCount ?? 0;
  if (deleted > 0) {
    onProgress?.({ stage: 'deleted', message: `Deleted ${deleted} fence(s) no longer in platform.` });
  }

  return { inserted, updated, deleted };
}

export interface SyncResult {
  fetched: number;
  inserted: number;
  updated: number;
  deleted: number;
}

/** Fetch platform fences, load to staging, merge into tbl_geofences. Runs in a single transaction. Calls onProgress for real-time progress. */
export async function syncTracksolidPlatformGeofences(
  accessToken: string,
  endpoint: string | null | undefined,
  client: DbClient,
  onProgress?: (e: SyncProgressEvent) => void
): Promise<SyncResult> {
  onProgress?.({ stage: 'fetch', message: 'Fetching platform fence list from Tracksolid…' });
  const { fences, resultKeys } = await listPlatformFences(accessToken, endpoint);
  if (resultKeys?.length && fences.length === 0) {
    onProgress?.({ stage: 'debug_result_keys', message: `API returned 0 fences. Result top-level keys: ${resultKeys.join(', ')} — fence list may be nested under one of these.` });
  }
  onProgress?.({
    stage: 'fetched',
    message: `Fetched ${fences.length} fence(s) from API`,
    fetched: fences.length,
  });

  onProgress?.({ stage: 'convert', message: `Converting ${fences.length} fence(s) to staging rows…` });
  const rows = fences.map(tracksolidFenceToStaging);
  onProgress?.({ stage: 'converted', message: `Converted ${rows.length} row(s). Loading to staging…` });

  await client.query('BEGIN');
  try {
    onProgress?.({ stage: 'staging_start', message: `Writing to stg_tracksolid_geofences (${rows.length} rows in batches of ${STAGING_BATCH})…` });
    await loadTracksolidFencesToStaging(client, rows, onProgress);

    onProgress?.({ stage: 'merge', message: 'Merging into tbl_geofences (insert new, update existing, delete removed)…' });
    const { inserted, updated, deleted } = await mergeStagingIntoTblGeofencesWithProgress(client, onProgress);
    await client.query('COMMIT');

    onProgress?.({
      stage: 'done',
      message: `Done. Inserted ${inserted}, updated ${updated}, deleted ${deleted}.`,
      fetched: fences.length,
      inserted,
      updated,
      deleted,
    });
    return { fetched: fences.length, inserted, updated, deleted };
  } catch (e) {
    await client.query('ROLLBACK');
    const msg = e instanceof Error ? e.message : String(e);
    onProgress?.({ stage: 'error', message: msg, error: msg });
    throw e;
  }
}
