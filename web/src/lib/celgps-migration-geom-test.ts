import type { Pool } from 'pg';
import {
  createMigrationPool,
  getCelgpsMigrationConfig,
  safeConnectionSummary,
  type CelgpsMigrationConfig,
} from '@/lib/celgps-migration-config';

export interface GeomTestCheck {
  name: string;
  ok: boolean;
  detail: string;
  ms: number;
}

/** One real tbl_tracking row exercised against tbl_geofences (read-only). */
export interface FenceSampleRow {
  trackingId: number;
  deviceName: string | null;
  positionTimeNz: string | null;
  lat: string | null;
  lon: string | null;
  storedGeofenceId: number | null;
  storedFenceName: string | null;
  computedGeofenceId: number | null;
  computedFenceName: string | null;
  withinStoredFence: boolean;
  matchesComputed: boolean;
}

export interface GeomTestResult {
  ok: boolean;
  error: string | null;
  durationMs: number;
  connection: ReturnType<typeof safeConnectionSummary> | null;
  checks: GeomTestCheck[];
  probeDate: string | null;
  fenceSamples: FenceSampleRow[];
}

const REQUIRED_STORE_FUNCTIONS = [
  'store_entryexit',
  'store_entryexit_device_from',
  'store_entryexit_device_from_date',
  'store_fences',
  'store_fences_for_date',
  'store_fences_for_date_scoped',
] as const;

async function timedCheck(
  name: string,
  run: () => Promise<{ ok: boolean; detail: string }>
): Promise<GeomTestCheck> {
  const started = Date.now();
  try {
    const { ok, detail } = await run();
    return { name, ok, detail, ms: Date.now() - started };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { name, ok: false, detail: message, ms: Date.now() - started };
  }
}

async function resolveProbeDate(pool: Pool, requested: string | null): Promise<string | null> {
  if (requested && /^\d{4}-\d{2}-\d{2}$/.test(requested)) return requested;
  const rows = await pool.query<{ d: string | null }>(
    `SELECT MAX(position_time_nz)::date::text AS d
     FROM tbl_tracking
     WHERE geom IS NOT NULL AND position_time_nz IS NOT NULL`
  );
  return rows.rows[0]?.d ?? null;
}

async function loadFenceSamples(pool: Pool, probeDate: string | null): Promise<FenceSampleRow[]> {
  const res = await pool.query<{
    id: number;
    device_name: string | null;
    position_time_nz: string | null;
    lat: string | null;
    lon: string | null;
    geofence_id: number | null;
    stored_fence_name: string | null;
    computed_fence_id: number | null;
    computed_fence_name: string | null;
    within_stored_fence: boolean | null;
  }>(
    `SELECT
       t.id,
       t.device_name,
       t.position_time_nz::text AS position_time_nz,
       t.lat::text AS lat,
       t.lon::text AS lon,
       t.geofence_id,
       g.fence_name AS stored_fence_name,
       (
         SELECT g2.fence_id
         FROM tbl_geofences g2
         WHERE g2.geom IS NOT NULL
           AND NOT ST_IsEmpty(g2.geom)
           AND ST_Within(
                 ST_Force2D(ST_SetSRID(t.geom, 4326)),
                 ST_Force2D(ST_SetSRID(g2.geom, 4326))
               )
         ORDER BY ST_Area(g2.geom) ASC
         LIMIT 1
       ) AS computed_fence_id,
       (
         SELECT g2.fence_name
         FROM tbl_geofences g2
         WHERE g2.geom IS NOT NULL
           AND NOT ST_IsEmpty(g2.geom)
           AND ST_Within(
                 ST_Force2D(ST_SetSRID(t.geom, 4326)),
                 ST_Force2D(ST_SetSRID(g2.geom, 4326))
               )
         ORDER BY ST_Area(g2.geom) ASC
         LIMIT 1
       ) AS computed_fence_name,
       CASE
         WHEN g.geom IS NOT NULL AND NOT ST_IsEmpty(g.geom) THEN
           ST_Within(
             ST_Force2D(ST_SetSRID(t.geom, 4326)),
             ST_Force2D(ST_SetSRID(g.geom, 4326))
           )
         ELSE FALSE
       END AS within_stored_fence
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
     WHERE t.geom IS NOT NULL
       AND t.geofence_mapped IS TRUE
       AND t.geofence_id IS NOT NULL
       AND ($1::date IS NULL OR t.position_time_nz::date = $1::date)
     ORDER BY t.id DESC
     LIMIT 3`,
    [probeDate]
  );

  return res.rows.map((r) => ({
    trackingId: r.id,
    deviceName: r.device_name,
    positionTimeNz: r.position_time_nz,
    lat: r.lat,
    lon: r.lon,
    storedGeofenceId: r.geofence_id,
    storedFenceName: r.stored_fence_name,
    computedGeofenceId: r.computed_fence_id,
    computedFenceName: r.computed_fence_name,
    withinStoredFence: r.within_stored_fence === true,
    matchesComputed: r.geofence_id != null && r.computed_fence_id === r.geofence_id,
  }));
}

function formatFenceSampleSummary(samples: FenceSampleRow[]): string {
  if (samples.length === 0) return 'No geofence_mapped tracking rows found for probe date';
  const parts = samples.map((s) => {
    const who = s.deviceName ?? `id ${s.trackingId}`;
    const when = s.positionTimeNz ?? '?';
    const stored = s.storedFenceName ?? `#${s.storedGeofenceId ?? '?'}`;
    const ok = s.withinStoredFence && s.matchesComputed;
    return `${who} @ ${when}: stored=${stored}, ST_Within=${s.withinStoredFence ? 'yes' : 'no'}, computed=${s.computedFenceName ?? 'none'}${ok ? ' ✓' : ' ✗'}`;
  });
  const allOk = samples.every((s) => s.withinStoredFence && s.matchesComputed);
  return `${allOk ? 'OK' : 'MISMATCH'} — ${parts.join(' | ')}`;
}

/** Read-only PostGIS / store_* verification against Supabase (never mutates data). */
export async function runSupabaseGeomTests(probeDateInput: string | null): Promise<GeomTestResult> {
  const started = Date.now();
  const cfg = getCelgpsMigrationConfig();
  if (!cfg.ok) {
    return {
      ok: false,
      error: cfg.error,
      durationMs: Date.now() - started,
      connection: null,
      checks: [],
      probeDate: null,
      fenceSamples: [],
    };
  }

  const pool = createMigrationPool(cfg as CelgpsMigrationConfig);
  const checks: GeomTestCheck[] = [];
  let fenceSamples: FenceSampleRow[] = [];

  try {
    checks.push(
      await timedCheck('PostGIS version', async () => {
        const res = await pool.query<{ v: string }>(`SELECT PostGIS_Version() AS v`);
        const v = res.rows[0]?.v ?? '';
        return { ok: !!v, detail: v || 'PostGIS_Version() returned empty' };
      })
    );

    checks.push(
      await timedCheck('postgis extension', async () => {
        const res = await pool.query<{ extname: string; extversion: string }>(
          `SELECT extname, extversion FROM pg_extension WHERE extname = 'postgis'`
        );
        const row = res.rows[0];
        if (!row) return { ok: false, detail: 'postgis extension not installed' };
        return { ok: true, detail: `${row.extname} ${row.extversion}` };
      })
    );

    checks.push(
      await timedCheck('store_* functions', async () => {
        const res = await pool.query<{ proname: string }>(
          `SELECT p.proname
           FROM pg_proc p
           JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE n.nspname = 'public'
             AND p.proname = ANY($1::text[])
           ORDER BY p.proname`,
          [REQUIRED_STORE_FUNCTIONS as unknown as string[]]
        );
        const found = new Set(res.rows.map((r) => r.proname));
        const missing = REQUIRED_STORE_FUNCTIONS.filter((f) => !found.has(f));
        if (missing.length > 0) {
          return {
            ok: false,
            detail: `Missing: ${missing.join(', ')}. Found: ${[...found].join(', ') || 'none'}`,
          };
        }
        return { ok: true, detail: `All ${REQUIRED_STORE_FUNCTIONS.length} functions present` };
      })
    );

    checks.push(
      await timedCheck('Geofence geometries', async () => {
        const res = await pool.query<{ total: string; with_geom: string; valid: string }>(
          `SELECT
             COUNT(*)::text AS total,
             COUNT(*) FILTER (WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom))::text AS with_geom,
             COUNT(*) FILTER (
               WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) AND ST_IsValid(geom)
             )::text AS valid
           FROM tbl_geofences`
        );
        const r = res.rows[0];
        const withGeom = Number(r?.with_geom ?? 0);
        return {
          ok: withGeom > 0,
          detail: `${withGeom} with geom (${r?.valid ?? 0} valid) of ${r?.total ?? 0} fences`,
        };
      })
    );

    checks.push(
      await timedCheck('Tracking point geometries', async () => {
        const res = await pool.query<{ total: string; with_geom: string }>(
          `SELECT
             COUNT(*)::text AS total,
             COUNT(*) FILTER (WHERE geom IS NOT NULL)::text AS with_geom
           FROM tbl_tracking`
        );
        const r = res.rows[0];
        const withGeom = Number(r?.with_geom ?? 0);
        return {
          ok: withGeom > 0,
          detail: `${withGeom.toLocaleString()} rows with geom of ${Number(r?.total ?? 0).toLocaleString()} total`,
        };
      })
    );

    checks.push(
      await timedCheck('ST_MakePoint (WGS84)', async () => {
        const res = await pool.query<{ wkt: string; srid: number }>(
          `SELECT ST_AsText(ST_SetSRID(ST_MakePoint(174.7762, -41.2865), 4326)) AS wkt,
                  ST_SRID(ST_SetSRID(ST_MakePoint(174.7762, -41.2865), 4326)) AS srid`
        );
        const row = res.rows[0];
        const ok = row?.srid === 4326 && (row?.wkt ?? '').includes('POINT');
        return {
          ok,
          detail: ok ? `${row!.wkt} (SRID ${row!.srid})` : 'ST_MakePoint failed',
        };
      })
    );

    const probeDate = await resolveProbeDate(pool, probeDateInput);

    checks.push(
      await timedCheck('Sample fence area (ST_Area geography)', async () => {
        const res = await pool.query<{ fence_name: string | null; area_m2: string | null }>(
          `SELECT fence_name, ST_Area(geom::geography)::text AS area_m2
           FROM tbl_geofences
           WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
           ORDER BY fence_id
           LIMIT 1`
        );
        const row = res.rows[0];
        if (!row?.area_m2) return { ok: false, detail: 'No geofence with computable area' };
        const area = Number(row.area_m2);
        return {
          ok: Number.isFinite(area) && area > 0,
          detail: `${row.fence_name ?? 'fence'} ≈ ${Math.round(area).toLocaleString()} m²`,
        };
      })
    );

    if (probeDate) {
      checks.push(
        await timedCheck(`ST_Within probe (${probeDate})`, async () => {
          const res = await pool.query<{
            day_points: string;
            mapped: string;
            inside_fence: string;
          }>(
            `WITH day_pts AS (
               SELECT id, geom, geofence_id, geofence_mapped
               FROM tbl_tracking
               WHERE geom IS NOT NULL
                 AND position_time_nz IS NOT NULL
                 AND position_time_nz::date = $1::date
             ),
             containment AS (
               SELECT d.id,
                      EXISTS (
                        SELECT 1
                        FROM tbl_geofences g
                        WHERE g.geom IS NOT NULL
                          AND NOT ST_IsEmpty(g.geom)
                          AND ST_Within(
                                ST_Force2D(ST_SetSRID(d.geom, 4326)),
                                ST_Force2D(ST_SetSRID(g.geom, 4326))
                              )
                      ) AS inside_any
               FROM day_pts d
             )
             SELECT
               (SELECT COUNT(*)::text FROM day_pts) AS day_points,
               (SELECT COUNT(*)::text FROM day_pts WHERE geofence_mapped IS TRUE) AS mapped,
               (SELECT COUNT(*)::text FROM containment WHERE inside_any) AS inside_fence`,
            [probeDate]
          );
          const r = res.rows[0];
          const dayPoints = Number(r?.day_points ?? 0);
          const inside = Number(r?.inside_fence ?? 0);
          const mapped = Number(r?.mapped ?? 0);
          return {
            ok: dayPoints === 0 || inside > 0,
            detail: `${dayPoints.toLocaleString()} GPS points on ${probeDate}; ${inside.toLocaleString()} ST_Within any fence; ${mapped.toLocaleString()} geofence_mapped=true`,
          };
        })
      );
    } else {
      checks.push({
        name: 'ST_Within probe',
        ok: false,
        detail: 'No probe date — no tbl_tracking rows with geom + position_time_nz',
        ms: 0,
      });
    }

    if (probeDate) {
      fenceSamples = await loadFenceSamples(pool, probeDate);
      if (fenceSamples.length === 0) {
        fenceSamples = await loadFenceSamples(pool, null);
      }
      checks.push(
        await timedCheck('Real fence sample (tbl_tracking)', async () => {
          if (fenceSamples.length === 0) {
            return {
              ok: false,
              detail: 'No geofence_mapped rows with geom found in tbl_tracking',
            };
          }
          const allOk = fenceSamples.every((s) => s.withinStoredFence && s.matchesComputed);
          return {
            ok: allOk,
            detail: formatFenceSampleSummary(fenceSamples),
          };
        })
      );
    } else {
      checks.push({
        name: 'Real fence sample (tbl_tracking)',
        ok: false,
        detail: 'No probe date — cannot pick sample tracking rows',
        ms: 0,
      });
    }

    const allOk = checks.every((c) => c.ok);
    return {
      ok: allOk,
      error: allOk ? null : 'One or more geom checks failed',
      durationMs: Date.now() - started,
      connection: safeConnectionSummary(cfg),
      checks,
      probeDate,
      fenceSamples,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: false,
      error: message,
      durationMs: Date.now() - started,
      connection: safeConnectionSummary(cfg),
      checks,
      probeDate: null,
      fenceSamples,
    };
  } finally {
    await pool.end().catch(() => undefined);
  }
}
