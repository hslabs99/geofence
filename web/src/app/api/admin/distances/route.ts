import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';
import {
  mappingsByVworkName,
  expandedFenceNameList,
  type GpsMappingRow,
} from '@/lib/gps-mappings-by-vwork-name';
import { fenceCentroidLatLngForFenceNames, googleMapsDrivingDirectionsUrl } from '@/lib/distances-fence-centroids';

export type { GpsMappingRow };

export type DistanceRow = {
  id: number;
  delivery_winery: string;
  vineyard_name: string;
  distance_m: number | null;
  duration_min: string | null;
  /** Rows in tbl_vworkjobs matching this pair (lower(trim) on both sides); not stored on tbl_distances. */
  vwork_job_count?: number | null;
  gps_sample_count?: number | null;
  gps_attempt_count?: number | null;
  gps_avg_distance_m?: number | null;
  gps_avg_duration_min?: string | null;
  gps_plus_sample_count?: number | null;
  gps_plus_avg_distance_m?: number | null;
  gps_plus_avg_duration_min?: string | null;
  /** GPSTAGS | GPS+ | FAIL — set by GPS harvest rollup. */
  distance_via?: string | null;
  /** Distinct tbl_vworkjobs.customer values for this pair (comma-separated). */
  pair_clients?: string | null;
  manual_distance_m?: number | null;
  manual_duration_min?: string | null;
  manual_notes?: string | null;
  /** COALESCE(manual, tbl_distances) for metres. */
  effective_distance_m?: number | null;
  /** COALESCE(manual minutes when set, else tbl_distances). */
  effective_duration_min?: string | null;
  /** MANUAL when a manual row exists; else distance_via from rollup. */
  display_distance_via?: string | null;
  /** Google Maps driving directions between fence centroids (when both resolve). */
  maps_drive_url?: string | null;
};

async function centroidByVworkNames(
  uniqueNames: string[],
  gpsMaps: Record<string, GpsMappingRow[]>
): Promise<Record<string, { lat: number; lng: number } | null>> {
  const out: Record<string, { lat: number; lng: number } | null> = {};
  const uniq = [...new Set(uniqueNames.map((n) => n.trim()).filter(Boolean))];
  const chunk = 14;
  for (let i = 0; i < uniq.length; i += chunk) {
    const slice = uniq.slice(i, i + chunk);
    await Promise.all(
      slice.map(async (nm) => {
        const list = expandedFenceNameList(nm, gpsMaps[nm] ?? []);
        out[nm] = await fenceCentroidLatLngForFenceNames(list);
      })
    );
  }
  return out;
}

/**
 * GET: filterable rows + distinct winery/vineyard names for filter UI.
 * Query params: winery, vineyard, client (each optional; partial case-insensitive match; client matches pair_clients).
 * Response includes customerOptions: distinct tbl_vworkjobs.customer for the Client filter dropdown.
 * List query uses WITH dist_base AS (...) so alias `d` exists only inside the CTE; outer SELECT uses `b` only.
 */
const DISTANCES_CTE_CORE = `SELECT
           d.id,
           d.delivery_winery,
           d.vineyard_name,
           d.distance_m,
           d.duration_min::text AS duration_min,
           COALESCE(vj.cnt, 0) AS vwork_job_count,
           d.gps_sample_count,
           COALESCE(a.gps_attempt_count, 0) AS gps_attempt_count,
           d.gps_avg_distance_m,
           d.gps_avg_duration_min::text AS gps_avg_duration_min,
           d.gps_plus_sample_count,
           d.gps_plus_avg_distance_m,
           d.gps_plus_avg_duration_min::text AS gps_plus_avg_duration_min,
           d.distance_via
         FROM tbl_distances d
         LEFT JOIN (
           SELECT lower(trim(COALESCE(v.delivery_winery, ''))) AS dw,
                  lower(trim(COALESCE(v.vineyard_name, ''))) AS vn,
                  COUNT(*)::int AS cnt
           FROM tbl_vworkjobs v
           GROUP BY 1, 2
         ) vj ON vj.dw = lower(trim(COALESCE(d.delivery_winery, '')))
             AND vj.vn = lower(trim(COALESCE(d.vineyard_name, '')))
         LEFT JOIN (
           SELECT distance_id, COUNT(*)::int AS gps_attempt_count
           FROM tbl_distances_gps_samples
           GROUP BY distance_id
         ) a ON a.distance_id = d.id
         WHERE ($1::text = '' OR strpos(lower(d.delivery_winery), lower($1)) > 0)
           AND ($2::text = '' OR strpos(lower(d.vineyard_name), lower($2)) > 0)`;

const SQL_LIST_MANUAL_AND_CUSTOMER = `WITH dist_base AS (
${DISTANCES_CTE_CORE}
)
SELECT
  b.id,
  b.delivery_winery,
  b.vineyard_name,
  b.distance_m,
  b.duration_min,
  b.vwork_job_count,
  b.gps_sample_count,
  b.gps_attempt_count,
  b.gps_avg_distance_m,
  b.gps_avg_duration_min,
  b.gps_plus_sample_count,
  b.gps_plus_avg_distance_m,
  b.gps_plus_avg_duration_min,
  b.distance_via,
  cust.pair_clients,
  m.distance_m AS manual_distance_m,
  m.duration_min::text AS manual_duration_min,
  m.notes AS manual_notes,
  CASE WHEN m.id IS NOT NULL THEN m.distance_m ELSE b.distance_m END AS effective_distance_m,
  CASE
    WHEN m.id IS NOT NULL AND m.duration_min IS NOT NULL THEN m.duration_min::text
    ELSE b.duration_min
  END AS effective_duration_min,
  CASE WHEN m.id IS NOT NULL THEN 'MANUAL'::text ELSE b.distance_via END AS display_distance_via
FROM dist_base b
LEFT JOIN tbl_distances_manual m
  ON lower(trim(m.delivery_winery)) = lower(trim(b.delivery_winery))
 AND lower(trim(m.vineyard_name)) = lower(trim(b.vineyard_name))
LEFT JOIN (
  SELECT lower(trim(COALESCE(v.delivery_winery, ''))) AS dw,
         lower(trim(COALESCE(v.vineyard_name, ''))) AS vn,
         string_agg(DISTINCT trim(COALESCE(v.customer::text, '')), ', ') AS pair_clients
  FROM tbl_vworkjobs v
  WHERE trim(COALESCE(v.customer::text, '')) <> ''
  GROUP BY 1, 2
) cust ON cust.dw = lower(trim(COALESCE(b.delivery_winery, '')))
      AND cust.vn = lower(trim(COALESCE(b.vineyard_name, '')))
ORDER BY b.delivery_winery, b.vineyard_name`;

const SQL_LIST_NO_MANUAL_CUSTOMER = `WITH dist_base AS (
${DISTANCES_CTE_CORE}
)
SELECT
  b.id,
  b.delivery_winery,
  b.vineyard_name,
  b.distance_m,
  b.duration_min,
  b.vwork_job_count,
  b.gps_sample_count,
  b.gps_attempt_count,
  b.gps_avg_distance_m,
  b.gps_avg_duration_min,
  b.gps_plus_sample_count,
  b.gps_plus_avg_distance_m,
  b.gps_plus_avg_duration_min,
  b.distance_via,
  cust.pair_clients,
  NULL::int AS manual_distance_m,
  NULL::text AS manual_duration_min,
  NULL::text AS manual_notes,
  b.distance_m AS effective_distance_m,
  b.duration_min AS effective_duration_min,
  b.distance_via AS display_distance_via
FROM dist_base b
LEFT JOIN (
  SELECT lower(trim(COALESCE(v.delivery_winery, ''))) AS dw,
         lower(trim(COALESCE(v.vineyard_name, ''))) AS vn,
         string_agg(DISTINCT trim(COALESCE(v.customer::text, '')), ', ') AS pair_clients
  FROM tbl_vworkjobs v
  WHERE trim(COALESCE(v.customer::text, '')) <> ''
  GROUP BY 1, 2
) cust ON cust.dw = lower(trim(COALESCE(b.delivery_winery, '')))
      AND cust.vn = lower(trim(COALESCE(b.vineyard_name, '')))
ORDER BY b.delivery_winery, b.vineyard_name`;

const SQL_LIST_NO_MANUAL_NO_CUSTOMER = `WITH dist_base AS (
${DISTANCES_CTE_CORE}
)
SELECT
  b.id,
  b.delivery_winery,
  b.vineyard_name,
  b.distance_m,
  b.duration_min,
  b.vwork_job_count,
  b.gps_sample_count,
  b.gps_attempt_count,
  b.gps_avg_distance_m,
  b.gps_avg_duration_min,
  b.gps_plus_sample_count,
  b.gps_plus_avg_distance_m,
  b.gps_plus_avg_duration_min,
  b.distance_via,
  NULL::text AS pair_clients,
  NULL::int AS manual_distance_m,
  NULL::text AS manual_duration_min,
  NULL::text AS manual_notes,
  b.distance_m AS effective_distance_m,
  b.duration_min AS effective_duration_min,
  b.distance_via AS display_distance_via
FROM dist_base b
ORDER BY b.delivery_winery, b.vineyard_name`;

const SQL_LIST_MANUAL_NO_CUSTOMER_COL = `WITH dist_base AS (
${DISTANCES_CTE_CORE}
)
SELECT
  b.id,
  b.delivery_winery,
  b.vineyard_name,
  b.distance_m,
  b.duration_min,
  b.vwork_job_count,
  b.gps_sample_count,
  b.gps_attempt_count,
  b.gps_avg_distance_m,
  b.gps_avg_duration_min,
  b.gps_plus_sample_count,
  b.gps_plus_avg_distance_m,
  b.gps_plus_avg_duration_min,
  b.distance_via,
  NULL::text AS pair_clients,
  m.distance_m AS manual_distance_m,
  m.duration_min::text AS manual_duration_min,
  m.notes AS manual_notes,
  CASE WHEN m.id IS NOT NULL THEN m.distance_m ELSE b.distance_m END AS effective_distance_m,
  CASE
    WHEN m.id IS NOT NULL AND m.duration_min IS NOT NULL THEN m.duration_min::text
    ELSE b.duration_min
  END AS effective_duration_min,
  CASE WHEN m.id IS NOT NULL THEN 'MANUAL'::text ELSE b.distance_via END AS display_distance_via
FROM dist_base b
LEFT JOIN tbl_distances_manual m
  ON lower(trim(m.delivery_winery)) = lower(trim(b.delivery_winery))
 AND lower(trim(m.vineyard_name)) = lower(trim(b.vineyard_name))
ORDER BY b.delivery_winery, b.vineyard_name`;

function pgExtras(err: unknown): { code?: string; detail?: string; hint?: string; position?: string } {
  const o = err as { code?: string; detail?: string; hint?: string; position?: string };
  return {
    code: o.code,
    detail: o.detail,
    hint: o.hint,
    position: o.position != null ? String(o.position) : undefined,
  };
}

export async function GET(request: Request) {
  let lastSqlForDebug = '';
  try {
    const { searchParams } = new URL(request.url);
    const wineryQ = searchParams.get('winery')?.trim() ?? '';
    const vineyardQ = searchParams.get('vineyard')?.trim() ?? '';
    const clientQ = searchParams.get('client')?.trim() ?? '';

    type RowDb = {
      id: number;
      delivery_winery: string;
      vineyard_name: string;
      distance_m: number | null;
      duration_min: string | null;
      vwork_job_count: number | null;
      gps_sample_count: number | null;
      gps_attempt_count: number | null;
      gps_avg_distance_m: number | null;
      gps_avg_duration_min: string | null;
      gps_plus_sample_count: number | null;
      gps_plus_avg_distance_m: number | null;
      gps_plus_avg_duration_min: string | null;
      distance_via: string | null;
      pair_clients: string | null;
      manual_distance_m: number | null;
      manual_duration_min: string | null;
      manual_notes: string | null;
      effective_distance_m: number | null;
      effective_duration_min: string | null;
      display_distance_via: string | null;
    };

    let rows: RowDb[];
    try {
      lastSqlForDebug = 'CTE: manual + customer';
      rows = await query<RowDb>(SQL_LIST_MANUAL_AND_CUSTOMER, [wineryQ, vineyardQ]);
    } catch (e) {
      const msg0 = e instanceof Error ? e.message : String(e);
      console.error('[GET /api/admin/distances] query failed:', msg0, pgExtras(e));
      if (/relation "tbl_distances_manual" does not exist/i.test(msg0)) {
        try {
          lastSqlForDebug = 'CTE: no manual table, with customer';
          rows = await query<RowDb>(SQL_LIST_NO_MANUAL_CUSTOMER, [wineryQ, vineyardQ]);
        } catch (e2) {
          const msg1 = e2 instanceof Error ? e2.message : String(e2);
          console.error('[GET /api/admin/distances] fallback query failed:', msg1, pgExtras(e2));
          if (/column.*customer|relation.*tbl_vworkjobs/i.test(msg1)) {
            lastSqlForDebug = 'CTE: no manual, no customer column';
            rows = await query<RowDb>(SQL_LIST_NO_MANUAL_NO_CUSTOMER, [wineryQ, vineyardQ]);
          } else {
            throw e2;
          }
        }
      } else if (/column.*customer/i.test(msg0)) {
        lastSqlForDebug = 'CTE: customer column missing, manual + null clients';
        rows = await query<RowDb>(SQL_LIST_MANUAL_NO_CUSTOMER_COL, [wineryQ, vineyardQ]);
      } else {
        throw e;
      }
    }

    if (clientQ) {
      const q = clientQ.toLowerCase();
      rows = rows.filter((r) => (r.pair_clients ?? '').toLowerCase().includes(q));
    }

    const [wineryOptions, vineyardOptions, customerOptionsRows] = await Promise.all([
      query<{ delivery_winery: string }>(
        `SELECT DISTINCT delivery_winery FROM tbl_distances ORDER BY delivery_winery`
      ),
      query<{ vineyard_name: string }>(
        `SELECT DISTINCT vineyard_name FROM tbl_distances ORDER BY vineyard_name`
      ),
      query<{ customer: string }>(
        `SELECT DISTINCT trim(COALESCE(customer::text, '')) AS customer
         FROM tbl_vworkjobs
         WHERE trim(COALESCE(customer::text, '')) <> ''
         ORDER BY 1`
      ).catch(() => [] as { customer: string }[]),
    ]);
    const customerOptions = customerOptionsRows.map((r) => r.customer);

    const wineryNames = rows.map((r) => r.delivery_winery);
    const vineyardNames = rows.map((r) => r.vineyard_name);
    let gpsWineryMappings: Record<string, GpsMappingRow[]> = {};
    let gpsVineyardMappings: Record<string, GpsMappingRow[]> = {};
    try {
      ;[gpsWineryMappings, gpsVineyardMappings] = await Promise.all([
        mappingsByVworkName('Winery', wineryNames),
        mappingsByVworkName('Vineyard', vineyardNames),
      ]);
    } catch {
      /* tbl_gpsmappings missing or query error — UI still loads distances */
    }

    let wineryCentroids: Record<string, { lat: number; lng: number } | null> = {};
    let vineyardCentroids: Record<string, { lat: number; lng: number } | null> = {};
    try {
      ;[wineryCentroids, vineyardCentroids] = await Promise.all([
        centroidByVworkNames(wineryNames, gpsWineryMappings),
        centroidByVworkNames(vineyardNames, gpsVineyardMappings),
      ]);
    } catch {
      /* PostGIS / tbl_geofences optional */
    }

    return NextResponse.json({
      wineryOptions: wineryOptions.map((r) => r.delivery_winery),
      vineyardOptions: vineyardOptions.map((r) => r.vineyard_name),
      customerOptions,
      gpsWineryMappings,
      gpsVineyardMappings,
      rows: rows.map((c) => {
        const wo = wineryCentroids[c.delivery_winery] ?? null;
        const vo = vineyardCentroids[c.vineyard_name] ?? null;
        const maps_drive_url =
          wo != null && vo != null ? googleMapsDrivingDirectionsUrl(wo, vo) : null;
        return {
          id: c.id,
          delivery_winery: c.delivery_winery,
          vineyard_name: c.vineyard_name,
          distance_m: c.distance_m,
          duration_min: c.duration_min,
          vwork_job_count: c.vwork_job_count ?? 0,
          gps_sample_count: c.gps_sample_count ?? 0,
          gps_attempt_count: c.gps_attempt_count ?? 0,
          gps_avg_distance_m: c.gps_avg_distance_m,
          gps_avg_duration_min: c.gps_avg_duration_min,
          gps_plus_sample_count: c.gps_plus_sample_count ?? 0,
          gps_plus_avg_distance_m: c.gps_plus_avg_distance_m,
          gps_plus_avg_duration_min: c.gps_plus_avg_duration_min,
          distance_via: c.distance_via,
          pair_clients: c.pair_clients,
          manual_distance_m: c.manual_distance_m,
          manual_duration_min: c.manual_duration_min,
          manual_notes: c.manual_notes,
          effective_distance_m: c.effective_distance_m,
          effective_duration_min: c.effective_duration_min,
          display_distance_via: c.display_distance_via,
          maps_drive_url,
        };
      }) satisfies DistanceRow[],
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const extras = pgExtras(err);
    console.error('[GET /api/admin/distances] fatal:', msg, extras, { lastSqlBranch: lastSqlForDebug });
    if (/relation "tbl_distances" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'Table tbl_distances does not exist. Run web/sql/create_tbl_distances.sql on your database.',
        },
        { status: 500 }
      );
    }
    const { searchParams } = new URL(request.url);
    const debugDistances = searchParams.get('debug') === '1';
    const debugBody =
      debugDistances || process.env.NODE_ENV !== 'production'
        ? {
            debug: {
              pgCode: extras.code,
              pgDetail: extras.detail,
              pgHint: extras.hint,
              pgPosition: extras.position,
              lastSqlBranch: lastSqlForDebug,
              tip: 'Open /api/admin/distances?debug=1 in the browser for this JSON in production.',
            },
          }
        : {};
    return NextResponse.json({ error: msg, ...debugBody }, { status: 500 });
  }
}

type PatchBody = {
  id?: number;
  distance_m?: number | null;
  duration_min?: number | null;
};

function parseOptionalInt(v: unknown): number | null | undefined {
  if (v === undefined) return undefined;
  if (v === null || v === '') return null;
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error('distance_m must be a number or empty');
  return Math.round(n);
}

function parseOptionalDuration(v: unknown): number | null | undefined {
  if (v === undefined) return undefined;
  if (v === null || v === '') return null;
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error('duration_min must be a number or empty');
  return n;
}

/** PATCH: update by id (send distance_m and/or duration_min; null clears). */
export async function PATCH(request: Request) {
  try {
    const body = (await request.json()) as PatchBody;
    const id = body.id != null ? Number(body.id) : NaN;
    const hasId = Number.isFinite(id) && id > 0;

    let distance_m: number | null | undefined;
    let duration_min: number | null | undefined;
    try {
      distance_m = parseOptionalInt(body.distance_m);
      duration_min = parseOptionalDuration(body.duration_min);
    } catch (e) {
      const m = e instanceof Error ? e.message : String(e);
      return NextResponse.json({ error: m }, { status: 400 });
    }

    if (!hasId) {
      return NextResponse.json({ error: 'PATCH requires id' }, { status: 400 });
    }

    if (distance_m === undefined && duration_min === undefined) {
      return NextResponse.json(
        { error: 'Send distance_m and/or duration_min to update' },
        { status: 400 }
      );
    }
    const sets: string[] = ['updated_at = now()'];
    const params: unknown[] = [];
    let i = 1;
    if (distance_m !== undefined) {
      sets.push(`distance_m = $${i}`);
      params.push(distance_m);
      i++;
    }
    if (duration_min !== undefined) {
      sets.push(`duration_min = $${i}`);
      params.push(duration_min);
      i++;
    }
    params.push(id);
    const n = await execute(
      `UPDATE tbl_distances SET ${sets.join(', ')} WHERE id = $${i}`,
      params
    );
    if (n === 0) {
      return NextResponse.json({ error: 'No row updated; check id' }, { status: 404 });
    }
    return NextResponse.json({ ok: true, updated: n });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
