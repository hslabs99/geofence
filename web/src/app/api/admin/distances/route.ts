import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

export type DistanceRow = {
  id: number;
  delivery_winery: string;
  vineyard_name: string;
  distance_m: number | null;
  duration_min: string | null;
  /** Rows in tbl_vworkjobs matching this pair (trim + lower); not stored on tbl_distances. */
  vwork_job_count?: number | null;
  gps_sample_count?: number | null;
  gps_attempt_count?: number | null;
  gps_avg_distance_m?: number | null;
  gps_avg_duration_min?: string | null;
};

export type GpsMappingRow = { vwname: string | null; gpsname: string | null };

/** Same name-matching rule as derived steps / getFenceIdsForVworkNameWithDebug. */
async function mappingsByVworkName(
  type: 'Winery' | 'Vineyard',
  names: string[]
): Promise<Record<string, GpsMappingRow[]>> {
  const unique = [...new Set(names.map((n) => n.trim()).filter(Boolean))];
  const out: Record<string, GpsMappingRow[]> = {};
  for (const n of unique) out[n] = [];
  if (unique.length === 0) return out;

  const rows = await query<{ vwname: string | null; gpsname: string | null; vwork_name: string }>(
    `WITH names AS (
       SELECT DISTINCT trim(x) AS nm FROM unnest($1::text[]) AS x
       WHERE trim(x) IS NOT NULL AND trim(x) <> ''
     )
     SELECT m.vwname, m.gpsname, n.nm AS vwork_name
     FROM names n
     INNER JOIN tbl_gpsmappings m ON m.type = $2
       AND (
         lower(trim(coalesce(m.vwname, ''))) = lower(n.nm)
         OR lower(trim(coalesce(m.gpsname, ''))) = lower(n.nm)
       )
     ORDER BY n.nm, m.vwname, m.gpsname`,
    [unique, type]
  );
  for (const r of rows) {
    const key = r.vwork_name;
    if (!out[key]) out[key] = [];
    out[key].push({ vwname: r.vwname, gpsname: r.gpsname });
  }
  return out;
}

/**
 * GET: filterable rows + distinct winery/vineyard names for filter UI.
 * Query: ?winery= & ?vineyard= — case-insensitive substring match (empty = no filter on that axis).
 * Includes gpsWineryMappings / gpsVineyardMappings (tbl_gpsmappings) keyed by exact delivery_winery / vineyard_name.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const wineryQ = searchParams.get('winery')?.trim() ?? '';
    const vineyardQ = searchParams.get('vineyard')?.trim() ?? '';

    const [wineryOptions, vineyardOptions, rows] = await Promise.all([
      query<{ delivery_winery: string }>(
        `SELECT DISTINCT delivery_winery FROM tbl_distances ORDER BY delivery_winery`
      ),
      query<{ vineyard_name: string }>(
        `SELECT DISTINCT vineyard_name FROM tbl_distances ORDER BY vineyard_name`
      ),
      query<{
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
      }>(
        `SELECT d.id, d.delivery_winery, d.vineyard_name, d.distance_m,
                duration_min::text AS duration_min,
                COALESCE(vj.cnt, 0) AS vwork_job_count,
                d.gps_sample_count,
                COALESCE(a.gps_attempt_count, 0) AS gps_attempt_count,
                d.gps_avg_distance_m,
                d.gps_avg_duration_min::text AS gps_avg_duration_min
         FROM tbl_distances d
         LEFT JOIN (
           SELECT lower(trim(COALESCE(delivery_winery, ''))) AS dw,
                  lower(trim(COALESCE(vineyard_name, ''))) AS vn,
                  COUNT(*)::int AS cnt
           FROM tbl_vworkjobs
           GROUP BY 1, 2
         ) vj ON vj.dw = lower(trim(COALESCE(d.delivery_winery, '')))
             AND vj.vn = lower(trim(COALESCE(d.vineyard_name, '')))
         LEFT JOIN (
           SELECT distance_id, COUNT(*)::int AS gps_attempt_count
           FROM tbl_distances_gps_samples
           GROUP BY distance_id
         ) a ON a.distance_id = d.id
         WHERE ($1::text = '' OR strpos(lower(delivery_winery), lower($1)) > 0)
           AND ($2::text = '' OR strpos(lower(vineyard_name), lower($2)) > 0)
         ORDER BY delivery_winery, vineyard_name`,
        [wineryQ, vineyardQ]
      ),
    ]);

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

    return NextResponse.json({
      wineryOptions: wineryOptions.map((r) => r.delivery_winery),
      vineyardOptions: vineyardOptions.map((r) => r.vineyard_name),
      gpsWineryMappings,
      gpsVineyardMappings,
      rows: rows.map((c) => ({
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
      })) satisfies DistanceRow[],
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'Table tbl_distances does not exist. Run web/sql/create_tbl_distances.sql on your database.',
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
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
