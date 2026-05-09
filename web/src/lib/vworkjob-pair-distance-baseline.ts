/**
 * Effective winery→vineyard pair metrics from tbl_distances (+ tbl_distances_manual when present).
 * Semantics: distance_m and duration_min are one-way (winery↔vineyard leg). tbl_vworkjobs.distance
 * from Populate vWork is round-trip km: (effective_m / 1000) × 2; tbl_vworkjobs.minutes is one-way.
 */

import { query } from '@/lib/db';

export type PairDistanceContext = {
  pair_label: string;
  effective_distance_m: number | null;
  effective_duration_min: number | null;
  tbl_distance_m: number | null;
  tbl_duration_min: number | null;
  gps_avg_duration_min: number | null;
  manual_override: boolean;
  vwork_distance_round_trip_km: number | null;
  vwork_minutes_one_way: number | null;
};

function numOrNull(v: unknown): number | null {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseDurationText(v: unknown): number | null {
  if (v == null) return null;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

function pickJobDistanceMinutes(row: Record<string, unknown>): {
  vwork_distance_round_trip_km: number | null;
  vwork_minutes_one_way: number | null;
} {
  const get = (...keys: string[]): unknown => {
    for (const k of keys) {
      if (row[k] !== undefined) return row[k];
    }
    return undefined;
  };
  return {
    vwork_distance_round_trip_km: numOrNull(get('distance', 'Distance')),
    vwork_minutes_one_way: numOrNull(get('minutes', 'Minutes')),
  };
}

/** Build distance context for a job row + pair; returns null if winery or vineyard missing or no tbl_distances row. */
export async function pairDistanceContextForJobRow(
  row: Record<string, unknown>,
  deliveryWinery: string | null | undefined,
  vineyardName: string | null | undefined
): Promise<PairDistanceContext | null> {
  const w = deliveryWinery != null ? String(deliveryWinery).trim() : '';
  const v = vineyardName != null ? String(vineyardName).trim() : '';
  if (!w || !v) return null;

  const vm = pickJobDistanceMinutes(row);

  const mapRow = (r: Record<string, unknown>): PairDistanceContext => ({
    pair_label: `${w} → ${v}`,
    effective_distance_m: numOrNull(r.effective_distance_m),
    effective_duration_min: parseDurationText(r.effective_duration_min_t),
    tbl_distance_m: numOrNull(r.tbl_distance_m),
    tbl_duration_min: parseDurationText(r.tbl_duration_min_t),
    gps_avg_duration_min: parseDurationText(r.gps_avg_duration_min_t),
    manual_override: r.manual_override === true || r.manual_override === 1,
    vwork_distance_round_trip_km: vm.vwork_distance_round_trip_km,
    vwork_minutes_one_way: vm.vwork_minutes_one_way,
  });

  const sqlWithManual = `
    SELECT
      d.distance_m AS tbl_distance_m,
      d.duration_min::text AS tbl_duration_min_t,
      d.gps_avg_duration_min::text AS gps_avg_duration_min_t,
      (m.id IS NOT NULL) AS manual_override,
      CASE WHEN m.id IS NOT NULL THEN m.distance_m ELSE d.distance_m END AS effective_distance_m,
      CASE
        WHEN m.id IS NOT NULL AND m.duration_min IS NOT NULL THEN m.duration_min::text
        ELSE d.duration_min::text
      END AS effective_duration_min_t
    FROM tbl_distances d
    LEFT JOIN tbl_distances_manual m
      ON lower(trim(m.delivery_winery)) = lower(trim(d.delivery_winery))
     AND lower(trim(m.vineyard_name)) = lower(trim(d.vineyard_name))
    WHERE lower(trim(d.delivery_winery)) = lower(trim($1))
      AND lower(trim(d.vineyard_name)) = lower(trim($2))
    LIMIT 1`;

  try {
    const rows = await query<Record<string, unknown>>(sqlWithManual, [w, v]);
    const r0 = rows[0];
    if (!r0) return null;
    return mapRow(r0);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (!/tbl_distances_manual/i.test(msg) || !/does not exist/i.test(msg)) throw e;
    const rows = await query<Record<string, unknown>>(
      `SELECT
         d.distance_m AS tbl_distance_m,
         d.duration_min::text AS tbl_duration_min_t,
         d.gps_avg_duration_min::text AS gps_avg_duration_min_t,
         false AS manual_override,
         d.distance_m AS effective_distance_m,
         d.duration_min::text AS effective_duration_min_t
       FROM tbl_distances d
       WHERE lower(trim(d.delivery_winery)) = lower(trim($1))
         AND lower(trim(d.vineyard_name)) = lower(trim($2))
       LIMIT 1`,
      [w, v]
    );
    const r0 = rows[0];
    if (!r0) return null;
    return mapRow(r0);
  }
}
