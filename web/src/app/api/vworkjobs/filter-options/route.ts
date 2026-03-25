import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** Read first string column from each row (handles pg lowercase keys). */
function rowsToSortedDistinct(rows: unknown): string[] {
  if (!Array.isArray(rows)) return [];
  const seen = new Set<string>();
  for (const r of rows) {
    const row = r as Record<string, unknown>;
    const raw = row.val ?? row.v;
    if (raw == null) continue;
    const s = String(raw).trim();
    if (s) seen.add(s);
  }
  return [...seen].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

async function distinctTextColumn(qualifiedCol: string): Promise<string[]> {
  const sql = `
    SELECT DISTINCT btrim(${qualifiedCol}::text) AS val
    FROM tbl_vworkjobs
    WHERE ${qualifiedCol} IS NOT NULL AND btrim(${qualifiedCol}::text) <> ''
    ORDER BY 1`;
  try {
    const rows = await query(sql);
    return rowsToSortedDistinct(rows);
  } catch {
    return [];
  }
}

/** Distinct filter values for Inspect / tagging (not limited to current page). */
export async function GET() {
  try {
    const [truckRows, tmRows, vineyardNames, deliveryWineries, workers] = await Promise.all([
      query<{ v: string }>(
        `SELECT DISTINCT btrim(truck_id::text) AS v FROM tbl_vworkjobs
         WHERE truck_id IS NOT NULL AND btrim(truck_id::text) <> ''
         ORDER BY 1`,
      ).catch(() => []),
      query<{ v: string }>(
        `SELECT DISTINCT btrim(COALESCE(trailermode::text, trailertype::text, '')) AS v FROM tbl_vworkjobs
         WHERE btrim(COALESCE(trailermode::text, trailertype::text, '')) <> ''
         ORDER BY 1`,
      ).catch(() => []),
      distinctTextColumn('vineyard_name'),
      distinctTextColumn('delivery_winery'),
      distinctTextColumn('worker'),
    ]);

    const truckIds = rowsToSortedDistinct(truckRows);
    const trailermodes = rowsToSortedDistinct(tmRows);

    return NextResponse.json({
      truckIds,
      trailermodes,
      vineyardNames,
      deliveryWineries,
      workers,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
