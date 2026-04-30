import { query } from '@/lib/db';

export type GpsMappingRow = { vwname: string | null; gpsname: string | null };

/** Same name-matching rule as derived steps / getFenceIdsForVworkNameWithDebug. */
export async function mappingsByVworkName(
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

/** vWork name plus any vwname/gpsname from tbl_gpsmappings rows linked to that name (for geofence name matching). */
export function expandedFenceNameList(vworkName: string, maps: GpsMappingRow[]): string[] {
  const s = new Set<string>();
  const base = vworkName.trim();
  if (base) s.add(base);
  for (const m of maps) {
    const a = (m.vwname ?? '').trim();
    const b = (m.gpsname ?? '').trim();
    if (a) s.add(a);
    if (b) s.add(b);
  }
  return [...s];
}
