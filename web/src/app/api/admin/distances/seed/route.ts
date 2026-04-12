import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';

/**
 * POST: sync tbl_distances with tbl_vworkjobs distinct pairs.
 * 1) DELETE rows whose (delivery_winery, vineyard_name) no longer appears on any job
 *    (match: lower(trim) both sides; same idea as list/harvest). CASCADE removes gps_samples.
 * 2) INSERT distinct trimmed pairs from tbl_vworkjobs that are not yet in tbl_distances
 *    (ON CONFLICT DO NOTHING). Does not UPDATE distance_m / duration_min on existing rows.
 */
export async function POST() {
  const client = await getClient();
  try {
    await client.query('BEGIN');

    const delRes = await client.query(
      `DELETE FROM tbl_distances d
       WHERE NOT EXISTS (
         SELECT 1
         FROM tbl_vworkjobs v
         WHERE lower(trim(COALESCE(v.delivery_winery, ''))) = lower(trim(COALESCE(d.delivery_winery, '')))
           AND lower(trim(COALESCE(v.vineyard_name, ''))) = lower(trim(COALESCE(d.vineyard_name, '')))
           AND trim(COALESCE(v.delivery_winery, '')) <> ''
           AND trim(COALESCE(v.vineyard_name, '')) <> ''
       )`
    );
    const deleted = delRes.rowCount ?? 0;

    const insRes = await client.query(
      `INSERT INTO tbl_distances (delivery_winery, vineyard_name, distance_m, duration_min)
       SELECT DISTINCT
         trim(delivery_winery) AS delivery_winery,
         trim(vineyard_name) AS vineyard_name,
         0::integer,
         0::numeric
       FROM tbl_vworkjobs
       WHERE delivery_winery IS NOT NULL AND trim(delivery_winery) <> ''
         AND vineyard_name IS NOT NULL AND trim(vineyard_name) <> ''
       ON CONFLICT (delivery_winery, vineyard_name) DO NOTHING`
    );
    const inserted = insRes.rowCount ?? 0;

    await client.query('COMMIT');
    return NextResponse.json({ ok: true, inserted, deleted });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
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
  } finally {
    client.release();
  }
}
