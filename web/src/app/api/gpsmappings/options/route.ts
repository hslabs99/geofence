import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET() {
  try {
    const [fenceNames, geofencesRows, trackingCounts, deliveryWineries, vineyardRows] = await Promise.all([
      query<{ fence_name: string | null }>(
        `SELECT DISTINCT fence_name FROM tbl_gpsdata
         WHERE fence_name IS NOT NULL AND fence_name != ''
         ORDER BY fence_name`
      ),
      query<{ fence_id: number; fence_name: string | null }>(
        `SELECT fence_id, fence_name FROM tbl_geofences ORDER BY fence_name`
      ),
      // DataCount per fence_id: count of tbl_tracking rows where geofence_id = tbl_geofences.fence_id
      query<{ fence_id: number; tracking_count: string }>(
        `SELECT g.fence_id, COUNT(t.id)::text AS tracking_count
         FROM tbl_geofences g
         LEFT JOIN tbl_tracking t ON t.geofence_id = g.fence_id
         GROUP BY g.fence_id
         ORDER BY g.fence_id`
      ),
      query<{ delivery_winery: string | null }>(
        `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
         WHERE delivery_winery IS NOT NULL AND delivery_winery != ''
         ORDER BY delivery_winery`
      ),
      query<{ vineyard_name: string; job_count: string }>(
        `SELECT vineyard_name, COUNT(*)::text AS job_count
         FROM tbl_vworkjobs
         WHERE vineyard_name IS NOT NULL AND trim(vineyard_name) <> ''
         GROUP BY vineyard_name
         ORDER BY vineyard_name`
      ),
    ]);
    const trackingCountByFenceId: Record<number, number> = {};
    trackingCounts.forEach((r) => {
      trackingCountByFenceId[r.fence_id] = parseInt(r.tracking_count, 10) || 0;
    });
    const trackingFences = geofencesRows.map((r) => ({
      fence_id: r.fence_id,
      fence_name: (r.fence_name ?? '').trim(),
    }));
    const vineyardNames = vineyardRows.map((r) => r.vineyard_name).filter(Boolean) as string[];
    const vineyardJobCounts: Record<string, number> = {};
    for (const r of vineyardRows) {
      if (r.vineyard_name) {
        vineyardJobCounts[r.vineyard_name] = parseInt(r.job_count, 10) || 0;
      }
    }
    return NextResponse.json({
      fenceNames: fenceNames.map((r) => r.fence_name).filter(Boolean) as string[],
      /** tbl_geofences rows (fence_id, fence_name); no duplicates */
      trackingFences,
      /** Count of tbl_tracking rows per fence_id */
      trackingCountByFenceId,
      deliveryWineries: deliveryWineries.map((r) => r.delivery_winery).filter(Boolean) as string[],
      vineyardNames,
      /** tbl_vworkjobs row count per distinct vineyard_name */
      vineyardJobCounts,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
