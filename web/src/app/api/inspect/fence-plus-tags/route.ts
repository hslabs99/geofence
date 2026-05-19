import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { fencePlusTagsForTrackingIds } from '@/lib/inspect-fence-plus-tags';
import { getStepsPlusSettings } from '@/lib/steps-plus-settings';

const MAX_IDS = 500;

/**
 * POST body: { device: string, vineyardName: string, trackingIds: number[], bufferMeters?: number }
 * Returns { tags: Record<string, string | null>, bufferMeters, fenceNames } for Inspect GPS grid only.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      device?: string;
      vineyardName?: string;
      trackingIds?: unknown;
      bufferMeters?: unknown;
    };
    const device = typeof body.device === 'string' ? body.device.trim() : '';
    const vineyardName = typeof body.vineyardName === 'string' ? body.vineyardName.trim() : '';
    if (!device || !vineyardName) {
      return NextResponse.json({ error: 'device and vineyardName are required' }, { status: 400 });
    }
    const rawIds = Array.isArray(body.trackingIds) ? body.trackingIds : [];
    const trackingIds = rawIds
      .map((x) => (typeof x === 'number' ? x : typeof x === 'string' ? parseInt(String(x), 10) : NaN))
      .filter((n) => Number.isFinite(n) && n > 0);
    if (trackingIds.length === 0) {
      return NextResponse.json({ error: 'trackingIds must be a non-empty array' }, { status: 400 });
    }
    if (trackingIds.length > MAX_IDS) {
      return NextResponse.json({ error: `At most ${MAX_IDS} tracking ids per request` }, { status: 400 });
    }

    const mappings = await query<{ vwname: string | null; gpsname: string | null }>(
      `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard'
       AND (
         LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1::text))
         OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1::text))
       )`,
      [vineyardName]
    );
    const fenceNames: string[] = [vineyardName];
    for (const m of mappings) {
      const gps = (m.gpsname ?? '').trim();
      if (gps && !fenceNames.some((n) => n.toLowerCase() === gps.toLowerCase())) fenceNames.push(gps);
    }

    const settings = await getStepsPlusSettings();
    const bufferMeters =
      typeof body.bufferMeters === 'number' && Number.isFinite(body.bufferMeters) && body.bufferMeters > 0
        ? Math.min(500, body.bufferMeters)
        : settings.bufferMeters;

    const rows = await fencePlusTagsForTrackingIds(fenceNames, bufferMeters, device, trackingIds);
    const tags: Record<string, string | null> = {};
    for (const r of rows) {
      const id = r.id != null ? String(r.id).trim() : '';
      if (id) tags[id] = r.fence_plus != null && String(r.fence_plus).trim() !== '' ? String(r.fence_plus).trim() : null;
    }
    return NextResponse.json({
      tags,
      bufferMeters,
      fenceNames,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
