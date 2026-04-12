import { NextResponse } from 'next/server';
import {
  runGpsHarvestForDistanceId,
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
  DEFAULT_HARVEST_WINDOW_MINUTES,
} from '@/lib/distances-gps-harvest';
import { query } from '@/lib/db';

/**
 * POST: Run GPS harvest for many distance rows.
 * Body: { winery?: string, vineyard?: string, maxDistances?: number, maxJobs?: number }
 * - Filters are case-insensitive substring match (same as GET /api/admin/distances)
 * - Always persists results (writes tbl_distances_gps_samples + updates tbl_distances rollup)
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      winery?: string;
      vineyard?: string;
      maxDistances?: number;
      maxJobs?: number;
      maxSuccesses?: number;
      startLessMinutes?: number;
      endPlusMinutes?: number;
      windowMinutes?: number;
    };

    const wineryQ = String(body.winery ?? '').trim();
    const vineyardQ = String(body.vineyard ?? '').trim();
    const maxDistancesRaw = body.maxDistances == null ? null : Number(body.maxDistances);
    const maxDistances =
      maxDistancesRaw == null || !Number.isFinite(maxDistancesRaw) || maxDistancesRaw <= 0
        ? null
        : Math.floor(maxDistancesRaw);

    const ids = await query<{ id: number; delivery_winery: string; vineyard_name: string }>(
      `SELECT id, delivery_winery, vineyard_name
       FROM tbl_distances
       WHERE ($1::text = '' OR strpos(lower(delivery_winery), lower($1)) > 0)
         AND ($2::text = '' OR strpos(lower(vineyard_name), lower($2)) > 0)
       ORDER BY delivery_winery, vineyard_name`,
      [wineryQ, vineyardQ]
    );

    const picked = maxDistances == null ? ids : ids.slice(0, maxDistances);

    const results: Array<{
      id: number;
      delivery_winery: string;
      vineyard_name: string;
      ok: boolean;
      jobsPolled?: number;
      successCount?: number;
      failedCount?: number;
      insertedOrUpdated?: number;
      message?: string;
      error?: string;
    }> = [];

    let ran = 0;
    let okCount = 0;
    let errCount = 0;
    let totalInsertedOrUpdated = 0;

    for (const r of picked) {
      ran++;
      try {
        const out = await runGpsHarvestForDistanceId(r.id, {
          persist: true,
          resetExistingSamples: true,
          startLessMinutes: body.startLessMinutes ?? DEFAULT_HARVEST_START_LESS_MINUTES,
          endPlusMinutes: body.endPlusMinutes ?? DEFAULT_HARVEST_END_PLUS_MINUTES,
          windowMinutes: body.windowMinutes ?? DEFAULT_HARVEST_WINDOW_MINUTES,
          maxJobs: body.maxJobs,
          maxSuccesses: body.maxSuccesses,
        });
        okCount++;
        totalInsertedOrUpdated += out.insertedOrUpdated ?? 0;
        results.push({
          id: r.id,
          delivery_winery: r.delivery_winery,
          vineyard_name: r.vineyard_name,
          ok: true,
          jobsPolled: out.jobsPolled,
          successCount: out.successCount,
          failedCount: out.failedCount,
          insertedOrUpdated: out.insertedOrUpdated,
          message: out.message,
        });
      } catch (e) {
        errCount++;
        results.push({
          id: r.id,
          delivery_winery: r.delivery_winery,
          vineyard_name: r.vineyard_name,
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        });
      }
    }

    return NextResponse.json({
      filter: { winery: wineryQ, vineyard: vineyardQ },
      requested: ids.length,
      ran,
      okCount,
      errCount,
      totalInsertedOrUpdated,
      results,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances_gps_samples" does not exist|column .* does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'GPS samples table or columns missing. Run web/sql/create_tbl_distances_gps_samples.sql, web/sql/alter_tbl_distances_gps_samples_outcomes.sql (if upgrading), and web/sql/add_gps_avg_tbl_distances.sql.',
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

