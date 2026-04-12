import { NextResponse } from 'next/server';
import {
  runGpsHarvestForDistanceId,
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
  DEFAULT_HARVEST_WINDOW_MINUTES,
} from '@/lib/distances-gps-harvest';

/** POST: { distanceId, persist?: boolean, dryRun?: boolean, resetExistingSamples?: boolean, startLessMinutes?, endPlusMinutes?, windowMinutes?, maxJobs?, maxSuccesses? } */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      distanceId?: number;
      persist?: boolean;
      dryRun?: boolean;
      resetExistingSamples?: boolean;
      startLessMinutes?: number;
      endPlusMinutes?: number;
      windowMinutes?: number;
      maxJobs?: number;
      maxSuccesses?: number;
    };
    const distanceId = Number(body.distanceId);
    if (!Number.isFinite(distanceId) || distanceId <= 0) {
      return NextResponse.json({ error: 'distanceId is required' }, { status: 400 });
    }
    const persist = body.dryRun === true ? false : body.persist !== false;
    const resetExistingSamples =
      persist && body.dryRun !== true ? body.resetExistingSamples !== false : false;

    const result = await runGpsHarvestForDistanceId(distanceId, {
      persist,
      resetExistingSamples,
      startLessMinutes: body.startLessMinutes ?? DEFAULT_HARVEST_START_LESS_MINUTES,
      endPlusMinutes: body.endPlusMinutes ?? DEFAULT_HARVEST_END_PLUS_MINUTES,
      windowMinutes: body.windowMinutes ?? DEFAULT_HARVEST_WINDOW_MINUTES,
      maxJobs: body.maxJobs,
      maxSuccesses: body.maxSuccesses,
    });
    return NextResponse.json(result);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances_gps_samples" does not exist|column .* does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'GPS samples table or columns missing. ' +
            'Run web/sql/create_tbl_distances_gps_samples.sql, web/sql/alter_tbl_distances_gps_samples_outcomes.sql (if upgrading), and web/sql/add_gps_avg_tbl_distances.sql.',
          detail: msg,
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
