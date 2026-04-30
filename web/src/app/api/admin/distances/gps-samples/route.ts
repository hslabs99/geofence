import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET ?distanceId= — all harvest rows (success + failed) with debug_json */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const distanceId = Number(searchParams.get('distanceId'));
    if (!Number.isFinite(distanceId) || distanceId <= 0) {
      return NextResponse.json({ error: 'distanceId is required' }, { status: 400 });
    }

    const rows = await query<{
      id: string;
      job_id: string;
      outcome: string | null;
      failure_reason: string | null;
      winery_tracking_id: string | null;
      vineyard_tracking_id: string | null;
      meters: string | null;
      minutes: string | null;
      segment_point_count: number | null;
      debug_json: unknown;
      run_index: number | null;
      created_at: string;
      updated_at: string;
      gps_plus_meters: string | null;
      gps_plus_minutes: string | null;
      gps_plus_outcome: string | null;
    }>(
      `SELECT id::text, job_id::text,
              COALESCE(outcome, 'success') AS outcome,
              failure_reason,
              winery_tracking_id::text, vineyard_tracking_id::text,
              meters::text, minutes::text, segment_point_count, debug_json, run_index,
              to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
              to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
              gps_plus_meters::text, gps_plus_minutes::text, gps_plus_outcome
       FROM tbl_distances_gps_samples
       WHERE distance_id = $1
       ORDER BY COALESCE(run_index, 9999), updated_at DESC`,
      [distanceId]
    );

    return NextResponse.json({
      samples: rows.map((r) => ({
        id: r.id,
        job_id: r.job_id,
        outcome: r.outcome ?? 'success',
        failure_reason: r.failure_reason,
        winery_tracking_id: r.winery_tracking_id,
        vineyard_tracking_id: r.vineyard_tracking_id,
        meters: r.meters != null && r.meters !== '' ? Number(r.meters) : null,
        minutes: r.minutes != null && r.minutes !== '' ? Number(r.minutes) : null,
        segment_point_count: r.segment_point_count,
        debug_json: r.debug_json,
        run_index: r.run_index,
        created_at: r.created_at,
        updated_at: r.updated_at,
        gps_plus_meters:
          r.gps_plus_meters != null && r.gps_plus_meters !== '' ? Number(r.gps_plus_meters) : null,
        gps_plus_minutes:
          r.gps_plus_minutes != null && r.gps_plus_minutes !== '' ? Number(r.gps_plus_minutes) : null,
        gps_plus_outcome: r.gps_plus_outcome,
      })),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances_gps_samples" does not exist/i.test(msg)) {
      return NextResponse.json(
        { error: 'Table tbl_distances_gps_samples does not exist.', samples: [] },
        { status: 500 }
      );
    }
    if (/column .*outcome.*does not exist|column "outcome" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'GPS samples table is missing outcome columns. Run web/sql/alter_tbl_distances_gps_samples_outcomes.sql (or recreate from create_tbl_distances_gps_samples.sql).',
          samples: [],
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
