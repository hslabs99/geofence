import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

type ColumnCheck = {
  table: string;
  column: string;
  exists: boolean;
};

export async function GET() {
  try {
    const [samplesReg, distancesReg] = await Promise.all([
      query<{ reg: string | null }>(`SELECT to_regclass('public.tbl_distances_gps_samples')::text AS reg`),
      query<{ reg: string | null }>(`SELECT to_regclass('public.tbl_distances')::text AS reg`),
    ]);

    const samplesExists = (samplesReg[0]?.reg ?? null) !== null;
    const distancesExists = (distancesReg[0]?.reg ?? null) !== null;

    const checks: ColumnCheck[] = [];

    // Columns required by INSERT/UPDATE into tbl_distances_gps_samples
    const sampleColumns = [
      'distance_id',
      'job_id',
      'outcome',
      'failure_reason',
      'winery_tracking_id',
      'vineyard_tracking_id',
      'meters',
      'minutes',
      'segment_point_count',
      'debug_json',
      'run_index',
      'updated_at',
      'gps_plus_meters',
      'gps_plus_minutes',
      'gps_plus_outcome',
      'gps_plus_vineyard_tracking_id',
    ];
    for (const c of sampleColumns) checks.push({ table: 'tbl_distances_gps_samples', column: c, exists: false });

    // Columns required by rollup update on tbl_distances
    const distColumns = [
      'gps_sample_count',
      'gps_avg_distance_m',
      'gps_avg_duration_min',
      'gps_averaged_at',
      'gps_plus_sample_count',
      'gps_plus_avg_distance_m',
      'gps_plus_avg_duration_min',
      'gps_plus_averaged_at',
      'distance_via',
      'distance_m',
      'duration_min',
      'updated_at',
    ];
    for (const c of distColumns) checks.push({ table: 'tbl_distances', column: c, exists: false });

    const cols = await query<{ table_name: string; column_name: string }>(
      `SELECT table_name, column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name IN ('tbl_distances_gps_samples', 'tbl_distances')`
    );

    const set = new Set(cols.map((r) => `${r.table_name}.${r.column_name}`));
    for (const ch of checks) {
      ch.exists = set.has(`${ch.table}.${ch.column}`);
    }

    return NextResponse.json({
      schema: 'public',
      tables: {
        tbl_distances: { exists: distancesExists, regclass: distancesReg[0]?.reg ?? null },
        tbl_distances_gps_samples: { exists: samplesExists, regclass: samplesReg[0]?.reg ?? null },
      },
      columns: checks,
      missing: checks.filter((c) => !c.exists),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

