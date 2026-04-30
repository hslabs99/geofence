/**
 * One-off: why Matador EXIT ~10:08 not used for job 49853973 — read-only DB probe.
 * Run: node scripts/debug-job-49853973.mjs (from web/, loads .env.local)
 */
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import pg from 'pg';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, '..', '.env.local');
const env = fs.readFileSync(envPath, 'utf8');
for (const line of env.split(/\r?\n/)) {
  const m = line.match(/^DATABASE_URL=(.+)$/);
  if (m) {
    process.env.DATABASE_URL = m[1].trim().replace(/^["']|["']$/g, '');
    break;
  }
}
if (!process.env.DATABASE_URL) {
  console.error('No DATABASE_URL in .env.local');
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const jid = '49853973';

try {
  const job = await pool.query(
    `SELECT job_id, worker, truck_id, actual_start_time, actual_end_time,
            vineyard_name, delivery_winery, step1oride, step_1_completed_at,
            to_char(NULLIF(TRIM(COALESCE(step1oride::text, '')), '')::timestamp, 'YYYY-MM-DD HH24:MI:SS') AS step1oride_nz,
            to_char(actual_start_time::timestamp, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_nz
     FROM tbl_vworkjobs WHERE job_id::text = $1 LIMIT 1`,
    [jid]
  );
  const row = job.rows[0];
  console.log('--- JOB', jid, '---');
  console.log(JSON.stringify(row, null, 2));
  const worker = row?.worker != null ? String(row.worker).trim() : '';
  const truck = row?.truck_id != null ? String(row.truck_id).trim() : '';
  const deviceApi = worker || truck;
  console.log('\nDerived-steps uses device = worker || truck_id =>', JSON.stringify(deviceApi));

  const actualStart = row?.actual_start_time;
  const startLess = 10; // Inspect default startLessMinutes (see inspect/page.tsx useState(10))
  console.log('\nUsing startLessMinutes =', startLess, '(Inspect default; refetch steps matches)');

  // Matador fence ids for vineyard mapping
  const vname = row?.vineyard_name ? String(row.vineyard_name).trim() : '';
  const maps = await pool.query(
    `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard'
     AND (LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1)) OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1)))`,
    [vname]
  );
  const names = [vname, ...maps.rows.flatMap((r) => [r.vwname, r.gpsname].filter(Boolean))];
  const uniq = [...new Set(names.map((n) => String(n).trim()).filter(Boolean))];
  const fences = await pool.query(
    `SELECT fence_id, fence_name FROM tbl_geofences WHERE fence_name = ANY($1::text[])`,
    [uniq]
  );
  console.log('\nVineyard name:', vname);
  console.log('Mapped fence_ids for vineyard:', fences.rows);

  const fenceIds = fences.rows.map((r) => r.fence_id);
  // Row user cited: 17-HBT Matador EXIT — find by job's actual_start date if possible
  const track = await pool.query(
    `SELECT t.id, t.device_name, t.geofence_id, t.geofence_type,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
            g.fence_name
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
     WHERE t.device_name = '17-HBT'
       AND t.geofence_type = 'EXIT'
       AND g.fence_name ILIKE '%matador%'
       AND t.position_time_nz >= ($1::timestamp - interval '2 hours')
       AND t.position_time_nz <= ($1::timestamp + interval '14 hours')
     ORDER BY t.position_time_nz
     LIMIT 20`,
    [actualStart]
  );
  console.log('\n17-HBT Matador EXIT rows around job actual_start:', track.rows);

  if (track.rows[0] && fenceIds.length) {
    const gid = track.rows[0].geofence_id;
    console.log('\nTarget row geofence_id', gid, 'in vineyard fence id list?', fenceIds.includes(Number(gid)));
  }

  const endPlus = 60;
  const win = await pool.query(
    `WITH j AS (
       SELECT actual_start_time::timestamp AS ast,
              actual_end_time::timestamp AS aet
       FROM tbl_vworkjobs WHERE job_id::text = $1
     )
     SELECT
       to_char(j.ast - ($2::text || ' minutes')::interval, 'YYYY-MM-DD HH24:MI:SS') AS position_after,
       to_char(COALESCE(j.aet, j.ast) + ($3::text || ' minutes')::interval, 'YYYY-MM-DD HH24:MI:SS') AS position_before
     FROM j`,
    [jid, String(startLess), String(endPlus)]
  );
  const positionAfter = win.rows[0]?.position_after;
  const positionBefore = win.rows[0]?.position_before;
  console.log('\nComputed window (like runFetchStepsForJobs): positionAfter =', positionAfter, '| positionBefore =', positionBefore);

  if (deviceApi && fenceIds.length && positionAfter) {
    const q = await pool.query(
      `SELECT id, to_char(position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS tnz, geofence_id
       FROM tbl_tracking
       WHERE device_name = $1
         AND geofence_id = ANY($2::int[])
         AND geofence_type = 'EXIT'
         AND position_time_nz > $3::timestamp
         AND position_time_nz < $4::timestamp
       ORDER BY position_time_nz ASC
       LIMIT 8`,
      [deviceApi, fenceIds, positionAfter, positionBefore]
    );
    console.log('\nFirst vineyard EXIT rows (same filters as getFirstTracking window, step3 path uses vineyardBefore ~ this):');
    console.log(q.rows);
    const hit = q.rows.find((r) => String(r.tnz).includes('10:08'));
    console.log('\n10:08 row in that result set?', hit ? 'YES id=' + hit.id : 'NO');
  } else {
    console.log('\nSkip simulate: missing device or fence ids or positionAfter');
  }

  // --- Refine-after-winery-exit path (derived-steps fetchGpsStepCandidates) ---
  const deliveryWinery = row?.delivery_winery ? String(row.delivery_winery).trim() : '';
  const wmaps = await pool.query(
    `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Winery'
     AND (LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1)) OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1)))`,
    [deliveryWinery]
  );
  const wnames = [deliveryWinery, ...wmaps.rows.flatMap((r) => [r.vwname, r.gpsname].filter(Boolean))];
  const wuniq = [...new Set(wnames.map((n) => String(n).trim()).filter(Boolean))];
  const wfences = await pool.query(`SELECT fence_id, fence_name FROM tbl_geofences WHERE fence_name = ANY($1::text[])`, [wuniq]);
  const wineryIds = wfences.rows.map((r) => r.fence_id);
  const anchorStr = row?.step1oride_nz || row?.actual_start_nz || '';
  const paStr = positionAfter;
  const twSql = await pool.query(
    `SELECT LEAST($1::timestamp, $2::timestamp) AS tw`,
    [paStr, anchorStr || paStr]
  );
  const trackingWindowAfter = twSql.rows[0]?.tw;
  console.log('\n--- Refine path (approx) ---');
  console.log('positionAfter (raw NZ string from window):', positionAfter);
  console.log('anchor (step1oride NZ from DB to_char):', anchorStr);
  console.log('trackingWindowAfter (min):', trackingWindowAfter);

  if (deviceApi && wineryIds.length && fenceIds.length && trackingWindowAfter && positionBefore) {
    const step2First = await pool.query(
      `SELECT to_char(position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS t FROM tbl_tracking
       WHERE device_name = $1 AND geofence_id = ANY($2::int[]) AND geofence_type = 'ENTER'
         AND position_time_nz > $3::timestamp AND position_time_nz < $4::timestamp
       ORDER BY position_time_nz ASC LIMIT 3`,
      [deviceApi, fenceIds, trackingWindowAfter, positionBefore]
    );
    console.log('First 3 vineyard ENTER after trackingWindowAfter:', step2First.rows);

    const step1Cap = await pool.query(
      `SELECT to_char(position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS t, geofence_id FROM tbl_tracking
       WHERE device_name = $1 AND geofence_id = ANY($2::int[]) AND geofence_type = 'EXIT'
         AND position_time_nz > $3::timestamp AND position_time_nz < $4::timestamp
       ORDER BY position_time_nz ASC LIMIT 1`,
      [deviceApi, wineryIds, trackingWindowAfter, positionBefore]
    );
    const step1Val = step1Cap.rows[0]?.t;
    console.log('First winery EXIT (morning leave) in window:', step1Cap.rows[0] || null);

    if (step1Val) {
      const lowerEnter = await pool.query(`SELECT GREATEST($1::timestamp, $2::timestamp) AS lb`, [
        trackingWindowAfter,
        step1Val,
      ]);
      const lb = lowerEnter.rows[0]?.lb;
      const r2 = await pool.query(
        `SELECT to_char(position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS t FROM tbl_tracking
         WHERE device_name = $1 AND geofence_id = ANY($2::int[]) AND geofence_type = 'ENTER'
           AND position_time_nz > $3::timestamp AND position_time_nz < $4::timestamp
         ORDER BY position_time_nz ASC LIMIT 1`,
        [deviceApi, fenceIds, lb, positionBefore]
      );
      const ent = r2.rows[0]?.t;
      console.log('Refined: first vineyard ENTER after max(tw, winery EXIT) =', ent);
      if (ent) {
        const r3 = await pool.query(
          `SELECT to_char(position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS t, id FROM tbl_tracking
           WHERE device_name = $1 AND geofence_id = ANY($2::int[]) AND geofence_type = 'EXIT'
             AND position_time_nz > $3::timestamp AND position_time_nz < $4::timestamp
           ORDER BY position_time_nz ASC LIMIT 3`,
          [deviceApi, fenceIds, ent, positionBefore]
        );
        console.log('Refined: vineyard EXIT(s) after that ENTER (first 3):', r3.rows);
      }
    }
  }
} finally {
  await pool.end();
}
