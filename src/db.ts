import { Pool, PoolClient } from 'pg';
import { logger } from './logger';

const pool = new Pool({
  host: process.env.PGHOST ?? 'localhost',
  port: parseInt(process.env.PGPORT ?? '5432', 10),
  database: process.env.PGDATABASE ?? 'geodata',
  user: process.env.PGUSER ?? 'geofence',
  password: process.env.PGPASSWORD,
});

pool.on('connect', () => logger.db({ host: process.env.PGHOST }, 'pg pool connected'));
pool.on('error', (err) => logger.db({ error: err.message }, 'pg pool error'));

export function getPool(): Pool {
  return pool;
}

export async function getNextBatchNumber(client: PoolClient): Promise<number> {
  const res = await client.query("SELECT nextval('seq_vwork_batch') as batch");
  return parseInt(res.rows[0].batch, 10);
}

export async function isFileAlreadyProcessed(
  client: PoolClient,
  driveFileId: string
): Promise<boolean> {
  const res = await client.query(
    "SELECT 1 FROM import_file WHERE drive_file_id = $1 AND status = 'processed'",
    [driveFileId]
  );
  return res.rowCount !== null && res.rowCount > 0;
}

export async function insertImportFile(
  client: PoolClient,
  driveFileId: string,
  filename: string
): Promise<void> {
  await client.query(
    `INSERT INTO import_file (drive_file_id, filename, status)
     VALUES ($1, $2, 'processing')
     ON CONFLICT (drive_file_id) DO UPDATE SET
       filename = EXCLUDED.filename,
       status = 'processing',
       error_message = NULL`,
    [driveFileId, filename]
  );
}

export async function updateImportFileSuccess(
  client: PoolClient,
  driveFileId: string,
  rowCount: number,
  batchnumber: number
): Promise<void> {
  await client.query(
    `UPDATE import_file
     SET status = 'processed', row_count = $2, batchnumber = $3, error_message = NULL
     WHERE drive_file_id = $1`,
    [driveFileId, rowCount, batchnumber]
  );
}

export async function updateImportFileError(
  client: PoolClient,
  driveFileId: string,
  errorMessage: string
): Promise<void> {
  await client.query(
    `UPDATE import_file SET status = 'error', error_message = $2 WHERE drive_file_id = $1`,
    [driveFileId, errorMessage]
  );
}

export interface MappedRow {
  [column: string]: unknown;
}

/** Columns that expect bigint/integer - coerce to number or null. */
const NUMERIC_COLUMNS = new Set([
  'job_id', 'planned_duration_mins', 'gps_duration_mins', 'worker_duration_mins',
  'number_of_steps', 'number_of_loads', 'field_docket_number', 'booking_id',
]);

/** Columns that expect timestamp - parse date strings to ISO. */
const TIMESTAMP_COLUMNS = new Set([
  'planned_start_time', 'gps_start_time', 'gps_end_time',
  'step_1_completed_at', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at',
]);

/** Max character length per varchar column (from tbl_vworkjobs schema). Truncate longer values. */
const COLUMN_MAX_LENGTHS: Record<string, number> = {
  job_id: 20,
  customer: 100,
  template: 50,
  worker: 50,
  proof_of_delivery: 100,
  progress_state: 50,
  step_1_name: 100,
  step_2_name: 100,
  step_3_name: 100,
  step_4_name: 100,
  step_1_address: 500,
  step_2_address: 500,
  step_3_address: 500,
  step_4_address: 500,
  contains_mog: 50,
  delivery_location_map: 500,
  delivery_winery: 200,
  vinename: 100,
  driver_notes: 1000,
  field_docket_number: 50,
  field_notes: 1000,
  pickup_location_map: 500,
  trailer_rego: 20,
  truck_id: 20,
  truck_rego: 20,
  comments: 1000,
};

function truncateString(s: string, maxLen: number): string {
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen);
}

function parseExcelDate(v: unknown): string | null {
  if (v == null || v === '') return null;
  if (v instanceof Date && !isNaN(v.getTime())) return v.toISOString();
  const n = Number(v);
  if (!isNaN(n)) {
    if (n > 1e12) {
      const date = new Date(n);
      if (!isNaN(date.getTime())) return date.toISOString();
    }
    if (n > 0 && n < 2958466) {
      const date = new Date((n - 25569) * 86400 * 1000);
      if (!isNaN(date.getTime())) return date.toISOString();
    }
  }
  const s = String(v).trim();
  if (!s) return null;
  // Try full string first so "7/03/2025 6:45:56 AM" keeps time (regex would strip to midnight)
  const hasTime = /\d{1,2}:\d{1,2}(:\d{1,2})?(\s*[ap]m)?/i.test(s) || /\s+\d/.test(s);
  if (hasTime) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  // Date-only formats: "1/10/00", "01/10/2000", "2020-01-15" (d/m/y)
  const m = s.match(/^(\d{1,4})[\/\-](\d{1,2})[\/\-](\d{1,4})/);
  if (m) {
    const [, a, b, c] = m;
    let y = parseInt(c!, 10);
    if (y < 100) y += 2000; // "00" -> 2000
    const month = parseInt(b!, 10) - 1;
    const day = parseInt(a!, 10);
    const d = new Date(y, month, day);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

function coerceValue(col: string, v: unknown): unknown {
  if (v === '' || v == null) return null;
  const s = String(v).trim();
  if (!s) return null;

  if (NUMERIC_COLUMNS.has(col)) {
    const n = Number(v);
    if (!isNaN(n) && Number.isInteger(n)) return n;
    // Excel date string in numeric column - treat as invalid, use null
    if (/^\d{1,4}[\/\-]\d{1,2}[\/\-]\d{1,4}/.test(s)) return null;
    const parsed = parseInt(s, 10);
    return isNaN(parsed) ? null : parsed;
  }
  if (TIMESTAMP_COLUMNS.has(col)) {
    return parseExcelDate(v) ?? s;
  }
  const maxLen = COLUMN_MAX_LENGTHS[col];
  if (maxLen != null && typeof v === 'string') {
    return truncateString(v, maxLen);
  }
  if (maxLen != null && typeof v === 'number') {
    return truncateString(String(v), maxLen);
  }
  return v;
}

const MAPPED_COLUMNS = new Set([
  'job_id', 'customer', 'template', 'worker', 'planned_start_time', 'planned_duration_mins',
  'gps_start_time', 'gps_end_time', 'gps_duration_mins', 'worker_duration_mins',
  'proof_of_delivery', 'progress_state', 'number_of_steps',
  'step_1_name', 'step_1_completed_at', 'step_1_address',
  'step_2_name', 'step_2_completed_at', 'step_2_address',
  'step_3_name', 'step_3_completed_at', 'step_3_address',
  'step_4_name', 'step_4_completed_at', 'step_4_address',
  'booking_id', 'comments', 'contains_mog', 'delivery_location_map', 'delivery_winery',
  'vinename', 'driver_notes', 'field_docket_number', 'field_notes',
  'load_size', 'number_of_loads', 'pickup_location_map', 'trailer_rego', 'truck_id', 'truck_rego',
  'batchnumber', 'raw_row'
]);

export async function upsertVworkJob(
  client: PoolClient,
  mapped: MappedRow,
  rawRow: Record<string, unknown>,
  batchnumber: number
): Promise<void> {
  const jobIdVal = coerceValue('job_id', mapped.job_id);
  if (jobIdVal == null) return; // Skip rows with invalid job_id

  const mappedCols = Object.keys(mapped).filter(k => k !== 'job_id' && MAPPED_COLUMNS.has(k));
  const allCols = ['job_id', 'batchnumber', 'raw_row', ...mappedCols];
  const values: unknown[] = [
    jobIdVal,
    batchnumber,
    JSON.stringify(rawRow)
  ];
  for (const col of mappedCols) {
    const v = coerceValue(col, mapped[col]);
    values.push(v === '' ? null : v);
  }

  const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
  const updateSet = mappedCols
    .map((c, i) => `${c} = EXCLUDED.${c}`)
    .join(', ');
  const updateSetFull = `batchnumber = EXCLUDED.batchnumber, raw_row = EXCLUDED.raw_row, ${updateSet}`;

  try {
    await client.query(
      `INSERT INTO tbl_vworkjobs (${allCols.join(', ')})
       VALUES (${placeholders})
       ON CONFLICT (job_id) DO UPDATE SET ${updateSetFull}`,
      values
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.db(
      { job_id: jobIdVal, batchnumber, columns: allCols, values_preview: values.slice(0, 5), pg_error: msg },
      `upsert failed: ${msg}`
    );
    throw err;
  }
}
