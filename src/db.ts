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

export async function getNextGpsDataBatchNumber(client: PoolClient): Promise<number> {
  const res = await client.query("SELECT nextval('seq_gpsdata_batch') as batch");
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

export interface MappingsResult {
  headerToColumn: Record<string, string>;
  columnMaxLengths: Record<string, number>;
}

export async function getMappings(
  client: PoolClient,
  type: string
): Promise<MappingsResult> {
  const res = await client.query(
    `SELECT filefieldname, dbcolumnname, dbmaxlength FROM tbl_mappings WHERE type = $1 AND is_active = true`,
    [type]
  );
  const headerToColumn: Record<string, string> = {};
  const columnMaxLengths: Record<string, number> = {};
  for (const row of res.rows) {
    const filefieldname = (row as { filefieldname: string }).filefieldname;
    const dbcolumnname = (row as { dbcolumnname: string }).dbcolumnname;
    const dbmaxlength = (row as { dbmaxlength: number | null }).dbmaxlength;
    if (filefieldname && dbcolumnname) {
      const norm = filefieldname.trim().toLowerCase().replace(/\s+/g, ' ').trim();
      if (norm) headerToColumn[norm] = dbcolumnname;
    }
    if (dbcolumnname && dbmaxlength != null) {
      columnMaxLengths[dbcolumnname] = dbmaxlength;
    }
  }
  return { headerToColumn, columnMaxLengths };
}

export async function insertLog(
  client: PoolClient,
  logtype: string,
  logcat1: string | null,
  logcat2: string | null,
  logdetails: string | null
): Promise<void> {
  await client.query(
    `INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)`,
    [logtype, logcat1 ?? null, logcat2 ?? null, logdetails ?? null]
  );
}

export interface MappedRow {
  [column: string]: unknown;
}

/** Columns that expect bigint/integer - coerce to number or null. job_id is varchar(20), so not numeric. */
const NUMERIC_COLUMNS = new Set([
  'planned_duration_mins', 'gps_duration_mins', 'worker_duration_mins',
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

/** SQL timestamp format - no Date, no timezone. Import = preserve raw data; only reformat when needed. */
const SQL_TS = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/;

/** Already YYYY-MM-DD HH:mm:ss — pass through as-is. */
function isSqlTimestamp(s: string): boolean {
  return SQL_TS.test(s.trim());
}

const MONTH_NAMES: Record<string, string> = {
  jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
  jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12',
};

/** Reformat "19 Feb 2026 21:55:00" → "2026-02-19 21:55:00". Literal extract only, no Date. */
function reformatVworkTimestamp(s: string): string | null {
  const t = s.trim();
  const m1 = t.match(/^(\d{1,2})\s+([a-z]{3})\s+(\d{2,4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?/i);
  if (m1) {
    const [, d, mon, y, h, min, sec = '00'] = m1;
    const mo = MONTH_NAMES[mon!.toLowerCase().slice(0, 3)];
    if (!mo) return null;
    const yy = y!.length === 2 ? `20${y}` : y!;
    const pad = (x: string) => x.padStart(2, '0');
    return `${yy}-${mo}-${pad(d!)} ${pad(h!)}:${pad(min!)}:${pad(sec)}`;
  }
  const m2 = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (m2) {
    const [, d, mo, y, h, min, sec = '00'] = m2;
    const yy = y!.length === 2 ? `20${y}` : y!;
    const pad = (x: string) => x.padStart(2, '0');
    return `${yy}-${pad(mo!)}-${pad(d!)} ${pad(h!)}:${pad(min!)}:${pad(sec)}`;
  }
  return null;
}

/** Vwork timestamp: pass through SQL format, or reformat dd Mon yyyy / dd/mm/yy. No Date. */
function formatVworkTimestamp(v: unknown): string | null {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;
  if (isSqlTimestamp(s)) return s;
  return reformatVworkTimestamp(s);
}

/** GPS timestamp: pass through YYYY-MM-DD HH:mm:ss as-is. No conversion. */
function formatGpsTimestamp(v: unknown): string | null {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;
  if (isSqlTimestamp(s)) return s;
  return null;
}

function coerceValue(
  col: string,
  v: unknown,
  maxLengths?: Record<string, number>
): unknown {
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
    return formatVworkTimestamp(v) ?? s;
  }
  const maxLen = maxLengths?.[col] ?? COLUMN_MAX_LENGTHS[col] ?? 500;
  const str = typeof v === 'string' ? v : String(v);
  return truncateString(str, maxLen);
}

/** Primary key column for tbl_vworkjobs (from tbl_mappings). Supports Job_Id (new) or job_id (legacy). */
function vworkPkColumn(mapped: MappedRow): string {
  if ('Job_Id' in mapped && mapped.Job_Id != null && mapped.Job_Id !== '') return 'Job_Id';
  return 'job_id';
}

/** Returns true if row was inserted, false if skipped (job_id already exists). */
export async function upsertVworkJob(
  client: PoolClient,
  mapped: MappedRow,
  rawRow: Record<string, unknown>,
  batchnumber: number,
  maxLengths?: Record<string, number>
): Promise<boolean> {
  const pkCol = vworkPkColumn(mapped);
  const pkVal = coerceValue(pkCol, mapped[pkCol], maxLengths);
  if (pkVal == null) return false;

  const dataCols = Object.keys(mapped)
    .filter((k) => k !== pkCol && k !== 'batchnumber' && k !== 'raw_row')
    .sort();
  // tbl_vworkjobs has no raw_row or raw_row is integer; do not insert JSON
  const allCols = [pkCol, 'batchnumber', ...dataCols];
  const values: unknown[] = [pkVal, batchnumber];
  for (const col of dataCols) {
    const v = coerceValue(col, mapped[col], maxLengths);
    values.push(v === '' ? null : v);
  }

  const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
  const toSqlCol = (c: string) => c.toLowerCase();
  const sqlCols = allCols.map(toSqlCol).join(', ');

  try {
    const res = await client.query(
      `INSERT INTO tbl_vworkjobs (${sqlCols}) VALUES (${placeholders})
       ON CONFLICT (${toSqlCol(pkCol)}) DO NOTHING`,
      values
    );
    return (res.rowCount ?? 0) > 0;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.db(
      { [pkCol]: pkVal, batchnumber, columns: allCols, values_preview: values.slice(0, 5), pg_error: msg },
      `upsert failed: ${msg}`
    );
    throw err;
  }
}

// --- tbl_gpsdata ---

const GPSDATA_VARCHAR_LENGTHS: Record<string, number> = {
  device_name: 50,
  imei: 50,
  model: 50,
  driver_name: 50,
  number_plate: 50,
  sim: 50,
  phone: 50,
  fence_name: 150,
  vworkjob: 100,
};

const GPSDATA_TIMESTAMP_COLS = new Set(['enter_time', 'outer_time']);

/** Parse "HH:MM:SS" or "MM:SS" to total seconds (for stay_time). */
function parseDurationToSeconds(v: unknown): number | null {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;
  const n = Number(v);
  if (!isNaN(n) && n >= 0 && Number.isInteger(n)) return n;
  const match = s.match(/^(?:(\d+):)?(\d+):(\d+)$/);
  if (match) {
    const [, h, m, sec] = match;
    const hours = h ? parseInt(h, 10) : 0;
    const mins = parseInt(m!, 10);
    const seconds = parseInt(sec!, 10);
    return hours * 3600 + mins * 60 + seconds;
  }
  return null;
}

function coerceGpsDataValue(
  col: string,
  v: unknown,
  maxLengths?: Record<string, number>
): unknown {
  if (v === '' || v == null) return null;
  const s = String(v).trim();
  if (!s) return null;

  if (col === 'no') {
    const n = Number(v);
    if (!isNaN(n) && Number.isInteger(n)) return n;
    const parsed = parseInt(s, 10);
    return isNaN(parsed) ? null : parsed;
  }
  if (col === 'stay_time') {
    return parseDurationToSeconds(v);
  }
  if (GPSDATA_TIMESTAMP_COLS.has(col)) {
    return formatGpsTimestamp(v);
  }
  const maxLen = maxLengths?.[col] ?? GPSDATA_VARCHAR_LENGTHS[col];
  if (maxLen != null) {
    return truncateString(typeof v === 'string' ? v : String(v), maxLen);
  }
  return v;
}

export interface GpsDataMappedRow {
  [key: string]: unknown;
}

/** Returns true if row was inserted, false if skipped (duplicate device_name+enter_time+fence_name). */
export async function insertGpsDataRow(
  client: PoolClient,
  mapped: GpsDataMappedRow,
  batchnumber: number,
  maxLengths?: Record<string, number>
): Promise<boolean> {
  const dataCols = Object.keys(mapped).filter((k) => k && k !== 'batchnumber').sort();
  if (dataCols.length === 0) return false;
  const cols = ['batchnumber', ...dataCols];
  const values: unknown[] = [batchnumber];
  for (const col of dataCols) {
    values.push(coerceGpsDataValue(col, mapped[col], maxLengths));
  }
  // Duplicate key: device_name + enter_time + fence_name
  const deviceName = coerceGpsDataValue('device_name', mapped.device_name ?? mapped['device_name'], maxLengths);
  const enterTime = coerceGpsDataValue('enter_time', mapped.enter_time ?? mapped['enter_time'], maxLengths);
  const fenceName = coerceGpsDataValue('fence_name', mapped.fence_name ?? mapped['fence_name'], maxLengths);
  const dupCheck = await client.query(
    `SELECT 1 FROM tbl_gpsdata WHERE device_name IS NOT DISTINCT FROM $1 AND enter_time IS NOT DISTINCT FROM $2 AND fence_name IS NOT DISTINCT FROM $3 LIMIT 1`,
    [deviceName, enterTime, fenceName]
  );
  if (dupCheck.rowCount && dupCheck.rowCount > 0) return false;
  const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
  const columnList = cols.join(', ');
  await client.query(
    `INSERT INTO tbl_gpsdata (${columnList}) VALUES (${placeholders})`,
    values
  );
  return true;
}

// --- tbl_tracking (GPS Tracking import) ---

export interface TrackingMappedRow {
  device_no?: string;
  device_name?: string;
  imei?: string;
  model?: string;
  ignition?: string;
  position_time?: string;
  speed_raw?: string;
  speed_kmh?: number | null;
  azimuth?: string;
  position_type?: string;
  satellites?: string;
  data_type?: string;
  lat: number;
  lon: number;
  address?: string;
}

/** Insert a row into tbl_tracking. Returns true if inserted. */
export async function insertTrackingRow(
  client: PoolClient,
  mapped: TrackingMappedRow
): Promise<boolean> {
  const positionTime = mapped.position_time?.trim();
  if (!positionTime || mapped.lat == null || mapped.lon == null) return false;

  // Use ST_GeomFromText with WKT string to avoid parameter type conflicts ($13/$14 numeric vs geom)
  const geomWkt = `POINT(${mapped.lon} ${mapped.lat})`;

  await client.query(
    `INSERT INTO tbl_tracking (device_no, device_name, imei, model, ignition, position_time, speed_raw, speed_kmh, azimuth, position_type, satellites, data_type, lat, lon, geom, address)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, ST_SetSRID(ST_GeomFromText($15), 4326), $16)`,
    [
      mapped.device_no ?? null,
      mapped.device_name ?? null,
      mapped.imei ?? null,
      mapped.model ?? null,
      mapped.ignition ?? null,
      positionTime,
      mapped.speed_raw ?? null,
      mapped.speed_kmh ?? null,
      mapped.azimuth ?? null,
      mapped.position_type ?? null,
      mapped.satellites ?? null,
      mapped.data_type ?? null,
      mapped.lat,
      mapped.lon,
      geomWkt,
      mapped.address ?? null,
    ]
  );
  return true;
}
