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
      const fileKey = filefieldname.trim().toLowerCase().replace(/\s+/g, ' ').trim();
      /** PG column names are case-insensitive unless quoted; JS mapped row keys must match code (always lowercase). */
      const sqlCol = dbcolumnname.trim().toLowerCase().replace(/\s+/g, ' ').trim();
      if (fileKey && sqlCol) headerToColumn[fileKey] = sqlCol;
    }
    if (dbcolumnname && dbmaxlength != null) {
      const sqlCol = dbcolumnname.trim().toLowerCase().replace(/\s+/g, ' ').trim();
      if (sqlCol) columnMaxLengths[sqlCol] = dbmaxlength;
    }
  }
  return { headerToColumn, columnMaxLengths };
}

/** Distinct normalized `dbcolumnname` values for active VW mappings (source of truth for which job columns we import). */
export async function getVworkMappingDbcolumnNames(client: PoolClient): Promise<Set<string>> {
  const res = await client.query<{ col: string }>(
    `SELECT DISTINCT lower(regexp_replace(trim(dbcolumnname), '\\s+', ' ', 'g')) AS col
     FROM tbl_mappings
     WHERE type = 'VW' AND is_active = true
       AND dbcolumnname IS NOT NULL AND trim(dbcolumnname) <> ''`
  );
  return new Set(res.rows.map((r) => String(r.col)));
}

/** Insertable columns: `tbl_mappings` (VW) targets ∩ physical table only. */
export function buildVworkAllowedInsertColumns(
  mappingDbcolumns: Set<string>,
  tableColumns: Set<string>
): Set<string> {
  const allowed = new Set<string>();
  for (const c of mappingDbcolumns) {
    if (tableColumns.has(c)) allowed.add(c);
  }
  return allowed;
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

/**
 * Lowercase physical column names on `public.tbl_vworkjobs` (pg_catalog; omits dropped attributes).
 * Uses schema `public` explicitly so pooled connections with a non-default `search_path` still match INSERT targets.
 */
export async function getTblVworkjobsColumns(client: PoolClient): Promise<Set<string>> {
  const res = await client.query<{ column_name: string }>(
    `SELECT a.attname::text AS column_name
     FROM pg_catalog.pg_attribute a
     INNER JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
     INNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public'
       AND c.relname = 'tbl_vworkjobs'
       AND a.attnum > 0
       AND NOT a.attisdropped`
  );
  return new Set(res.rows.map((r) => String(r.column_name).toLowerCase()));
}

const PG_UNDEFINED_COLUMN = '42703';

/** Best-effort parse of PostgreSQL undefined-column messages (quoted or not). */
function parseUndefinedColumnName(message: string): string | null {
  let m = message.match(/column "([^"]+)" of relation/i);
  if (m) return m[1].toLowerCase();
  m = message.match(/column\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+of\s+relation/i);
  if (m) return m[1].toLowerCase();
  return null;
}

function isPostgresUndefinedColumnError(err: unknown, message: string): boolean {
  const code =
    typeof err === 'object' && err !== null && 'code' in err
      ? String((err as { code: unknown }).code)
      : '';
  if (code === PG_UNDEFINED_COLUMN) return true;
  return /\bcolumn\b[\s\S]{0,120}\bdoes not exist\b/i.test(message);
}

/**
 * Drop mapped keys that are not real columns on `tbl_vworkjobs` (must run after catalog load).
 * Keeps primary key column only if present.
 */
export function stripMappedVworkRowToAllowedColumns(mapped: MappedRow, allowed: Set<string>): void {
  const pkCol = vworkPkColumn(mapped);
  const pkLow = pkCol.toLowerCase();
  for (const k of [...Object.keys(mapped)]) {
    const low = k.toLowerCase();
    if (low === pkLow) continue;
    if (!allowed.has(low)) delete mapped[k];
  }
}

/** Mapped keys present in export rows that are not actual table columns (values were not stored). */
export function vworkMappedKeysNotInTable(mapped: MappedRow[], tableColumns: Set<string>): string[] {
  const seen = new Set<string>();
  for (const row of mapped) {
    for (const k of Object.keys(row)) {
      const low = k.toLowerCase();
      if (low === 'batchnumber' || low === 'raw_row') continue;
      if (!tableColumns.has(low)) seen.add(low);
    }
  }
  return [...seen].sort();
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

/**
 * Parse "Load Size" cell: first number wins (e.g. "3 t" → 3, "approx 2.5" → 2.5). Dates → null.
 * Exported for Drive backfill; also used in coerceValue for column loadsize.
 */
export function parseLoadSizeNumeric(v: unknown): number | null {
  if (v === '' || v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (/^\d{1,4}[\/\-]\d{1,2}[\/\-]\d{1,4}/.test(s)) return null;
  const m = s.match(/-?\d+(?:\.\d+)?/);
  if (!m) return null;
  const n = parseFloat(m[0]);
  return Number.isFinite(n) ? n : null;
}

function coerceValue(
  col: string,
  v: unknown,
  maxLengths?: Record<string, number>
): unknown {
  if (v === '' || v == null) return null;
  const s = String(v).trim();
  if (!s) return null;

  if (col === 'loadsize') {
    return parseLoadSizeNumeric(v);
  }

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

/**
 * Hard gate: drop every non-PK field whose name is not an active VW `dbcolumnname` in `tbl_mappings`.
 * Run after parse so nothing (including parser bugs / old defaults) can reach INSERT.
 */
export function enforceVworkMappedRowMappingTargetsOnly(
  mapped: MappedRow,
  mappingDbcolumnNames: Set<string>
): void {
  const pkCol = vworkPkColumn(mapped);
  const pkLow = pkCol.toLowerCase();
  for (const k of [...Object.keys(mapped)]) {
    const low = k.toLowerCase();
    if (low === pkLow) continue;
    if (!mappingDbcolumnNames.has(low)) delete mapped[k];
  }
}

/** Trimmed job id string for UPDATEs, or null if missing. */
export function getVworkJobPkString(mapped: MappedRow, maxLengths?: Record<string, number>): string | null {
  const pkCol = vworkPkColumn(mapped);
  const pkVal = coerceValue(pkCol, mapped[pkCol], maxLengths);
  if (pkVal == null) return null;
  const t = String(pkVal).trim();
  return t || null;
}

/** Sets only loadsize; does not touch other columns. Returns row count (0 or 1). */
export async function updateVworkJobLoadsizeOnly(
  client: PoolClient,
  jobIdTrimmed: string,
  loadsize: number
): Promise<number> {
  const res = await client.query(
    `UPDATE tbl_vworkjobs SET loadsize = $2::numeric WHERE trim(job_id::text) = $1`,
    [jobIdTrimmed, loadsize]
  );
  return res.rowCount ?? 0;
}

/** Returns true if row was inserted, false if skipped (job_id already exists). */
export async function upsertVworkJob(
  client: PoolClient,
  mapped: MappedRow,
  rawRow: Record<string, unknown>,
  batchnumber: number,
  /** Mutable: physical columns; 42703 may shrink for the rest of the import run. */
  tableColumns: Set<string>,
  /** Active VW `dbcolumnname` set from `tbl_mappings` — non-PK INSERT columns must be listed here. */
  mappingDbcolumnNames: Set<string>,
  maxLengths?: Record<string, number>
): Promise<boolean> {
  const pkCol = vworkPkColumn(mapped);
  const pkVal = coerceValue(pkCol, mapped[pkCol], maxLengths);
  if (pkVal == null) return false;

  const pkSql = pkCol.toLowerCase();
  if (!tableColumns.has(pkSql)) {
    const msg = `primary key column "${pkSql}" not found on tbl_vworkjobs`;
    logger.db({ [pkCol]: pkVal, batchnumber, pg_error: msg }, `upsert skipped: ${msg}`);
    throw new Error(msg);
  }

  const toSqlCol = (c: string) => c.toLowerCase();

  for (let attempt = 0; attempt < 96; attempt++) {
    const dataCols = Object.keys(mapped)
      .filter((k) => k !== pkCol && k !== 'batchnumber' && k !== 'raw_row')
      .filter((k) => mappingDbcolumnNames.has(k.toLowerCase()))
      .filter((k) => tableColumns.has(k.toLowerCase()))
      .sort();
    // tbl_vworkjobs has no raw_row or raw_row is integer; do not insert JSON
    const allCols = [pkCol, 'batchnumber', ...dataCols];
    const values: unknown[] = [pkVal, batchnumber];
    for (const col of dataCols) {
      const v = coerceValue(col, mapped[col], maxLengths);
      values.push(v === '' ? null : v);
    }

    const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
    const sqlCols = allCols.map(toSqlCol).join(', ');

    try {
      const res = await client.query(
        `INSERT INTO tbl_vworkjobs (${sqlCols}) VALUES (${placeholders})
         ON CONFLICT (${toSqlCol(pkCol)}) DO NOTHING`,
        values
      );
      return (res.rowCount ?? 0) > 0;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (isPostgresUndefinedColumnError(err, msg)) {
        const bad = parseUndefinedColumnName(msg);
        if (bad) {
          const removedFromSet = tableColumns.delete(bad);
          mappingDbcolumnNames.delete(bad);
          for (const k of [...Object.keys(mapped)]) {
            if (k.toLowerCase() === bad) delete mapped[k];
          }
          logger.db(
            {
              [pkCol]: pkVal,
              batchnumber,
              droppedColumn: bad,
              removedFromAllowedSet: removedFromSet,
              pg_error: msg,
            },
            'tbl_vworkjobs INSERT: undefined column; dropped from allowed set and/or mapped row, retrying'
          );
          continue;
        }
      }
      logger.db(
        { [pkCol]: pkVal, batchnumber, columns: allCols, values_preview: values.slice(0, 5), pg_error: msg },
        `upsert failed: ${msg}`
      );
      throw err;
    }
  }

  throw new Error('upsertVworkJob: too many undefined-column retries');
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
