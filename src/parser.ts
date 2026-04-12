import * as XLSX from 'xlsx';
import { parse } from 'csv-parse';
import { Readable } from 'stream';
import { HEADER_TO_COLUMN, GPS_HEADER_TO_COLUMN, TRACKING_HEADER_TO_COLUMN } from './mapping';
import { MappedRow } from './db';

export type GpsDataRow = Record<string, unknown>;

/** Format Date to YYYY-MM-DD HH:mm:ss for import. Excel dates → string for db layer. */
function excelDateToSql(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/** Normalize sheet/CSV header: BOM strip, case-insensitive, collapse spaces. */
export function normalizeHeader(h: string): string {
  const s = String(h ?? '')
    .replace(/^\uFEFF/, '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
  if (!s || s === '#ref!') return '';
  return s;
}

function effectiveVworkHeaderToColumn(headerToColumn?: Record<string, string>): Record<string, string> {
  if (headerToColumn === undefined) return HEADER_TO_COLUMN;
  return headerToColumn;
}

/**
 * Normalized CSV/XLSX header names that appear in the file but have no mapping (ignored; not inserted).
 */
export function collectUnmappedCsvHeaderNorms(
  rows: Record<string, string | undefined>[],
  headerToColumn?: Record<string, string>
): string[] {
  const h2c = effectiveVworkHeaderToColumn(headerToColumn);
  const seen = new Set<string>();
  for (const r of rows) {
    for (const headerKey of Object.keys(r)) {
      const norm = normalizeHeader(headerKey);
      if (!norm) continue;
      if (h2c[norm] === undefined) seen.add(norm);
    }
  }
  return [...seen].sort();
}

/**
 * Convert raw header -> value record to mapped column -> value.
 * Planned Duration (hrs) -> planned_duration_mins: multiply by 60.
 */
/**
 * @param headerToColumn — Omit for full `HEADER_TO_COLUMN`. Pass an explicit map (even `{}`) after
 *   `filterHeaderToColumnByMappingTargets`; never fall back to defaults when an object is passed — otherwise
 *   empty filters would re-enable every hardcoded column (e.g. `field_notes`).
 */
export function mapRow(
  row: Record<string, string | undefined>,
  headerToColumn?: Record<string, string>
): MappedRow {
  const h2c = headerToColumn === undefined ? HEADER_TO_COLUMN : headerToColumn;
  const mapped: MappedRow = {};
  for (const [header, value] of Object.entries(row)) {
    const norm = normalizeHeader(header);
    if (!norm) continue;
    const col = h2c[norm];
    if (!col) continue;
    let v: string | number | null = value === '' || value === undefined ? null : value;
    if (col === 'planned_duration_mins' && norm === 'planned duration (hrs)' && v != null) {
      const hrs = typeof v === 'string' ? parseFloat(v) : Number(v);
      v = isNaN(hrs) ? null : String(Math.round(hrs * 60));
    }
    mapped[col] = v;
  }
  return mapped;
}

/**
 * Parse XLSX buffer. First sheet, cellDates: true.
 * @param headerRowOffset - Number of rows to skip before using the next row as headers (0 = first row is headers; 1 = skip row 1, use row 2 as headers)
 */
export function parseXlsx(
  buffer: Buffer,
  headerRowOffset = 0
): Record<string, string | undefined>[] {
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const raw = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
    raw: false,
    defval: '',
    blankrows: false,
    range: headerRowOffset,
  });

  const rows: Record<string, string | number | undefined>[] = [];
  for (const r of raw) {
    const row: Record<string, string | number | undefined> = {};
    for (const [k, v] of Object.entries(r)) {
      const norm = normalizeHeader(k);
      if (!norm) continue;
      if (v == null) row[norm] = '';
      else if (v instanceof Date) row[norm] = excelDateToSql(v);
      else row[norm] = String(v);
    }
    rows.push(row);
  }
  return rows as Record<string, string | undefined>[];
}

/**
 * Parse CSV buffer.
 */
export async function parseCsv(buffer: Buffer): Promise<Record<string, string | undefined>[]> {
  const rows: Record<string, string | undefined>[] = [];
  const stream = Readable.from(buffer);

  return new Promise((resolve, reject) => {
    let headers: string[] = [];

    stream
      .pipe(
        parse({
          columns: false,
          trim: true,
          skip_empty_lines: true,
        })
      )
      .on('data', (row: string[]) => {
        if (headers.length === 0) {
          headers = row;
          return;
        }
        const obj: Record<string, string | undefined> = {};
        row.forEach((val, i) => {
          const h = headers[i];
          const norm = normalizeHeader(h);
          if (!norm) return;
          obj[norm] = val ?? '';
        });
        rows.push(obj);
      })
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

/**
 * Parse buffer as XLSX or CSV based on filename.
 * Returns array of mapped rows and raw rows (for raw_row jsonb).
 */
export async function parseFile(
  buffer: Buffer,
  filename: string,
  headerToColumn?: Record<string, string>
): Promise<{
  mapped: MappedRow[];
  raw: Record<string, unknown>[];
  unmappedCsvHeaders: string[];
}> {
  const lower = filename.toLowerCase();
  let rows: Record<string, string | undefined>[];

  if (lower.endsWith('.xlsx') || lower.endsWith('.xls')) {
    rows = parseXlsx(buffer);
  } else if (lower.endsWith('.csv')) {
    rows = await parseCsv(buffer);
  } else {
    throw new Error(`Unsupported file type: ${filename}`);
  }

  const mapped: MappedRow[] = [];
  const raw: Record<string, unknown>[] = [];

  const unmappedCsvHeaders = collectUnmappedCsvHeaderNorms(rows, headerToColumn);
  for (const r of rows) {
    const m = mapRow(r, headerToColumn);
    if (Object.keys(m).length === 0) continue;
    const rawRow: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) {
      rawRow[k] = v;
    }
    mapped.push(m);
    raw.push(rawRow);
  }

  return { mapped, raw, unmappedCsvHeaders };
}

/**
 * Map raw row (header -> value) to tbl_gpsdata column names using GPS_HEADER_TO_COLUMN.
 */
export function mapRowGpsData(
  row: Record<string, string | undefined>,
  headerToColumn: Record<string, string> = GPS_HEADER_TO_COLUMN
): GpsDataRow {
  const h2c = Object.keys(headerToColumn).length ? headerToColumn : GPS_HEADER_TO_COLUMN;
  const mapped: GpsDataRow = {};
  for (const [header, value] of Object.entries(row)) {
    const norm = normalizeHeader(header);
    if (!norm) continue;
    const col = h2c[norm];
    if (!col) continue;
    const v = value === '' || value === undefined ? null : value;
    mapped[col] = v;
  }
  return mapped;
}

/**
 * Parse buffer as XLSX or CSV for gpsdata import. Returns rows with tbl_gpsdata column names.
 */
export async function parseFileForGpsData(
  buffer: Buffer,
  filename: string,
  headerToColumn?: Record<string, string>
): Promise<{
  mapped: GpsDataRow[];
  raw: Record<string, unknown>[];
  rawRowCount: number;
  sampleHeaders: string[];
}> {
  const lower = filename.toLowerCase();
  let rows: Record<string, string | undefined>[];

  if (lower.endsWith('.xlsx') || lower.endsWith('.xls')) {
    rows = parseXlsx(buffer, 1); // GPS .xls: headers in row 2, data from row 3
  } else if (lower.endsWith('.csv')) {
    rows = await parseCsv(buffer);
  } else {
    throw new Error(`Unsupported file type: ${filename}`);
  }

  const mapped: GpsDataRow[] = [];
  const raw: Record<string, unknown>[] = [];
  const h2c = headerToColumn ?? {};
  const sampleHeaders = rows[0] ? Object.keys(rows[0]) : [];

  for (const r of rows) {
    const m = mapRowGpsData(r, h2c);
    if (Object.keys(m).length === 0) continue;
    const rawRow: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) {
      rawRow[k] = v;
    }
    mapped.push(m);
    raw.push(rawRow);
  }

  return { mapped, raw, rawRowCount: rows.length, sampleHeaders };
}

export type TrackingRow = {
  device_no?: string;
  device_name?: string;
  imei?: string;
  model?: string;
  ignition?: string;
  position_time?: string;
  speed_raw?: string;
  speed_kmh?: number;
  azimuth?: string;
  position_type?: string;
  satellites?: string;
  data_type?: string;
  lat?: number;
  lon?: number;
  address?: string;
};

/**
 * Parse lat,lon from Coordinates column.
 * Handles: (1) plain "lat,lon", (2) Excel HYPERLINK display text, (3) URL params lat=&lng=
 * Example: =HYPERLINK("http://...?lat=-39.547938&lng=176.680178","-39.547938,176.680178")
 */
function parseCoordinates(v: unknown): { lat: number; lon: number } | null {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;

  // Plain "lat,lon"
  let match = s.match(/^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$/);
  if (match) {
    const lat = parseFloat(match[1]);
    const lon = parseFloat(match[2]);
    if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
      return { lat, lon };
    }
  }

  // HYPERLINK formula: extract display text (second arg) "lat,lon"
  match = s.match(/,\s*["'](-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)["']\s*\)/);
  if (match) {
    const lat = parseFloat(match[1]);
    const lon = parseFloat(match[2]);
    if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
      return { lat, lon };
    }
  }

  // URL params: lat=-39.547938&lng=176.680178
  const latMatch = s.match(/[?&]lat=(-?\d+(?:\.\d+)?)/);
  const lngMatch = s.match(/[?&]lng=(-?\d+(?:\.\d+)?)/);
  if (latMatch && lngMatch) {
    const lat = parseFloat(latMatch[1]);
    const lon = parseFloat(lngMatch[1]);
    if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
      return { lat, lon };
    }
  }

  return null;
}

/** Extract leading numeric speed from "67 Southwest(Direction236)". */
function parseSpeedKmh(v: unknown): number | null {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  const match = s.match(/^(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const n = parseFloat(match[1]);
  return isNaN(n) ? null : n;
}

/**
 * Map raw row to tbl_tracking columns. Coordinates -> lat, lon; Speed -> speed_raw + speed_kmh.
 */
function mapRowTracking(
  row: Record<string, string | undefined>,
  headerToColumn: Record<string, string> = TRACKING_HEADER_TO_COLUMN
): TrackingRow {
  const h2c = Object.keys(headerToColumn).length ? headerToColumn : TRACKING_HEADER_TO_COLUMN;
  const mapped: TrackingRow = {};
  for (const [header, value] of Object.entries(row)) {
    const norm = normalizeHeader(header);
    if (!norm) continue;
    const col = h2c[norm];
    if (!col) continue;
    if (col === 'coordinates') {
      const coords = parseCoordinates(value);
      if (coords) {
        mapped.lat = coords.lat;
        mapped.lon = coords.lon;
      }
      continue;
    }
    if (col === 'speed_raw') {
      const v = value === '' || value === undefined ? null : value;
      mapped.speed_raw = v ? String(v).trim() || undefined : undefined;
      mapped.speed_kmh = parseSpeedKmh(value) ?? undefined;
      continue;
    }
    if (col === 'position_time') {
      const v = value === '' || value === undefined ? null : value;
      if (v) {
        const s = String(v).trim();
        if (s) mapped.position_time = s;
      }
      continue;
    }
    const v = value === '' || value === undefined ? null : value;
    (mapped as Record<string, unknown>)[col] = v ? String(v).trim() || undefined : undefined;
  }
  return mapped;
}

/**
 * Parse XLS/XLSX for GPS Tracking import.
 * Handles Coordinates column (Excel hyperlink) - extracts raw lat,lon from display text.
 * Headers in row 2, data from row 3.
 */
export async function parseFileForTracking(
  buffer: Buffer,
  filename: string
): Promise<{ mapped: TrackingRow[]; rawRowCount: number; sampleHeaders: string[] }> {
  const lower = filename.toLowerCase();
  let rows: Record<string, string | undefined>[];

  if (lower.endsWith('.xlsx') || lower.endsWith('.xls')) {
    rows = parseXlsx(buffer, 1); // headers in row 2, data from row 3
  } else {
    throw new Error(`Unsupported file type for tracking: ${filename}`);
  }

  const mapped: TrackingRow[] = [];
  const sampleHeaders = rows[0] ? Object.keys(rows[0]) : [];

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const m = mapRowTracking(r);
    const rawCoord = r['coordinates'] ?? r['Coordinates'];
    if (i < 3) {
      console.log(`[tracking] Row ${i + 1} raw Coordinates:`, typeof rawCoord, JSON.stringify(String(rawCoord ?? '').slice(0, 120)));
    }
    if (m.lat == null || m.lon == null) {
      if (i < 5) console.log(`[tracking] Row ${i + 1} skipped (no lat/lon) - raw value:`, JSON.stringify(String(rawCoord ?? '').slice(0, 120)));
      continue;
    }
    mapped.push(m);
  }

  console.log(`[tracking] Parsed ${mapped.length}/${rows.length} rows with valid coords`);
  return { mapped, rawRowCount: rows.length, sampleHeaders };
}
