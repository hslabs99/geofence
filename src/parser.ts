import * as XLSX from 'xlsx';
import { parse } from 'csv-parse';
import { Readable } from 'stream';
import { HEADER_TO_COLUMN } from './mapping';
import { MappedRow } from './db';

function normalizeHeader(h: string): string {
  const s = String(h ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
  if (!s || s === '#ref!') return '';
  return s;
}

/**
 * Convert raw header -> value record to mapped column -> value.
 * Planned Duration (hrs) -> planned_duration_mins: multiply by 60.
 */
export function mapRow(row: Record<string, string | undefined>): MappedRow {
  const mapped: MappedRow = {};
  for (const [header, value] of Object.entries(row)) {
    const norm = normalizeHeader(header);
    if (!norm) continue;
    const col = HEADER_TO_COLUMN[norm];
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
 */
export function parseXlsx(buffer: Buffer): Record<string, string | undefined>[] {
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const raw = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
    raw: false,
    defval: '',
    blankrows: false,
  });

  const rows: Record<string, string | number | undefined>[] = [];
  for (const r of raw) {
    const row: Record<string, string | number | undefined> = {};
    for (const [k, v] of Object.entries(r)) {
      const norm = normalizeHeader(k);
      if (!norm) continue;
      if (v == null) row[norm] = '';
      else if (v instanceof Date) row[norm] = v.getTime(); // preserve time via Excel serial for db layer
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
  filename: string
): Promise<{ mapped: MappedRow[]; raw: Record<string, unknown>[] }> {
  const lower = filename.toLowerCase();
  let rows: Record<string, string | undefined>[];

  if (lower.endsWith('.xlsx')) {
    rows = parseXlsx(buffer);
  } else if (lower.endsWith('.csv')) {
    rows = await parseCsv(buffer);
  } else {
    throw new Error(`Unsupported file type: ${filename}`);
  }

  const mapped: MappedRow[] = [];
  const raw: Record<string, unknown>[] = [];

  for (const r of rows) {
    const m = mapRow(r);
    if (!m.job_id && Object.keys(m).length === 0) continue;
    const rawRow: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) {
      rawRow[k] = v;
    }
    mapped.push(m);
    raw.push(rawRow);
  }

  return { mapped, raw };
}
