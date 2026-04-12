import {
  getMappings,
  getPool,
  getVworkJobPkString,
  getVworkMappingDbcolumnNames,
  insertLog,
  MappedRow,
  parseLoadSizeNumeric,
  updateVworkJobLoadsizeOnly,
} from './db';
import { downloadFile, listImportableFiles } from './drive';
import { logger } from './logger';
import { filterHeaderToColumnByMappingTargets } from './mapping';
import { normalizeHeader, parseFile } from './parser';

const INDEVIN_SUBSTR = 'indevin';

function customerFromMapped(mapped: MappedRow): string | null {
  const c = mapped.customer;
  if (c != null && String(c).trim() !== '') return String(c);
  const rec = mapped as Record<string, unknown>;
  for (const k of Object.keys(mapped)) {
    if (k.toLowerCase() === 'customer') {
      const v = rec[k];
      if (v != null && String(v).trim() !== '') return String(v);
    }
  }
  return null;
}

/** Prefer mapped customer (any casing); else raw cells (tbl_mappings / CSV headers). */
function customerTextForIndevin(mapped: MappedRow, raw: Record<string, unknown>): string {
  const m = customerFromMapped(mapped);
  if (m != null) return m;
  for (const key of ['customer', 'client name']) {
    const v = raw[key];
    if (v != null && String(v).trim() !== '') return String(v);
  }
  for (const [k, v] of Object.entries(raw)) {
    const nk = normalizeHeader(k);
    if ((nk === 'customer' || nk === 'client name') && v != null && String(v).trim() !== '') {
      return String(v);
    }
  }
  return '';
}

function loadSizeFromMapped(mapped: MappedRow): unknown {
  const rec = mapped as Record<string, unknown>;
  const fromM = mapped.loadsize ?? rec['load_size'];
  if (fromM != null && String(fromM).trim() !== '') return fromM;
  for (const k of Object.keys(mapped)) {
    const kl = k.toLowerCase();
    if (kl === 'loadsize' || kl === 'load_size') {
      const v = rec[k];
      if (v != null && String(v).trim() !== '') return v;
    }
  }
  return null;
}

function loadSizeValueForBackfill(mapped: MappedRow, raw: Record<string, unknown>): unknown {
  const fromM = loadSizeFromMapped(mapped);
  if (fromM != null) return fromM;
  for (const key of ['load size', 'loadsize']) {
    const v = raw[key];
    if (v != null && String(v).trim() !== '') return v;
  }
  for (const [k, v] of Object.entries(raw)) {
    if (normalizeHeader(k) === 'load size' && v != null && String(v).trim() !== '') return v;
  }
  return null;
}

export type BackfillListResponse =
  | {
      ok: true;
      driveFolderId: string;
      filesTotal: number;
      files: { id: string; name: string }[];
    }
  | { error: string };

export type BackfillFileResponse =
  | {
      ok: true;
      driveFolderId: string;
      fileIndex: number;
      filesTotal: number;
      fileName: string;
      driveFileId: string;
      rowsInFile: number;
      rowsIndevinCustomer: number;
      rowsUpdated: number;
      rowsSkippedNoJobId: number;
      rowsSkippedNoLoadSize: number;
      rowsNotFoundInDb: number;
      errors: string[];
      errorsTruncated: boolean;
      done: boolean;
    }
  | { error: string };

function sortFiles<T extends { name: string }>(files: T[]): T[] {
  return [...files].sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
}

export async function vworkLoadsizeBackfillList(folderId: string): Promise<BackfillListResponse> {
  if (!folderId?.trim()) return { error: 'folderId is required' };
  try {
    const raw = await listImportableFiles(folderId.trim());
    const files = sortFiles(raw);
    return {
      ok: true,
      driveFolderId: folderId.trim(),
      filesTotal: files.length,
      files: files.map((f) => ({ id: f.id, name: f.name })),
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.file({ folderId, error: msg }, 'vwork loadsize backfill: list failed');
    return { error: `Drive list failed: ${msg}` };
  }
}

export async function vworkLoadsizeBackfillProcessFile(
  folderId: string,
  fileIndex: number
): Promise<BackfillFileResponse> {
  if (!folderId?.trim()) return { error: 'folderId is required' };
  if (!Number.isInteger(fileIndex) || fileIndex < 0) {
    return { error: 'fileIndex must be a non-negative integer' };
  }

  const pool = getPool();
  const client = await pool.connect();
  try {
    let files;
    try {
      files = sortFiles(await listImportableFiles(folderId.trim()));
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { error: `Drive list failed: ${msg}` };
    }

    if (files.length === 0) {
      return { error: 'No export CSV files found in folder' };
    }
    if (fileIndex >= files.length) {
      return { error: `fileIndex ${fileIndex} is out of range (0..${files.length - 1})` };
    }

    const file = files[fileIndex]!;
    const mappings = await getMappings(client, 'VW');
    const mappingDbcolumnNames = await getVworkMappingDbcolumnNames(client);
    const h2c = filterHeaderToColumnByMappingTargets(mappings.headerToColumn, mappingDbcolumnNames);
    let buffer: Buffer;
    try {
      buffer = await downloadFile(file.id);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { error: `Download failed for ${file.name}: ${msg}` };
    }

    let mapped: MappedRow[];
    let rawRows: Record<string, unknown>[];
    try {
      const parsed = await parseFile(buffer, file.name, h2c);
      mapped = parsed.mapped;
      rawRows = parsed.raw;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { error: `Parse failed for ${file.name}: ${msg}` };
    }

    let rowsIndevinCustomer = 0;
    let rowsUpdated = 0;
    let rowsSkippedNoJobId = 0;
    let rowsSkippedNoLoadSize = 0;
    let rowsNotFoundInDb = 0;
    const errors: string[] = [];

    for (let i = 0; i < mapped.length; i++) {
      const row = mapped[i]!;
      const rawRow = rawRows[i] ?? {};
      try {
        const custText = customerTextForIndevin(row, rawRow);
        const custLower = custText.toLowerCase();
        if (!custLower.includes(INDEVIN_SUBSTR)) continue;
        rowsIndevinCustomer++;

        const jobId = getVworkJobPkString(row, mappings.columnMaxLengths);
        if (!jobId) {
          rowsSkippedNoJobId++;
          continue;
        }

        const loadRaw = loadSizeValueForBackfill(row, rawRow);
        const loadNum = parseLoadSizeNumeric(loadRaw);
        if (loadNum == null) {
          rowsSkippedNoLoadSize++;
          continue;
        }

        const n = await updateVworkJobLoadsizeOnly(client, jobId, loadNum);
        if (n === 0) rowsNotFoundInDb++;
        else rowsUpdated++;
      } catch (rowErr) {
        errors.push(`Row ${i + 1}: ${rowErr instanceof Error ? rowErr.message : String(rowErr)}`);
      }
    }

    const logPayload = {
      fileIndex,
      file: file.name,
      rowsInFile: mapped.length,
      rowsIndevinCustomer,
      rowsUpdated,
      rowsSkippedNoJobId,
      rowsSkippedNoLoadSize,
      rowsNotFoundInDb,
      errorCount: errors.length,
    };
    await insertLog(
      client,
      'DataFix',
      'vwork-loadsize-backfill',
      file.name,
      JSON.stringify(logPayload)
    ).catch(() => {});

    const maxErr = 50;
    return {
      ok: true,
      driveFolderId: folderId.trim(),
      fileIndex,
      filesTotal: files.length,
      fileName: file.name,
      driveFileId: file.id,
      rowsInFile: mapped.length,
      rowsIndevinCustomer,
      rowsUpdated,
      rowsSkippedNoJobId,
      rowsSkippedNoLoadSize,
      rowsNotFoundInDb,
      errors: errors.slice(0, maxErr),
      errorsTruncated: errors.length > maxErr,
      done: fileIndex >= files.length - 1,
    };
  } finally {
    client.release();
  }
}

/** Smoke-test payload: parse real exports (row budget) using same Customer resolution + indevin substring as backfill. */
export type SampleScanResponse =
  | {
      ok: true;
      driveFolderId: string;
      filesTotal: number;
      filesTouched: number;
      rowsScanned: number;
      rowsWithCustomerText: number;
      indevinMatchCount: number;
      firstMatchCustomerPreview: string | null;
      maxRowsBudget: number;
      /** If set, scan stopped after this many files (Quick test). */
      maxFilesBudget: number | null;
      skippedNoFiles: boolean;
    }
  | { error: string };

/**
 * Download export CSVs in name order until row and/or file budget is used; count rows where Customer/Client name contains "indevin" (case-insensitive).
 * Uses tbl_mappings-only header map + customerTextForIndevin (same as backfill). Read-only.
 * @param maxFilesBudget — when set (e.g. 2 for UI smoke test), do not open more than this many files even if row budget remains.
 */
export async function vworkLoadsizeBackfillSampleScan(
  folderId: string,
  maxRowsBudget = 15000,
  maxFilesBudget?: number | null
): Promise<SampleScanResponse> {
  const cap = Math.min(Math.max(1, Math.floor(maxRowsBudget)), 50_000);
  let fileLimit: number | undefined;
  if (maxFilesBudget != null && Number.isFinite(maxFilesBudget)) {
    fileLimit = Math.min(500, Math.max(1, Math.floor(maxFilesBudget)));
  }
  if (!folderId?.trim()) return { error: 'folderId is required' };

  const pool = getPool();
  const client = await pool.connect();
  try {
    const mappings = await getMappings(client, 'VW');
    const mappingDbcolumnNames = await getVworkMappingDbcolumnNames(client);
    const h2c = filterHeaderToColumnByMappingTargets(mappings.headerToColumn, mappingDbcolumnNames);

    let filesList;
    try {
      filesList = sortFiles(await listImportableFiles(folderId.trim()));
    } catch (err) {
      return { error: `Drive list failed: ${err instanceof Error ? err.message : String(err)}` };
    }

    if (filesList.length === 0) {
      return {
        ok: true,
        driveFolderId: folderId.trim(),
        filesTotal: 0,
        filesTouched: 0,
        rowsScanned: 0,
        rowsWithCustomerText: 0,
        indevinMatchCount: 0,
        firstMatchCustomerPreview: null,
        maxRowsBudget: cap,
        maxFilesBudget: fileLimit ?? null,
        skippedNoFiles: true,
      };
    }

    let rowsScanned = 0;
    let rowsWithCustomerText = 0;
    let indevinMatchCount = 0;
    let firstMatchCustomerPreview: string | null = null;
    let filesTouched = 0;

    for (const file of filesList) {
      if (rowsScanned >= cap) break;
      if (fileLimit != null && filesTouched >= fileLimit) break;

      logger.file(
        {
          sample: 'vwork-loadsize',
          fileName: file.name,
          fileOrdinal: filesTouched + 1,
          rowsScannedSoFar: rowsScanned,
          maxRowsBudget: cap,
          maxFilesBudget: fileLimit ?? null,
        },
        'sample scan: download export'
      );

      let buffer: Buffer;
      try {
        buffer = await downloadFile(file.id);
      } catch (err) {
        return {
          error: `Sample scan: download failed for ${file.name}: ${err instanceof Error ? err.message : String(err)}`,
        };
      }

      let mapped: MappedRow[];
      let rawRows: Record<string, unknown>[];
      try {
        const parsed = await parseFile(buffer, file.name, h2c);
        mapped = parsed.mapped;
        rawRows = parsed.raw;
      } catch (err) {
        return {
          error: `Sample scan: parse failed for ${file.name}: ${err instanceof Error ? err.message : String(err)}`,
        };
      }

      filesTouched += 1;
      for (let i = 0; i < mapped.length && rowsScanned < cap; i++) {
        const row = mapped[i]!;
        const rawRow = rawRows[i] ?? {};
        rowsScanned += 1;
        const cust = customerTextForIndevin(row, rawRow);
        const trimmed = cust.trim();
        if (trimmed.length > 0) {
          rowsWithCustomerText += 1;
          if (trimmed.toLowerCase().includes(INDEVIN_SUBSTR)) {
            indevinMatchCount += 1;
            if (!firstMatchCustomerPreview) {
              firstMatchCustomerPreview = trimmed.length > 160 ? `${trimmed.slice(0, 160)}…` : trimmed;
            }
          }
        }
      }
    }

    return {
      ok: true,
      driveFolderId: folderId.trim(),
      filesTotal: filesList.length,
      filesTouched,
      rowsScanned,
      rowsWithCustomerText,
      indevinMatchCount,
      firstMatchCustomerPreview,
      maxRowsBudget: cap,
      maxFilesBudget: fileLimit ?? null,
      skippedNoFiles: false,
    };
  } finally {
    client.release();
  }
}
