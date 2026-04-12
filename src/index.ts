import 'dotenv/config';
import express, { Request, Response } from 'express';
import { getPool } from './db';
import { logger } from './logger';
import {
  getNextBatchNumber,
  getNextGpsDataBatchNumber,
  getMappings,
  isFileAlreadyProcessed,
  insertImportFile,
  updateImportFileSuccess,
  updateImportFileError,
  upsertVworkJob,
  getTblVworkjobsColumns,
  getVworkMappingDbcolumnNames,
  buildVworkAllowedInsertColumns,
  stripMappedVworkRowToAllowedColumns,
  enforceVworkMappedRowMappingTargetsOnly,
  insertGpsDataRow,
  insertTrackingRow,
  insertLog,
  type TrackingMappedRow,
} from './db';
import { listImportableFiles, listAuto2026Files, listTrackFiles, downloadFile } from './drive';
import { filterHeaderToColumnByMappingTargets } from './mapping';
import { parseFile, parseFileForGpsData, parseFileForTracking } from './parser';
import {
  vworkLoadsizeBackfillList,
  vworkLoadsizeBackfillProcessFile,
  vworkLoadsizeBackfillSampleScan,
} from './vwork-loadsize-backfill';

const app = express();
app.use(express.json());

const PORT = parseInt(process.env.PORT ?? '8080', 10);

const DEFAULT_DRIVE_FOLDER_ID = '1EcWF7MEx6hi3unN4TTh9z7PuUyiZTgZD';

type ResultItem = {
  file: string;
  status: string;
  rows?: number;
  error?: string;
  /** Mapped export columns not stored: not on `tbl_vworkjobs` or dropped after PG 42703 during this run. */
  omittedDbColumns?: string[];
  /** CSV headers (normalized) with no mapping — values ignored. */
  unmappedCsvHeaders?: string[];
  /** `dbcolumnname` in tbl_mappings (VW) with no column on the server table yet. */
  mappingTargetsMissingFromTable?: string[];
  /** Rows that failed after parse (DB/other); other rows in the file still commit. */
  rowFailureCount?: number;
  rowFailureSamples?: { row: number; error: string }[];
};

app.get('/health', (_req: Request, res: Response) => {
  res.json({ ok: true });
});

app.get('/debug', async (_req: Request, res: Response) => {
  const dbSet = Boolean(
    process.env.PGHOST || process.env.PGPASSWORD || process.env.PGDATABASE || process.env.PGUSER
  );
  const folderId = process.env.DRIVE_FOLDER_ID ?? DEFAULT_DRIVE_FOLDER_ID;

  let dbTest: { ok: boolean; error?: string } = { ok: false };
  try {
    const pool = getPool();
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    dbTest = { ok: true };
  } catch (err) {
    dbTest = { ok: false, error: err instanceof Error ? err.message : String(err) };
  }

  let driveTest: { ok: boolean; fileCount?: number; error?: string } = { ok: false };
  try {
    const files = await listImportableFiles(folderId);
    driveTest = { ok: true, fileCount: files.length };
  } catch (err) {
    driveTest = { ok: false, error: err instanceof Error ? err.message : String(err) };
  }

  res.json({
    env: {
      PGHOST: process.env.PGHOST ?? '(not set, default localhost)',
      PGPORT: process.env.PGPORT ?? '(not set, default 5432)',
      PGDATABASE: process.env.PGDATABASE ?? '(not set, default geodata)',
      PGUSER: process.env.PGUSER ?? '(not set, default geofence)',
      PGPASSWORD: process.env.PGPASSWORD ? '***set***' : '(not set)',
      DRIVE_FOLDER_ID: process.env.DRIVE_FOLDER_ID ?? '(not set, using default)',
      GOOGLE_APPLICATION_CREDENTIALS: process.env.GOOGLE_APPLICATION_CREDENTIALS ?? '(not set, using ADC)',
    },
    dbConfigured: dbSet || Boolean(process.env.PGPASSWORD),
    defaultDriveFolderId: DEFAULT_DRIVE_FOLDER_ID,
    dbTest,
    driveTest,
    hint: !driveTest.ok && driveTest.error?.includes('Insufficient')
      ? 'Run: gcloud auth application-default login (with headstartgeodata@gmail.com active)'
      : undefined,
  });
});

async function runVworkImport(folderIdParam?: string): Promise<{ ok: boolean; results: ResultItem[] } | { error: string }> {
  const folderId = folderIdParam ?? process.env.DRIVE_FOLDER_ID ?? DEFAULT_DRIVE_FOLDER_ID;
  logger.db({ folderId, source: folderIdParam ? 'body' : process.env.DRIVE_FOLDER_ID ? 'env' : 'default' }, 'runVworkImport start');
  if (!folderId) return { error: 'DRIVE_FOLDER_ID not set' };

  const pool = getPool();
  let client;
  try {
    client = await pool.connect();
    logger.db({}, 'DB connected for vwork');
  } catch (dbErr) {
    const msg = dbErr instanceof Error ? dbErr.message : String(dbErr);
    logger.db({ error: msg }, 'DB connection failed');
    return { error: `DB connection failed: ${msg}` };
  }
  try {
    let files;
    try {
      files = await listImportableFiles(folderId);
      logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list done');
    } catch (driveErr) {
      const msg = driveErr instanceof Error ? driveErr.message : String(driveErr);
      logger.file({ folderId, error: msg }, 'Drive list failed');
      return { error: `Drive list failed: ${msg}` };
    }
    if (files.length === 0) {
      await insertLog(client, 'Import', 'VW', null, JSON.stringify({ status: 'no_files', message: 'No *export*.csv files found in folder' }));
      return { ok: true, results: [{ file: '(none)', status: 'no_files', rows: 0 }] };
    }
    const mappings = await getMappings(client, 'VW');
    const mappingDbcolumnNames = await getVworkMappingDbcolumnNames(client);
    /** Mutable copy: 42703 may drop a name for the rest of the run (same as table column set). */
    const mappingColsForRun = new Set(mappingDbcolumnNames);
    const vworkTableColumns = await getTblVworkjobsColumns(client);
    /** tbl_mappings targets ∩ physical columns (mutable if INSERT hits 42703). */
    const allowedColsForRun = buildVworkAllowedInsertColumns(mappingDbcolumnNames, vworkTableColumns);
    const mappingTargetsMissingFromTable = [...mappingDbcolumnNames]
      .filter((c) => !vworkTableColumns.has(c))
      .sort();
    const results: ResultItem[] = [];

    for (const file of files) {
      try {
        const alreadyProcessed = await isFileAlreadyProcessed(client, file.id);
        if (alreadyProcessed) {
          results.push({ file: file.name, status: 'skipped' });
          await insertLog(client, 'Import', 'VW', file.name, JSON.stringify({ status: 'skipped' }));
          continue;
        }
        await client.query('BEGIN');
        await insertImportFile(client, file.id, file.name);
        await insertLog(client, 'Import', 'VW', file.name, JSON.stringify({ status: 'opening' }));
        logger.file({ filename: file.name, driveFileId: file.id }, 'parse start');
        const buffer = await downloadFile(file.id);
        /** Only `tbl_mappings` (VW): no hardcoded HEADER_TO_COLUMN merge — unknown CSV headers are ignored. */
        const h2c = filterHeaderToColumnByMappingTargets(
          mappings.headerToColumn,
          mappingDbcolumnNames
        );
        const { mapped, raw, unmappedCsvHeaders } = await parseFile(buffer, file.name, h2c);
        logger.file({ filename: file.name, rowCount: mapped.length, sampleHeaders: mapped[0] ? Object.keys(mapped[0]) : [] }, 'parse done');
        for (const row of mapped) {
          enforceVworkMappedRowMappingTargetsOnly(row, mappingDbcolumnNames);
        }
        const allMappedKeysFromFile = new Set<string>();
        for (const row of mapped) {
          for (const k of Object.keys(row)) {
            const low = k.toLowerCase();
            if (low === 'batchnumber' || low === 'raw_row') continue;
            allMappedKeysFromFile.add(low);
          }
        }
        for (const row of mapped) {
          stripMappedVworkRowToAllowedColumns(row, allowedColsForRun);
        }
        const batchnumber = await getNextBatchNumber(client);
        let inserted = 0;
        let rowFailureCount = 0;
        const rowFailureSamples: { row: number; error: string }[] = [];
        const MAX_FAILURE_SAMPLES = 40;
        for (let i = 0; i < mapped.length; i++) {
          const sp = `vwr_${i}`;
          await client.query(`SAVEPOINT ${sp}`);
          try {
            const ok = await upsertVworkJob(
              client,
              mapped[i]!,
              raw[i]!,
              batchnumber,
              allowedColsForRun,
              mappingColsForRun,
              mappings.columnMaxLengths
            );
            if (ok) inserted++;
            await client.query(`RELEASE SAVEPOINT ${sp}`);
          } catch (err) {
            await client.query(`ROLLBACK TO SAVEPOINT ${sp}`);
            await client.query(`RELEASE SAVEPOINT ${sp}`);
            rowFailureCount += 1;
            const msg = err instanceof Error ? err.message : String(err);
            logger.row(
              {
                filename: file.name,
                rowIndex: i + 1,
                pk: mapped[i]?.Job_Id ?? mapped[i]?.job_id,
                raw_preview: raw[i],
              },
              `row skipped: ${msg}`
            );
            if (rowFailureSamples.length < MAX_FAILURE_SAMPLES) {
              rowFailureSamples.push({ row: i + 1, error: msg });
            }
          }
        }
        const omittedDbColumns = [...allMappedKeysFromFile]
          .filter((k) => !allowedColsForRun.has(k))
          .sort();
        await updateImportFileSuccess(client, file.id, inserted, batchnumber);
        await insertLog(
          client,
          'Import',
          'VW',
          file.name,
          JSON.stringify({
            rows: inserted,
            status: 'processed',
            batchnumber,
            ...(omittedDbColumns.length ? { omittedDbColumns } : {}),
            ...(unmappedCsvHeaders.length ? { unmappedCsvHeaders } : {}),
            ...(mappingTargetsMissingFromTable.length
              ? { mappingTargetsMissingFromTable }
              : {}),
            ...(rowFailureCount ? { rowFailureCount, rowFailureSamples } : {}),
          })
        );
        await client.query('COMMIT');
        results.push({
          file: file.name,
          status: 'processed',
          rows: inserted,
          ...(omittedDbColumns.length ? { omittedDbColumns } : {}),
          ...(unmappedCsvHeaders.length ? { unmappedCsvHeaders } : {}),
          ...(mappingTargetsMissingFromTable.length
            ? { mappingTargetsMissingFromTable }
            : {}),
          ...(rowFailureCount ? { rowFailureCount, rowFailureSamples } : {}),
        });
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        const msg = err instanceof Error ? err.message : String(err);
        logger.error({ filename: file.name, driveFileId: file.id, error: msg }, 'file processing failed');
        await updateImportFileError(client, file.id, msg);
        await insertLog(client, 'Import', 'VW', file.name, JSON.stringify({ status: 'error', error: msg })).catch(() => {});
        results.push({ file: file.name, status: 'error', error: msg });
      }
    }
    return { ok: true, results };
  } finally {
    client.release();
  }
}

async function runGeoImport(folderIdParam?: string): Promise<{ ok: boolean; results: ResultItem[] } | { error: string }> {
  const folderId = folderIdParam ?? process.env.DRIVE_FOLDER_ID ?? DEFAULT_DRIVE_FOLDER_ID;
  console.log('[geo] runGeoImport start, folderId:', folderId);
  logger.db({ folderId, source: folderIdParam ? 'body' : process.env.DRIVE_FOLDER_ID ? 'env' : 'default' }, 'runGeoImport start');
  if (!folderId) {
    console.log('[geo] ERROR: DRIVE_FOLDER_ID not set');
    return { error: 'DRIVE_FOLDER_ID not set' };
  }

  const pool = getPool();
  let client;
  try {
    client = await pool.connect();
    console.log('[geo] DB connected');
    logger.db({}, 'DB connected for geo');
  } catch (dbErr) {
    const msg = dbErr instanceof Error ? dbErr.message : String(dbErr);
    console.log('[geo] DB connection failed:', msg);
    logger.db({ error: msg }, 'DB connection failed');
    return { error: `DB connection failed: ${msg}` };
  }
  try {
    let files;
    try {
      files = await listAuto2026Files(folderId);
      logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list (geo) done');
    } catch (driveErr) {
      const msg = driveErr instanceof Error ? driveErr.message : String(driveErr);
      console.log('[geo] Drive list failed:', msg);
      logger.file({ folderId, error: msg }, 'Drive list (geo) failed');
      return { error: `Drive list failed: ${msg}` };
    }
    if (files.length === 0) {
      console.log('[geo] No files to process, done');
      return { ok: true, results: [] };
    }
    console.log('[geo] Fetching GPS mappings from DB...');
    const mappings = await getMappings(client, 'GPS');
    const mappingCount = Object.keys(mappings.headerToColumn).length;
    const dbColumns = [...new Set(Object.values(mappings.headerToColumn))];
    console.log(`[geo] Mappings loaded: ${mappingCount} header mappings, ${dbColumns.length} db columns`);
    logger.db(
      { type: 'GPS', mappingCount, dbColumnCount: dbColumns.length, sampleColumns: dbColumns.slice(0, 10) },
      'GPS mappings loaded'
    );
    const results: ResultItem[] = [];

    for (const file of files) {
      try {
        console.log(`[geo] Processing file: ${file.name}`);
        const alreadyProcessed = await isFileAlreadyProcessed(client, file.id);
        if (alreadyProcessed) {
          console.log(`[geo] Skipped (already processed): ${file.name}`);
          results.push({ file: file.name, status: 'skipped' });
          await insertLog(client, 'Import', 'GPS', file.name, JSON.stringify({ status: 'skipped' }));
          continue;
        }
        await client.query('BEGIN');
        await insertImportFile(client, file.id, file.name);
        await insertLog(client, 'Import', 'GPS', file.name, JSON.stringify({ status: 'opening' }));
        console.log(`[geo] Opening file: ${file.name}`);
        logger.file({ filename: file.name, driveFileId: file.id }, 'gpsdata parse start');
        const buffer = await downloadFile(file.id);
        console.log(`[geo] Downloaded ${buffer.length} bytes, parsing...`);
        const { mapped, rawRowCount, sampleHeaders } = await parseFileForGpsData(buffer, file.name, mappings.headerToColumn);
        console.log(`[geo] Parsed ${mapped.length} rows (file had ${rawRowCount} raw rows)`);
        if (mapped.length === 0 && rawRowCount > 0) {
          const mappingKeys = Object.keys(mappings.headerToColumn).slice(0, 15);
          console.log(`[geo] No rows matched. File headers (first row): ${sampleHeaders.join(', ')}`);
          console.log(`[geo] Mapping keys (sample): ${mappingKeys.join(', ')}`);
          logger.file(
            { filename: file.name, rawRowCount, sampleHeaders, mappingKeysSample: mappingKeys },
            'gpsdata parse: 0 rows matched - header mismatch?'
          );
        }
        logger.file(
          { filename: file.name, rowCount: mapped.length, sampleColumns: mapped[0] ? Object.keys(mapped[0]) : [] },
          'gpsdata parse done'
        );
        const batchnumber = await getNextGpsDataBatchNumber(client);
        console.log(`[geo] Inserting ${mapped.length} rows (batch ${batchnumber})...`);
        logger.db({ filename: file.name, batchnumber, rowCount: mapped.length }, 'gpsdata insert start');
        let inserted = 0;
        for (let i = 0; i < mapped.length; i++) {
          try {
            if (await insertGpsDataRow(client, mapped[i], batchnumber, mappings.columnMaxLengths)) inserted++;
          } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            console.log(`[geo] Row ${i + 1} failed:`, errMsg);
            logger.row(
              { filename: file.name, rowIndex: i + 1, mapped: mapped[i] },
              `gpsdata row failed: ${errMsg}`
            );
            throw err;
          }
        }
        await updateImportFileSuccess(client, file.id, inserted, batchnumber);
        console.log(`[geo] Done: ${file.name} - inserted ${inserted} rows`);
        logger.file({ filename: file.name, batchnumber, inserted }, 'gpsdata insert done');
        await insertLog(client, 'Import', 'GPS', file.name, JSON.stringify({ rows: inserted, status: 'processed', batchnumber }));
        await client.query('COMMIT');
        results.push({ file: file.name, status: 'processed', rows: inserted });
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        const msg = err instanceof Error ? err.message : String(err);
        console.log(`[geo] File failed: ${file.name} -`, msg);
        logger.error({ filename: file.name, driveFileId: file.id, error: msg }, 'gpsdata file processing failed');
        await updateImportFileError(client, file.id, msg);
        await insertLog(client, 'Import', 'GPS', file.name, JSON.stringify({ status: 'error', error: msg })).catch(() => {});
        results.push({ file: file.name, status: 'error', error: msg });
      }
    }
    console.log('[geo] runGeoImport complete');
    return { ok: true, results };
  } finally {
    client.release();
  }
}

async function runTrackingImport(folderIdParam?: string): Promise<{ ok: boolean; results: ResultItem[] } | { error: string }> {
  const folderId = folderIdParam ?? process.env.DRIVE_FOLDER_ID ?? DEFAULT_DRIVE_FOLDER_ID;
  console.log('[tracking] runTrackingImport start, folderId:', folderId);
  logger.db({ folderId }, 'runTrackingImport start');
  if (!folderId) return { error: 'DRIVE_FOLDER_ID not set' };

  const pool = getPool();
  let client;
  try {
    client = await pool.connect();
    console.log('[tracking] DB connected');
  } catch (dbErr) {
    const msg = dbErr instanceof Error ? dbErr.message : String(dbErr);
    return { error: `DB connection failed: ${msg}` };
  }
  try {
    let files;
    try {
      files = await listTrackFiles(folderId);
      logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list (tracking) done');
    } catch (driveErr) {
      const msg = driveErr instanceof Error ? driveErr.message : String(driveErr);
      return { error: `Drive list failed: ${msg}` };
    }
    if (files.length === 0) {
      console.log('[tracking] No track files found');
      await insertLog(client, 'Import', 'TRACK', null, JSON.stringify({ status: 'no_files', message: 'No *track*.xls files found' }));
      return { ok: true, results: [{ file: '(none)', status: 'no_files', rows: 0 }] };
    }
    const results: ResultItem[] = [];

    for (const file of files) {
      try {
        const alreadyProcessed = await isFileAlreadyProcessed(client, file.id);
        if (alreadyProcessed) {
          console.log('[tracking] Skipped (already processed):', file.name);
          results.push({ file: file.name, status: 'skipped' });
          await insertLog(client, 'Import', 'TRACK', file.name, JSON.stringify({ status: 'skipped' }));
          continue;
        }
        await client.query('BEGIN');
        await insertImportFile(client, file.id, file.name);
        await insertLog(client, 'Import', 'TRACK', file.name, JSON.stringify({ status: 'opening' }));
        const buffer = await downloadFile(file.id);
        const { mapped } = await parseFileForTracking(buffer, file.name);
        console.log(`[tracking] Parsed ${mapped.length} rows from ${file.name}`);
        let inserted = 0;
        for (let i = 0; i < mapped.length; i++) {
          try {
            if (await insertTrackingRow(client, mapped[i] as TrackingMappedRow)) inserted++;
          } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            console.log(`[tracking] Row ${i + 1} failed:`, errMsg);
            throw err;
          }
        }
        await updateImportFileSuccess(client, file.id, inserted, 0);
        await insertLog(client, 'Import', 'TRACK', file.name, JSON.stringify({ rows: inserted, status: 'processed' }));
        await client.query('COMMIT');
        results.push({ file: file.name, status: 'processed', rows: inserted });
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        const msg = err instanceof Error ? err.message : String(err);
        console.log('[tracking] File failed:', file.name, msg);
        await updateImportFileError(client, file.id, msg);
        await insertLog(client, 'Import', 'TRACK', file.name, JSON.stringify({ status: 'error', error: msg })).catch(() => {});
        results.push({ file: file.name, status: 'error', error: msg });
      }
    }
    return { ok: true, results };
  } finally {
    client.release();
  }
}

app.get('/import/vwork', async (_req: Request, res: Response) => {
  try {
    const out = await runVworkImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});
app.post('/import/vwork', async (req: Request, res: Response) => {
  try {
    const body = (req.body ?? {}) as { driveFolderId?: string };
    const out = await runVworkImport(body.driveFolderId);
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});

/**
 * One-off / admin: re-scan all Drive export CSVs (ignores import_file skip) and UPDATE only loadsize
 * for rows where Customer contains "indevin" (case-insensitive).
 * POST JSON { action: "list" | "file" | "sample", driveFolderId?: string, fileIndex?: number, maxRows?: number }
 */
app.post('/import/vwork-backfill-loadsize', async (req: Request, res: Response) => {
  try {
    const body = (req.body ?? {}) as {
      action?: string;
      driveFolderId?: string;
      fileIndex?: number;
      maxRows?: number;
      maxFiles?: number;
    };
    const folderId =
      typeof body.driveFolderId === 'string' && body.driveFolderId.trim()
        ? body.driveFolderId.trim()
        : process.env.DRIVE_FOLDER_ID ?? DEFAULT_DRIVE_FOLDER_ID;

    if (body.action === 'list') {
      const out = await vworkLoadsizeBackfillList(folderId);
      if ('error' in out) {
        res.status(400).json(out);
        return;
      }
      res.json(out);
      return;
    }

    if (body.action === 'file') {
      const idx = body.fileIndex;
      if (idx == null || !Number.isInteger(idx)) {
        res.status(400).json({ error: 'fileIndex (integer) is required when action is "file"' });
        return;
      }
      const out = await vworkLoadsizeBackfillProcessFile(folderId, idx);
      if ('error' in out) {
        res.status(400).json(out);
        return;
      }
      res.json(out);
      return;
    }

    if (body.action === 'sample') {
      const rawMax = body.maxRows;
      const maxRows =
        typeof rawMax === 'number' && Number.isFinite(rawMax) ? Math.floor(rawMax) : 15_000;
      const rawMaxFiles = body.maxFiles;
      const maxFiles =
        typeof rawMaxFiles === 'number' && Number.isFinite(rawMaxFiles)
          ? Math.floor(rawMaxFiles)
          : undefined;
      const out = await vworkLoadsizeBackfillSampleScan(folderId, maxRows, maxFiles);
      if ('error' in out) {
        res.status(400).json(out);
        return;
      }
      res.json(out);
      return;
    }

    res.status(400).json({ error: 'action must be "list", "file", or "sample"' });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});

app.get('/import/geo', async (_req: Request, res: Response) => {
  res.setHeader('Content-Type', 'application/json');
  try {
    const out = await runGeoImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});
app.post('/import/geo', async (req: Request, res: Response) => {
  res.setHeader('Content-Type', 'application/json');
  try {
    const body = (req.body ?? {}) as { driveFolderId?: string };
    const out = await runGeoImport(body.driveFolderId);
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});

app.get('/import/tracking', async (_req: Request, res: Response) => {
  res.setHeader('Content-Type', 'application/json');
  try {
    const out = await runTrackingImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});
app.post('/import/tracking', async (req: Request, res: Response) => {
  res.setHeader('Content-Type', 'application/json');
  try {
    const body = (req.body ?? {}) as { driveFolderId?: string };
    const out = await runTrackingImport(body.driveFolderId);
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});

app.post('/run-import', async (_req: Request, res: Response) => {
  try {
    const out = await runVworkImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});
app.post('/run-import-gpsdata', async (_req: Request, res: Response) => {
  try {
    const out = await runGeoImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});
app.post('/run-import-tracking', async (_req: Request, res: Response) => {
  try {
    const out = await runTrackingImport();
    if ('error' in out) {
      res.status(500).json({ error: out.error });
      return;
    }
    res.json(out);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
});

app.listen(PORT, () => {
  console.log(`[import-service] Listening on port ${PORT}`);
  console.log(`[import-service] DEBUG: GET http://localhost:${PORT}/debug for env/connection info`);
  console.log(`[import-service] PGHOST=${process.env.PGHOST ?? 'localhost'}`);
  console.log(`[import-service] PGPASSWORD=${process.env.PGPASSWORD ? '***' : 'NOT SET'}`);
  console.log(`[import-service] DRIVE_FOLDER_ID=${process.env.DRIVE_FOLDER_ID ?? 'using default'}`);
  console.log(`[import-service] GOOGLE_APPLICATION_CREDENTIALS=${process.env.GOOGLE_APPLICATION_CREDENTIALS ?? 'using ADC'}`);
});
