import express, { Request, Response } from 'express';
import { getPool } from './db';
import { logger } from './logger';
import {
  getNextBatchNumber,
  isFileAlreadyProcessed,
  insertImportFile,
  updateImportFileSuccess,
  updateImportFileError,
  upsertVworkJob,
} from './db';
import { listImportableFiles, downloadFile } from './drive';
import { parseFile } from './parser';

const app = express();
app.use(express.json());

const PORT = parseInt(process.env.PORT ?? '8080', 10);

app.get('/health', (_req: Request, res: Response) => {
  res.json({ ok: true });
});

app.post('/run-import', async (_req: Request, res: Response) => {
  const folderId = process.env.DRIVE_FOLDER_ID;
  if (!folderId) {
    res.status(500).json({ error: 'DRIVE_FOLDER_ID not set' });
    return;
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    const files = await listImportableFiles(folderId);
    const results: { file: string; status: string; rows?: number; error?: string }[] = [];

    for (const file of files) {
      try {
        const alreadyProcessed = await isFileAlreadyProcessed(client, file.id);
        if (alreadyProcessed) {
          results.push({ file: file.name, status: 'skipped' });
          continue;
        }

        await client.query('BEGIN');

        await insertImportFile(client, file.id, file.name);

        logger.file({ filename: file.name, driveFileId: file.id }, 'parse start');
        const buffer = await downloadFile(file.id);
        const { mapped, raw } = await parseFile(buffer, file.name);
        logger.file({ filename: file.name, rowCount: mapped.length, sampleHeaders: mapped[0] ? Object.keys(mapped[0]) : [] }, 'parse done');

        const batchnumber = await getNextBatchNumber(client);

        let inserted = 0;
        for (let i = 0; i < mapped.length; i++) {
          try {
            await upsertVworkJob(client, mapped[i], raw[i], batchnumber);
            inserted++;
          } catch (err) {
            logger.row(
              { filename: file.name, rowIndex: i + 1, job_id: mapped[i]?.job_id, raw_preview: raw[i] },
              `row failed: ${err instanceof Error ? err.message : String(err)}`
            );
            throw err;
          }
        }

        await updateImportFileSuccess(client, file.id, inserted, batchnumber);
        await client.query('COMMIT');
        results.push({ file: file.name, status: 'processed', rows: inserted });
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        const msg = err instanceof Error ? err.message : String(err);
        logger.error({ filename: file.name, driveFileId: file.id, error: msg }, 'file processing failed');
        await updateImportFileError(client, file.id, msg);
        results.push({ file: file.name, status: 'error', error: msg });
      }
    }

    res.json({ ok: true, results });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: msg });
  } finally {
    client.release();
  }
});

app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});
