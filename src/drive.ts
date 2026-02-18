import { google } from 'googleapis';
import { drive_v3 } from 'googleapis/build/src/apis/drive/v3';
import { logger } from './logger';

let drive: drive_v3.Drive | null = null;

export async function getDriveClient(): Promise<drive_v3.Drive> {
  if (drive) return drive;
  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  drive = google.drive({ version: 'v3', auth });
  return drive;
}

export interface DriveFileInfo {
  id: string;
  name: string;
  mimeType: string;
}

/**
 * List files in a Drive folder. Returns only .xlsx and .csv files.
 * Ignores Google Sheets (application/vnd.google-apps.spreadsheet) and folders.
 */
export async function listImportableFiles(folderId: string): Promise<DriveFileInfo[]> {
  const driveClient = await getDriveClient();
  const res = await driveClient.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'files(id, name, mimeType)',
    pageSize: 500,
  });

  if (!res.data.files) {
    logger.file({ folderId, count: 0 }, 'Drive list: no files');
    return [];
  }

  const xlsxMime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  const csvMime = 'text/csv';

  const files = res.data.files
    .filter((f): f is drive_v3.Schema$File & { id: string; name: string } =>
      Boolean(f.id && f.name))
    .filter((f) => {
      if (f.mimeType === 'application/vnd.google-apps.folder') return false;
      if (f.mimeType === 'application/vnd.google-apps.spreadsheet') return false;
      const lower = f.name.toLowerCase();
      if (f.mimeType === xlsxMime || lower.endsWith('.xlsx')) return true;
      if (f.mimeType === csvMime || lower.endsWith('.csv')) return true;
      return false;
    })
    .map((f) => ({ id: f.id!, name: f.name!, mimeType: f.mimeType ?? '' }));

  logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list done');
  return files;
}

/**
 * Download file bytes via files.get with alt=media.
 */
export async function downloadFile(fileId: string): Promise<Buffer> {
  const driveClient = await getDriveClient();
  logger.file({ fileId }, 'Drive download start');
  try {
    const res = await driveClient.files.get(
      { fileId, alt: 'media' },
      { responseType: 'arraybuffer' }
    );
    const buf = Buffer.from(res.data as ArrayBuffer);
    logger.file({ fileId, size: buf.length }, 'Drive download ok');
    return buf;
  } catch (err) {
    logger.file({ fileId, error: err instanceof Error ? err.message : String(err) }, 'Drive download failed');
    throw err;
  }
}
