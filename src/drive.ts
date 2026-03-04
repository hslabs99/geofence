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
 * List files in a Drive folder for vwork import.
 * Returns only .csv files whose filename contains "export" (case-insensitive), e.g. full_export_200226.csv.
 */
export async function listImportableFiles(folderId: string): Promise<DriveFileInfo[]> {
  const driveClient = await getDriveClient();
  console.log('[vwork] Scanning folder...');
  const res = await driveClient.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'files(id, name, mimeType)',
    pageSize: 500,
  });

  if (!res.data.files) {
    console.log('[vwork] No files in folder');
    logger.file({ folderId, count: 0 }, 'Drive list: no files');
    return [];
  }

  const allNames = res.data.files
    .filter((f): f is drive_v3.Schema$File & { id: string; name: string } => Boolean(f.id && f.name))
    .map((f) => f.name);
  const csvMime = 'text/csv';

  const files = res.data.files
    .filter((f): f is drive_v3.Schema$File & { id: string; name: string } =>
      Boolean(f.id && f.name))
    .filter((f) => {
      const lower = f.name.toLowerCase();
      if (f.mimeType === 'application/vnd.google-apps.folder') return false;
      if (f.mimeType === 'application/vnd.google-apps.spreadsheet') return false;
      if (!lower.endsWith('.csv') && f.mimeType !== csvMime) return false;
      if (!lower.includes('export')) return false;
      return true;
    })
    .map((f) => ({ id: f.id!, name: f.name!, mimeType: f.mimeType ?? '' }));

  if (files.length === 0) {
    console.log(`[vwork] No *export*.csv files found. Folder has ${allNames.length} item(s): ${allNames.slice(0, 20).join(', ')}${allNames.length > 20 ? '...' : ''}`);
    logger.file({ folderId, totalInFolder: allNames.length, namesInFolder: allNames.slice(0, 30) }, 'Drive list: no *export*.csv files');
  } else {
    console.log(`[vwork] Found ${files.length} file(s): ${files.map((f) => f.name).join(', ')}`);
  }
  logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list done');
  return files;
}

/**
 * List files in a Drive folder for gpsdata (auto2026) import.
 * Returns .xls, .xlsx and .csv files whose filename contains "auto2026" (case-insensitive).
 */
export async function listAuto2026Files(folderId: string): Promise<DriveFileInfo[]> {
  const driveClient = await getDriveClient();
  console.log('[geo] Scanning folder...');
  const res = await driveClient.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'files(id, name, mimeType)',
    pageSize: 500,
  });

  if (!res.data.files) {
    console.log('[geo] No files in folder');
    logger.file({ folderId, count: 0 }, 'Drive list (auto2026): no files');
    return [];
  }

  const xlsxMime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  const xlsMime = 'application/vnd.ms-excel';
  const csvMime = 'text/csv';

  const files = res.data.files
    .filter((f): f is drive_v3.Schema$File & { id: string; name: string } =>
      Boolean(f.id && f.name))
    .filter((f) => {
      const lower = f.name.toLowerCase();
      if (!lower.includes('auto2026')) return false;
      if (f.mimeType === 'application/vnd.google-apps.folder') return false;
      if (f.mimeType === 'application/vnd.google-apps.spreadsheet') return false;
      if (f.mimeType === xlsxMime || lower.endsWith('.xlsx')) return true;
      if (f.mimeType === xlsMime || lower.endsWith('.xls')) return true;
      if (f.mimeType === csvMime || lower.endsWith('.csv')) return true;
      return false;
    })
    .map((f) => ({ id: f.id!, name: f.name!, mimeType: f.mimeType ?? '' }));

  if (files.length === 0) {
    console.log('[geo] No auto2026 files found (.xls, .xlsx, .csv with "auto2026" in name)');
  } else {
    console.log(`[geo] Found ${files.length} file(s): ${files.map((f) => f.name).join(', ')}`);
  }
  logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list (auto2026) done');
  return files;
}

/**
 * List files in a Drive folder for GPS Tracking import.
 * Returns .xls and .xlsx files whose filename contains "track" (case-insensitive).
 * Example: Track Details_20260223095117.xls
 */
export async function listTrackFiles(folderId: string): Promise<DriveFileInfo[]> {
  const driveClient = await getDriveClient();
  console.log('[tracking] Scanning folder...');
  const res = await driveClient.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'files(id, name, mimeType)',
    pageSize: 500,
  });

  if (!res.data.files) {
    console.log('[tracking] No files in folder');
    logger.file({ folderId, count: 0 }, 'Drive list (track): no files');
    return [];
  }

  const xlsxMime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  const xlsMime = 'application/vnd.ms-excel';

  const files = res.data.files
    .filter((f): f is drive_v3.Schema$File & { id: string; name: string } =>
      Boolean(f.id && f.name))
    .filter((f) => {
      const lower = f.name.toLowerCase();
      if (!lower.includes('track')) return false;
      if (f.mimeType === 'application/vnd.google-apps.folder') return false;
      if (f.mimeType === 'application/vnd.google-apps.spreadsheet') return false;
      if (f.mimeType === xlsxMime || lower.endsWith('.xlsx')) return true;
      if (f.mimeType === xlsMime || lower.endsWith('.xls')) return true;
      return false;
    })
    .map((f) => ({ id: f.id!, name: f.name!, mimeType: f.mimeType ?? '' }));

  if (files.length === 0) {
    console.log('[tracking] No track files found (.xls, .xlsx with "track" in name)');
  } else {
    console.log(`[tracking] Found ${files.length} file(s): ${files.map((f) => f.name).join(', ')}`);
  }
  logger.file({ folderId, count: files.length, names: files.map((f) => f.name) }, 'Drive list (track) done');
  return files;
}

/**
 * Download file bytes via files.get with alt=media.
 */
export async function downloadFile(fileId: string): Promise<Buffer> {
  const driveClient = await getDriveClient();
  console.log('[geo] Downloading file...');
  logger.file({ fileId }, 'Drive download start');
  try {
    const res = await driveClient.files.get(
      { fileId, alt: 'media' },
      { responseType: 'arraybuffer' }
    );
    const buf = Buffer.from(res.data as ArrayBuffer);
    console.log(`[geo] Download ok: ${buf.length} bytes`);
    logger.file({ fileId, size: buf.length }, 'Drive download ok');
    return buf;
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    console.log('[geo] Download failed:', errMsg);
    logger.file({ fileId, error: errMsg }, 'Drive download failed');
    throw err;
  }
}
