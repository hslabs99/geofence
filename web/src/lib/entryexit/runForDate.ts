/**
 * Run entry/exit tagging for a date range and one or more devices.
 * Single window: 00:00 on fromDate to midnight on toDate (inclusive of toDate), plus buffer at each end.
 * Reset ENTER/EXIT in that window, scan that window, run state machine (no per-day or write-window rules), batch update.
 */

import { query, execute } from '@/lib/db';
import { runStateMachine, type ScanRow, type TagUpdate } from './stateMachine';
import { taggingRange } from './dateWindow';

const ALGO_VERSION = '2.1';

const DEFAULT_BUFFER_HOURS = 1;

export interface DeviceResult {
  deviceName: string;
  rowsRead: number;
  updatesWritten: number;
  status: 'ok' | 'error';
  errorMessage?: string;
  startedAt: string;
  endedAt: string;
  durationMs: number;
  logId?: number | string;
}

export interface RunOptions {
  /** Start of range (YYYY-MM-DD). */
  dateFrom: string;
  /** End of range (YYYY-MM-DD), inclusive. */
  dateTo: string;
  deviceNames: string[];
  graceSeconds: number;
  /** Buffer in hours at each end of the range (default 1). */
  bufferHours?: number;
  onProgress?: (event: ProgressEvent) => void;
}

export type ProgressEvent =
  | { stage: 'reset'; message: string }
  | { stage: 'device_start'; deviceName: string; index: number; total: number }
  | { stage: 'device_scan'; deviceName: string; rowsRead: number }
  | { stage: 'device_update'; deviceName: string; updatesWritten: number }
  | { stage: 'device_log'; deviceName: string; logId: number | string | undefined }
  | { stage: 'device_done'; deviceName: string; result: DeviceResult }
  | { stage: 'done'; summary: RunSummary }
  | { stage: 'error'; message: string };

export interface RunSummary {
  dateFrom: string;
  dateTo: string;
  windowStart: string;
  windowEnd: string;
  deviceCount: number;
  totalRowsRead: number;
  totalUpdatesWritten: number;
  results: DeviceResult[];
  startedAt: string;
  endedAt: string;
  durationMs: number;
}

export async function runForDate(options: RunOptions): Promise<RunSummary> {
  const {
    dateFrom,
    dateTo,
    deviceNames,
    graceSeconds,
    bufferHours = DEFAULT_BUFFER_HOURS,
    onProgress,
  } = options;
  const startedAt = new Date().toISOString();
  const { start: windowStart, end: windowEnd } = taggingRange(dateFrom, dateTo, bufferHours);

  const results: DeviceResult[] = [];
  let totalRowsRead = 0;
  let totalUpdatesWritten = 0;

  onProgress?.({
    stage: 'reset',
    message: `Resetting ENTER/EXIT for ${dateFrom} to ${dateTo} (window ${windowStart} → ${windowEnd})…`,
  });
  for (const deviceName of deviceNames) {
    await execute(
      `UPDATE tbl_tracking SET geofence_type = NULL
       WHERE device_name = $1 AND position_time_nz >= $2 AND position_time_nz < $3
       AND geofence_type IN ('ENTER','EXIT','CHANGE')`,
      [deviceName.trim(), windowStart, windowEnd]
    );
  }

  for (let i = 0; i < deviceNames.length; i++) {
    const deviceName = deviceNames[i].trim();
    const deviceStartedAt = new Date().toISOString();
    onProgress?.({
      stage: 'device_start',
      deviceName,
      index: i + 1,
      total: deviceNames.length,
    });

    let rowsRead = 0;
    let updatesWritten = 0;
    let logId: number | string | undefined;

    try {
      const rows = await query<ScanRow>(
        `SELECT id, position_time_nz, geofence_id
         FROM tbl_tracking
         WHERE device_name = $1 AND position_time_nz >= $2 AND position_time_nz < $3
         ORDER BY position_time_nz, id`,
        [deviceName, windowStart, windowEnd]
      );
      rowsRead = rows?.length ?? 0;
      totalRowsRead += rowsRead;
      onProgress?.({ stage: 'device_scan', deviceName, rowsRead });

      const updates: TagUpdate[] = runStateMachine(rows ?? [], graceSeconds);
      updatesWritten = updates.length;
      totalUpdatesWritten += updatesWritten;
      onProgress?.({ stage: 'device_update', deviceName, updatesWritten });

      if (updates.length > 0) {
        for (const u of updates) {
          const id = typeof u.id === 'bigint' ? Number(u.id) : u.id;
          const type = u.geofence_type;
          await execute('UPDATE tbl_tracking SET geofence_type = $1 WHERE id = $2', [type, id]);
        }
      }

      const deviceEndedAt = new Date().toISOString();
      const durationMs = new Date(deviceEndedAt).getTime() - new Date(deviceStartedAt).getTime();
      const logDetails = JSON.stringify({
        date_from: dateFrom,
        date_to: dateTo,
        window_start: windowStart,
        window_end: windowEnd,
        rows_read: rowsRead,
        updates_written: updatesWritten,
        grace_seconds: graceSeconds,
        buffer_hours: bufferHours,
        status: 'ok',
        started_at: deviceStartedAt,
        ended_at: deviceEndedAt,
        duration_ms: durationMs,
        algo_version: ALGO_VERSION,
      });
      const logRows = await query<{ logid: number | string }>(
        'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4) RETURNING logid',
        ['entryexit', deviceName, `${dateFrom}_${dateTo}`, logDetails]
      );
      const log = logRows[0];
      logId = log != null ? (typeof log.logid === 'bigint' ? Number(log.logid) : log.logid) : undefined;
      onProgress?.({ stage: 'device_log', deviceName, logId });

      results.push({
        deviceName,
        rowsRead,
        updatesWritten,
        status: 'ok',
        startedAt: deviceStartedAt,
        endedAt: deviceEndedAt,
        durationMs,
        logId,
      });
      onProgress?.({ stage: 'device_done', deviceName, result: results[results.length - 1] });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const deviceEndedAt = new Date().toISOString();
      const durationMs = new Date(deviceEndedAt).getTime() - new Date(deviceStartedAt).getTime();
      const logDetails = JSON.stringify({
        date_from: dateFrom,
        date_to: dateTo,
        window_start: windowStart,
        window_end: windowEnd,
        rows_read: rowsRead,
        updates_written: updatesWritten,
        grace_seconds: graceSeconds,
        buffer_hours: bufferHours,
        status: 'error',
        error_message: message,
        started_at: deviceStartedAt,
        ended_at: deviceEndedAt,
        duration_ms: durationMs,
        algo_version: ALGO_VERSION,
      });
      try {
        const logRows = await query<{ logid: number | string }>(
          'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4) RETURNING logid',
          ['entryexit', deviceName, `${dateFrom}_${dateTo}`, logDetails]
        );
        const log = logRows[0];
        logId = log != null ? (typeof log.logid === 'bigint' ? Number(log.logid) : log.logid) : undefined;
      } catch {
        // ignore log failure
      }
      results.push({
        deviceName,
        rowsRead,
        updatesWritten,
        status: 'error',
        errorMessage: message,
        startedAt: deviceStartedAt,
        endedAt: deviceEndedAt,
        durationMs,
        logId,
      });
      onProgress?.({ stage: 'error', message });
    }
  }

  const endedAt = new Date().toISOString();
  const durationMs = new Date(endedAt).getTime() - new Date(startedAt).getTime();
  const summary: RunSummary = {
    dateFrom,
    dateTo,
    windowStart,
    windowEnd,
    deviceCount: deviceNames.length,
    totalRowsRead,
    totalUpdatesWritten,
    results,
    startedAt,
    endedAt,
    durationMs,
  };
  onProgress?.({ stage: 'done', summary });
  return summary;
}
