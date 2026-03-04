/**
 * Run entry/exit tagging for a single date and one or more devices.
 * - Reset ENTER/EXIT in write window
 * - Scan in extended window, run state machine, batch update, log per device
 */

import { prisma } from '@/lib/prisma';
import { runStateMachine, type ScanRow, type TagUpdate } from './stateMachine';
import { writeWindow, scanWindow } from './dateWindow';

const ALGO_VERSION = '1.0';

function escapeSql(s: string): string {
  return s.replace(/'/g, "''");
}

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
  dateX: string; // YYYY-MM-DD
  deviceNames: string[];
  graceSeconds: number;
  onProgress?: (event: ProgressEvent) => void;
}

export type ProgressEvent =
  | { stage: 'reset'; message: string }
  | { stage: 'device_start'; deviceName: string; index: number; total: number }
  | { stage: 'device_scan'; deviceName: string; rowsRead: number }
  | { stage: 'device_update'; deviceName: string; updatesWritten: number }
  | { stage: 'device_log'; deviceName: string; logId: number | string }
  | { stage: 'device_done'; deviceName: string; result: DeviceResult }
  | { stage: 'done'; summary: RunSummary }
  | { stage: 'error'; message: string };

export interface RunSummary {
  dateX: string;
  deviceCount: number;
  totalRowsRead: number;
  totalUpdatesWritten: number;
  results: DeviceResult[];
  startedAt: string;
  endedAt: string;
  durationMs: number;
}

export async function runForDate(options: RunOptions): Promise<RunSummary> {
  const { dateX, deviceNames, graceSeconds, onProgress } = options;
  const startedAt = new Date().toISOString();
  const { start: writeStart, end: writeEnd } = writeWindow(dateX);
  const { start: scanStart, end: scanEnd } = scanWindow(dateX);
  const escapedWriteStart = escapeSql(writeStart);
  const escapedWriteEnd = escapeSql(writeEnd);
  const escapedScanStart = escapeSql(scanStart);
  const escapedScanEnd = escapeSql(scanEnd);

  const results: DeviceResult[] = [];
  let totalRowsRead = 0;
  let totalUpdatesWritten = 0;

  // Reset: clear ENTER/EXIT (and legacy CHANGE) in write window for all selected devices
  onProgress?.({ stage: 'reset', message: `Resetting ENTER/EXIT for ${dateX}…` });
  for (const deviceName of deviceNames) {
    const escapedDevice = escapeSql(deviceName.trim());
    await prisma.$executeRawUnsafe(
      `UPDATE tbl_tracking SET geofence_type = NULL
       WHERE device_name = '${escapedDevice}'
         AND position_time_nz >= '${escapedWriteStart}' AND position_time_nz < '${escapedWriteEnd}'
         AND geofence_type IN ('ENTER','EXIT','CHANGE')`
    );
  }

  for (let i = 0; i < deviceNames.length; i++) {
    const deviceName = deviceNames[i].trim();
    const escapedDevice = escapeSql(deviceName);
    const deviceStartedAt = new Date().toISOString();
    onProgress?.({
      stage: 'device_start',
      deviceName,
      index: i + 1,
      total: deviceNames.length,
    });

    let rowsRead = 0;
    let updatesWritten = 0;
    let status: 'ok' | 'error' = 'ok';
    let errorMessage: string | undefined;
    let logId: number | string | undefined;

    try {
      const rows = await prisma.$queryRawUnsafe<ScanRow[]>(
        `SELECT id, position_time_nz, geofence_id
         FROM tbl_tracking
         WHERE device_name = '${escapedDevice}'
           AND position_time_nz >= '${escapedScanStart}' AND position_time_nz < '${escapedScanEnd}'
         ORDER BY position_time_nz, id`
      );
      rowsRead = rows?.length ?? 0;
      totalRowsRead += rowsRead;
      onProgress?.({ stage: 'device_scan', deviceName, rowsRead });

      const updates: TagUpdate[] = runStateMachine(rows ?? [], writeStart, writeEnd, graceSeconds);
      updatesWritten = updates.length;
      totalUpdatesWritten += updatesWritten;
      onProgress?.({ stage: 'device_update', deviceName, updatesWritten });

      if (updates.length > 0) {
        for (const u of updates) {
          const id = typeof u.id === 'bigint' ? Number(u.id) : u.id;
          const type = u.geofence_type;
          await prisma.$executeRawUnsafe(
            `UPDATE tbl_tracking SET geofence_type = '${type}' WHERE id = ${id}`
          );
        }
      }

      const deviceEndedAt = new Date().toISOString();
      const durationMs = new Date(deviceEndedAt).getTime() - new Date(deviceStartedAt).getTime();
      const logDetails = JSON.stringify({
        window_start: writeStart,
        window_end: writeEnd,
        scan_start: scanStart,
        scan_end: scanEnd,
        rows_read: rowsRead,
        updates_written: updatesWritten,
        grace_seconds: graceSeconds,
        status: 'ok',
        started_at: deviceStartedAt,
        ended_at: deviceEndedAt,
        duration_ms: durationMs,
        algo_version: ALGO_VERSION,
      });
      const log = await prisma.log.create({
        data: {
          logtype: 'entryexit',
          logcat1: deviceName,
          logcat2: dateX,
          logdetails: logDetails,
        },
      });
      logId = typeof log.logid === 'bigint' ? Number(log.logid) : log.logid;
      onProgress?.({ stage: 'device_log', deviceName, logId });

      const result: DeviceResult = {
        deviceName,
        rowsRead,
        updatesWritten,
        status: 'ok',
        startedAt: deviceStartedAt,
        endedAt: deviceEndedAt,
        durationMs,
        logId,
      };
      results.push(result);
      onProgress?.({ stage: 'device_done', deviceName, result });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      status = 'error';
      errorMessage = message;
      const deviceEndedAt = new Date().toISOString();
      const durationMs = new Date(deviceEndedAt).getTime() - new Date(deviceStartedAt).getTime();
      const logDetails = JSON.stringify({
        window_start: writeStart,
        window_end: writeEnd,
        rows_read: rowsRead,
        updates_written: updatesWritten,
        grace_seconds: graceSeconds,
        status: 'error',
        error_message: message,
        started_at: deviceStartedAt,
        ended_at: deviceEndedAt,
        duration_ms: durationMs,
        algo_version: ALGO_VERSION,
      });
      try {
        const log = await prisma.log.create({
          data: {
            logtype: 'entryexit',
            logcat1: deviceName,
            logcat2: dateX,
            logdetails: logDetails,
          },
        });
        logId = typeof log.logid === 'bigint' ? Number(log.logid) : log.logid;
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
    dateX,
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
