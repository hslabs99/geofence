'use client';

import { useCallback, useEffect, useState } from 'react';
import { runFetchStepsForJobs } from '@/lib/fetch-steps';

type StreamEvent =
  | { stage: 'reset'; message: string }
  | { stage: 'device_start'; deviceName: string; index: number; total: number }
  | { stage: 'device_scan'; deviceName: string; rowsRead: number }
  | { stage: 'device_update'; deviceName: string; updatesWritten: number }
  | { stage: 'device_log'; deviceName: string; logId: number | string }
  | { stage: 'device_done'; deviceName: string; result: DeviceResult }
  | { stage: 'done'; summary: RunSummary }
  | { stage: 'error'; message: string };

interface DeviceResult {
  deviceName: string;
  rowsRead: number;
  updatesWritten: number;
  status: string;
  errorMessage?: string;
  durationMs: number;
  logId?: number | string;
}

interface RunSummary {
  dateFrom: string;
  dateTo: string;
  windowStart?: string;
  windowEnd?: string;
  deviceCount: number;
  totalRowsRead: number;
  totalUpdatesWritten: number;
  results: DeviceResult[];
  durationMs: number;
}

interface LogRow {
  logid: number | string;
  logdatetime: string;
  logtype: string;
  logcat1: string | null;
  logcat2: string | null;
  logdetails: string | null;
}

function formatLogLine(ev: StreamEvent): string {
  switch (ev.stage) {
    case 'reset':
      return `[Reset] ${ev.message}`;
    case 'device_start':
      return `[Device ${ev.index}/${ev.total}] Starting ${ev.deviceName}…`;
    case 'device_scan':
      return `[${ev.deviceName}] Scanned ${ev.rowsRead} rows`;
    case 'device_update':
      return `[${ev.deviceName}] Wrote ${ev.updatesWritten} ENTER/EXIT tags`;
    case 'device_log':
      return `[${ev.deviceName}] Logged (id ${ev.logId})`;
    case 'device_done':
      return `[${ev.deviceName}] Done — ${ev.result.rowsRead} rows, ${ev.result.updatesWritten} tags in ${ev.result.durationMs}ms`;
    case 'done':
      return `[Done] ${ev.summary.deviceCount} devices, ${ev.summary.totalRowsRead} rows read, ${ev.summary.totalUpdatesWritten} tags in ${ev.summary.durationMs}ms`;
    case 'error':
      return `[Error] ${ev.message}`;
    default:
      return JSON.stringify(ev);
  }
}

const DEFAULT_DATE = (() => {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
})();

/** List of YYYY-MM-DD from fromDate to toDate inclusive. */
function dateRange(fromDate: string, toDate: string): string[] {
  const from = new Date(fromDate + 'T00:00:00Z');
  const to = new Date(toDate + 'T00:00:00Z');
  if (from.getTime() > to.getTime()) return [];
  const out: string[] = [];
  const cur = new Date(from);
  while (cur.getTime() <= to.getTime()) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

export default function TaggingPage() {
  const [dateFrom, setDateFrom] = useState(DEFAULT_DATE);
  const [dateTo, setDateTo] = useState(DEFAULT_DATE);
  const [stepsForce, setStepsForce] = useState(true);
  const [stepsStartLessMinutes, setStepsStartLessMinutes] = useState(15);
  const [stepsEndPlusMinutes, setStepsEndPlusMinutes] = useState(60);
  const [graceSeconds, setGraceSeconds] = useState(300);
  const [runStatus, setRunStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const [streamLogs, setStreamLogs] = useState<string[]>([]);
  const [currentStage, setCurrentStage] = useState<string>('');
  const [summary, setSummary] = useState<RunSummary | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [entryexitLogs, setEntryexitLogs] = useState<LogRow[]>([]);
  const [entryexitLogsLoading, setEntryexitLogsLoading] = useState(false);
  const [expandedLogId, setExpandedLogId] = useState<string | null>(null);
  const [fenceTaggingStatus, setFenceTaggingStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const [fenceTaggingLogs, setFenceTaggingLogs] = useState<string[]>([]);
  const [fenceTaggingError, setFenceTaggingError] = useState<string | null>(null);
  const [fenceTaggingReprocessAll, setFenceTaggingReprocessAll] = useState(false);

  const loadEntryexitLogs = useCallback(async () => {
    setEntryexitLogsLoading(true);
    try {
      const res = await fetch('/api/logs?type=entryexit&limit=50');
      const data = await res.json();
      setEntryexitLogs(Array.isArray(data.logs) ? data.logs : []);
    } catch {
      setEntryexitLogs([]);
    } finally {
      setEntryexitLogsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (runStatus === 'done' || runStatus === 'idle') loadEntryexitLogs();
  }, [runStatus, loadEntryexitLogs]);

  /** Call shared API: position_time_nz + store_fences (same as api-test between merge and tag). */
  const applyPositionTimeNzAndFences = async (): Promise<{ ok: boolean; error?: string }> => {
    try {
      const res = await fetch('/api/admin/tracking/apply-position-time-nz-and-fences', { method: 'POST' });
      const data = await res.json();
      if (!res.ok || !data?.ok) return { ok: false, error: data?.error ?? `HTTP ${res.status}` };
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  };

  /** Run store_fences_for_date for each day in the current date range (day by day). */
  const runFenceTaggingForRange = async () => {
    const from = dateFrom.trim();
    const to = dateTo.trim();
    const dates = dateRange(from, to);
    if (dates.length === 0) {
      setFenceTaggingError('Date from must be ≤ date to');
      return;
    }
    setFenceTaggingStatus('running');
    setFenceTaggingLogs([
      `Fence tagging for ${from} → ${to} (${dates.length} day(s))${fenceTaggingReprocessAll ? ' [reprocess all]' : ''}…`,
    ]);
    setFenceTaggingError(null);
    try {
      const res = await fetch('/api/admin/tracking/store-fences-for-date-range', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: from,
          dateTo: to,
          forceUpdate: fenceTaggingReprocessAll,
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        let errMsg = text;
        try {
          const j = JSON.parse(text);
          if (j?.error) errMsg = j.error;
        } catch {
          if (text) errMsg = text;
        }
        setFenceTaggingError(errMsg || `HTTP ${res.status}`);
        setFenceTaggingStatus('error');
        setFenceTaggingLogs((prev) => [...prev, `Error: ${errMsg || res.status}`]);
        return;
      }
      if (!res.body) {
        setFenceTaggingError('No response body');
        setFenceTaggingStatus('error');
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const ev = JSON.parse(line) as { type: string; updated?: number; date?: string; durationMs?: number; totalUpdated?: number; message?: string };
            if (ev.type === 'position_time_nz') {
              setFenceTaggingLogs((prev) => [...prev, `Set position_time_nz: ${ev.updated ?? 0} row(s) for range.`]);
            } else if (ev.type === 'day' && ev.date != null) {
              const sec = ev.durationMs != null ? ` (${(ev.durationMs / 1000).toFixed(1)}s)` : '';
              setFenceTaggingLogs((prev) => [...prev, `${ev.date}: ${ev.updated ?? 0} row(s) updated${sec}`]);
            } else if (ev.type === 'done') {
              setFenceTaggingLogs((prev) => [...prev, `Done. Total fences: ${ev.totalUpdated ?? 0} row(s) updated.`]);
              setFenceTaggingStatus('done');
            } else if (ev.type === 'error') {
              setFenceTaggingError(ev.message ?? 'Unknown error');
              setFenceTaggingStatus('error');
              setFenceTaggingLogs((prev) => [...prev, `Error: ${ev.message ?? 'Unknown'}`]);
            }
          } catch {
            setFenceTaggingLogs((prev) => [...prev, `[Raw] ${line}`]);
          }
        }
      }
      if (buffer.trim()) {
        try {
          const ev = JSON.parse(buffer) as { type: string; updated?: number; date?: string; durationMs?: number; totalUpdated?: number; message?: string };
          if (ev.type === 'position_time_nz') {
            setFenceTaggingLogs((prev) => [...prev, `Set position_time_nz: ${ev.updated ?? 0} row(s) for range.`]);
          } else if (ev.type === 'day' && ev.date != null) {
            const sec = ev.durationMs != null ? ` (${(ev.durationMs / 1000).toFixed(1)}s)` : '';
            setFenceTaggingLogs((prev) => [...prev, `${ev.date}: ${ev.updated ?? 0} row(s) updated${sec}`]);
          } else if (ev.type === 'done') {
            setFenceTaggingLogs((prev) => [...prev, `Done. Total fences: ${ev.totalUpdated ?? 0} row(s) updated.`]);
            setFenceTaggingStatus('done');
          } else if (ev.type === 'error') {
            setFenceTaggingError(ev.message ?? 'Unknown error');
            setFenceTaggingStatus('error');
            setFenceTaggingLogs((prev) => [...prev, `Error: ${ev.message ?? 'Unknown'}`]);
          }
        } catch {
          setFenceTaggingLogs((prev) => [...prev, `[Raw] ${buffer}`]);
        }
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setFenceTaggingError(msg);
      setFenceTaggingStatus('error');
      setFenceTaggingLogs((prev) => [...prev, `Error: ${msg}`]);
    }
  };

  const runTagging = async () => {
    const from = dateFrom.trim();
    const to = dateTo.trim();
    if (!from || !to || dateRange(from, to).length === 0) return;
    setRunStatus('running');
    setStreamLogs([]);
    setCurrentStage('Applying position_time_nz and store_fences…');
    setSummary(null);
    setRunError(null);

    try {
      const prep = await applyPositionTimeNzAndFences();
      if (!prep.ok) {
        setRunError(prep.error ?? 'Prep failed');
        setRunStatus('error');
        setCurrentStage('');
        return;
      }
      setCurrentStage('Fetching devices for range…');
      const devRes = await fetch(
        `/api/admin/tracking/devices-for-date-range?dateFrom=${encodeURIComponent(from)}&dateTo=${encodeURIComponent(to)}`
      );
      const devData = await devRes.json();
      const list: string[] = devData?.ok && Array.isArray(devData.devices) ? devData.devices : [];
      if (list.length === 0) {
        setRunError('No devices with tracking data in date range');
        setRunStatus('error');
        setCurrentStage('');
        return;
      }
      setStreamLogs((prev) => [
        ...prev,
        `Devices for ${from} → ${to}: ${list.length} — ${list.join(', ')}`,
      ]);
      setCurrentStage('Starting…');
      const res = await fetch('/api/admin/tagging/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: from,
          dateTo: to,
          deviceNames: list,
          graceSeconds: Math.max(0, Math.min(86400, graceSeconds)),
          bufferHours: 1,
        }),
      });
      if (!res.ok || !res.body) {
        const text = await res.text();
        let errMsg = text;
        try {
          const j = JSON.parse(text);
          if (j?.error) errMsg = j.error;
        } catch {
          if (text) errMsg = text;
        }
        setRunError(errMsg || `HTTP ${res.status}`);
        setRunStatus('error');
        setCurrentStage('');
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const ev = JSON.parse(line) as StreamEvent;
            setStreamLogs((prev) => [...prev, formatLogLine(ev)]);
            if (ev.stage === 'reset') setCurrentStage('Reset');
            if (ev.stage === 'device_start')
              setCurrentStage(`Device ${ev.index}/${ev.total}: ${ev.deviceName}`);
            if (ev.stage === 'device_done') setCurrentStage(`Done: ${ev.deviceName}`);
            if (ev.stage === 'done') {
              setSummary(ev.summary);
              setCurrentStage('Complete');
              setRunStatus('done');
            }
            if (ev.stage === 'error') {
              setRunError(ev.message);
              setRunStatus('error');
              setCurrentStage('');
            }
          } catch {
            setStreamLogs((prev) => [...prev, `[Raw] ${line}`]);
          }
        }
      }
      if (buffer.trim()) {
        try {
          const ev = JSON.parse(buffer) as StreamEvent;
          setStreamLogs((prev) => [...prev, formatLogLine(ev)]);
          if (ev.stage === 'done') {
            setSummary(ev.summary);
            setRunStatus('done');
            setCurrentStage('Complete');
          }
        } catch {
          setStreamLogs((prev) => [...prev, `[Raw] ${buffer}`]);
        }
      }
      setRunStatus((prev) => (prev === 'running' ? 'done' : prev));
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err));
      setRunStatus('error');
      setCurrentStage('');
    }
  };

  const runTagsAndSteps = async () => {
    const from = dateFrom.trim();
    const to = dateTo.trim();
    if (!from || !to) return;
    const dates = dateRange(from, to);
    if (dates.length === 0) {
      setRunError('Date from must be ≤ date to');
      return;
    }
    setRunStatus('running');
    setStreamLogs([]);
    setSummary(null);
    setRunError(null);

    const appendLog = (line: string) => {
      setStreamLogs((prev) => [...prev, line]);
    };

    try {
      for (let d = 0; d < dates.length; d++) {
        const date = dates[d];
        setCurrentStage(`${date}: devices + Tag Enter/Exit (${d + 1}/${dates.length})`);
        appendLog(`=== ${date}: Tag Enter/Exit ===`);

        const devRes = await fetch(`/api/admin/tracking/devices-for-date?date=${encodeURIComponent(date)}`);
        const devData = await devRes.json();
        const dayDevices: string[] = devData?.ok && Array.isArray(devData.devices) ? devData.devices : [];
        appendLog(`  Unique devices for ${date}: ${dayDevices.length > 0 ? dayDevices.join(', ') : 'none'}`);

        if (dayDevices.length > 0) {
          const tagRes = await fetch('/api/admin/tagging/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              dateFrom: date,
              dateTo: date,
              deviceNames: dayDevices,
              graceSeconds: Math.max(0, Math.min(86400, graceSeconds)),
              bufferHours: 1,
            }),
          });
          if (!tagRes.ok || !tagRes.body) {
            const text = await tagRes.text();
            let errMsg = text;
            try {
              const j = JSON.parse(text);
              if (j?.error) errMsg = j.error;
            } catch {
              if (text) errMsg = text;
            }
            setRunError(`Tags ${date}: ${errMsg || `HTTP ${tagRes.status}`}`);
            setRunStatus('error');
            setCurrentStage('');
            return;
          }
          const reader = tagRes.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() ?? '';
            for (const line of lines) {
              if (!line.trim()) continue;
              try {
                const ev = JSON.parse(line) as StreamEvent;
                appendLog(formatLogLine(ev));
                if (ev.stage === 'error') {
                  setRunError(ev.message);
                  setRunStatus('error');
                  setCurrentStage('');
                  return;
                }
              } catch {
                appendLog(`[Raw] ${line}`);
              }
            }
          }
          if (buffer.trim()) {
            try {
              const ev = JSON.parse(buffer) as StreamEvent;
              appendLog(formatLogLine(ev));
              if (ev.stage === 'error') {
                setRunError(ev.message);
                setRunStatus('error');
                setCurrentStage('');
                return;
              }
            } catch {
              appendLog(`[Raw] ${buffer}`);
            }
          }
        }

        setCurrentStage(`${date}: Steps (${d + 1}/${dates.length})`);
        appendLog(`=== ${date}: Steps ===`);
        const stepsUrl = stepsForce
          ? `/api/vworkjobs?date=${encodeURIComponent(date)}`
          : `/api/vworkjobs?date=${encodeURIComponent(date)}&stepsFetched=false`;
        const stepsRes = await fetch(stepsUrl);
        const stepsData = await stepsRes.json();
        if (!stepsRes.ok) {
          appendLog(`[Error] Steps job list: ${stepsData?.error ?? stepsRes.status}`);
          continue;
        }
        const jobs: Record<string, unknown>[] = stepsData.rows ?? [];
        appendLog(`  Jobs for ${date}: ${jobs.length}`);
        const getJob = (j: Record<string, unknown>, ...keys: string[]): string => {
          for (const k of keys) {
            const v = j[k];
            if (v != null && v !== '') return String(v).trim();
          }
          return '';
        };
        jobs.forEach((job) => {
          const jobId = job.job_id != null ? String(job.job_id) : '?';
          const worker = getJob(job, 'worker', 'Worker') || getJob(job, 'truck_id', 'Truck_ID') || '—';
          appendLog(`  Job ${jobId} (${worker})`);
        });
        if (jobs.length > 0) {
          const result = await runFetchStepsForJobs({
            jobs,
            startLessMinutes: stepsStartLessMinutes,
            endPlusMinutes: stepsEndPlusMinutes,
            onProgress: (current, total, log) => {
              const last = log[log.length - 1];
              if (last) appendLog(`    ${current}/${total} ${last.job_id} — ${last.status}${last.message ? `: ${last.message}` : ''}`);
            },
          });
          const okCount = result.log.filter((e) => e.status === 'ok').length;
          const errCount = result.log.filter((e) => e.status === 'error').length;
          appendLog(`  Steps set: ${okCount} ok, ${errCount} error`);
        }
      }
      setCurrentStage('Complete');
      setRunStatus('done');
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err));
      setRunStatus('error');
      setCurrentStage('');
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">
        Entry/Exit Tagging
      </h1>
      <p className="mb-6 text-sm text-zinc-600 dark:text-zinc-400">
        Tag <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking</code> with ENTER/EXIT by fence changes only.
        ENTER when the device goes null→fence or fence→fence and that fence persists for the grace duration; EXIT when it leaves a fence and the next state (null or new fence) persists for the grace duration. No pairs or dependencies.
        Code tags from <strong>00:00 on From date to midnight on To date</strong> (inclusive), plus a fixed 1 h buffer at each end (unlike Steps, no customisable buffer). One run over the full window; re-running the same range is idempotent (same tags). Uses{' '}
        <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> only.
      </p>

      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Fence tagging (date range)
        </h2>
        <p className="mb-3 text-xs text-zinc-600 dark:text-zinc-400">
          Once: set <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> for the range (rows where it is null). Then run{' '}
          <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">store_fences_for_date</code> for each day in the range. Assigns <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">geofence_id</code> from{' '}
          <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_geofences</code>. By default only rows not yet attempted; use the switch to reprocess all rows.
        </p>
        <div className="mb-3 flex flex-wrap items-center gap-4">
          <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            <input
              type="checkbox"
              checked={fenceTaggingReprocessAll}
              onChange={(e) => setFenceTaggingReprocessAll(e.target.checked)}
              disabled={fenceTaggingStatus === 'running'}
              className="rounded border-zinc-300 text-zinc-800 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700"
            />
            Reprocess all (include rows already attempted)
          </label>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={runFenceTaggingForRange}
            disabled={
              fenceTaggingStatus === 'running' ||
              runStatus === 'running' ||
              !dateFrom ||
              !dateTo ||
              dateRange(dateFrom, dateTo).length === 0
            }
            className="rounded bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50 dark:bg-emerald-700 dark:hover:bg-emerald-600"
          >
            {fenceTaggingStatus === 'running' ? 'Running…' : 'Run fence tagging for date range'}
          </button>
          {fenceTaggingStatus === 'done' && (
            <span className="text-sm text-zinc-500 dark:text-zinc-400">Complete</span>
          )}
          {fenceTaggingStatus === 'error' && fenceTaggingError && (
            <span className="text-sm text-red-600 dark:text-red-400">{fenceTaggingError}</span>
          )}
        </div>
        {fenceTaggingLogs.length > 0 && (
          <div className="mt-3 max-h-40 overflow-y-auto rounded border border-zinc-200 bg-zinc-50 p-2 font-mono text-xs text-zinc-700 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-300">
            {fenceTaggingLogs.map((line, i) => (
              <div key={i} className="border-b border-zinc-100 py-0.5 last:border-0 dark:border-zinc-700">
                {line}
              </div>
            ))}
          </div>
        )}
      </section>

      <div className="mb-8 grid gap-6 md:grid-cols-1 lg:grid-cols-2">
        <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="mb-4 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
            Parameters
          </h2>
          <div className="space-y-4">
            <div className="rounded border border-zinc-200 bg-zinc-50/50 p-3 dark:border-zinc-600 dark:bg-zinc-800/30">
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                Date range
              </h3>
              <div className="flex flex-wrap items-end gap-4">
                <div>
                  <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">From</label>
                  <input
                    type="date"
                    value={dateFrom}
                    onChange={(e) => setDateFrom(e.target.value)}
                    disabled={runStatus === 'running'}
                    className="mt-0.5 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">To</label>
                  <input
                    type="date"
                    value={dateTo}
                    onChange={(e) => setDateTo(e.target.value)}
                    disabled={runStatus === 'running'}
                    className="mt-0.5 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  />
                </div>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
                  <input
                    type="checkbox"
                    checked={stepsForce}
                    onChange={(e) => setStepsForce(e.target.checked)}
                    disabled={runStatus === 'running'}
                    className="rounded"
                  />
                  Steps: force (all jobs for date; default on)
                </label>
                <div className="flex items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
                  <span>Window:</span>
                  <input
                    type="number"
                    min={0}
                    max={120}
                    value={stepsStartLessMinutes}
                    onChange={(e) => setStepsStartLessMinutes(Math.max(0, Math.min(120, parseInt(e.target.value, 10) || 0)))}
                    disabled={runStatus === 'running'}
                    className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 dark:border-zinc-600 dark:bg-zinc-800"
                  />
                  <span>min before,</span>
                  <input
                    type="number"
                    min={0}
                    max={120}
                    value={stepsEndPlusMinutes}
                    onChange={(e) => setStepsEndPlusMinutes(Math.max(0, Math.min(120, parseInt(e.target.value, 10) || 0)))}
                    disabled={runStatus === 'running'}
                    className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 dark:border-zinc-600 dark:bg-zinc-800"
                  />
                  <span>min after</span>
                </div>
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300">
                Grace seconds
              </label>
              <input
                type="number"
                min={0}
                max={86400}
                value={graceSeconds}
                onChange={(e) => setGraceSeconds(parseInt(e.target.value, 10) || 0)}
                className="mt-1 block w-full rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
                ENTER/EXIT only if the new state persists at least this long (default 300 s).
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={runTagging}
                disabled={
                  runStatus === 'running' ||
                  !dateFrom ||
                  !dateTo ||
                  dateRange(dateFrom, dateTo).length === 0
                }
                className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
              >
                {runStatus === 'running' ? 'Running…' : 'Run tagging'}
              </button>
              <button
                type="button"
                onClick={runTagsAndSteps}
                disabled={
                  runStatus === 'running' ||
                  !dateFrom ||
                  !dateTo ||
                  dateRange(dateFrom, dateTo).length === 0
                }
                className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                title="For each date: get distinct device_name from tbl_tracking for that day, run Tag Enter/Exit for those devices, then run Steps (vworkjobs for that date). No device selection needed."
              >
                {runStatus === 'running' ? 'Running…' : 'Tag Enter/Exit and Steps'}
              </button>
            </div>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              Run tagging: fetches devices from tbl_tracking for the range, then tags ENTER/EXIT. Tag Enter/Exit and Steps: for each day, gets devices for that day from tbl_tracking and runs Enter/Exit then Steps (vworkjobs). Device lists appear in the log. Run Fence tagging above first if needed.
            </p>
          </div>
        </section>

        <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="mb-4 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
            Progress
          </h2>
          {runStatus === 'running' && (
            <div className="mb-3 flex items-center gap-2 rounded bg-amber-50 py-2 px-3 dark:bg-amber-950/30">
              <span
                className="inline-block h-2 w-2 animate-pulse rounded-full bg-amber-500"
                aria-hidden
              />
              <span className="text-sm font-medium text-amber-800 dark:text-amber-200">
                {currentStage}
              </span>
            </div>
          )}
          {runStatus === 'done' && summary && (
            <div className="mb-3 rounded bg-green-50 py-3 px-3 dark:bg-green-950/30">
              <p className="text-sm font-medium text-green-800 dark:text-green-200">
                Complete: {summary.dateFrom} → {summary.dateTo}, {summary.totalRowsRead} rows read, {summary.totalUpdatesWritten} tags
                written in {summary.durationMs}ms
              </p>
              {summary.windowStart != null && summary.windowEnd != null && (
                <p className="mt-1 text-xs text-green-700 dark:text-green-300">
                  Window: {summary.windowStart} to {summary.windowEnd}
                </p>
              )}
              <ul className="mt-2 list-inside list-disc text-xs text-green-700 dark:text-green-300">
                {summary.results.map((r) => (
                  <li key={r.deviceName}>
                    {r.deviceName}: {r.rowsRead} rows → {r.updatesWritten} tags
                    {r.status === 'error' && r.errorMessage ? ` (${r.errorMessage})` : ''}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {runStatus === 'error' && runError && (
            <div className="mb-3 rounded bg-red-50 py-2 px-3 dark:bg-red-950/30">
              <p className="text-sm font-medium text-red-800 dark:text-red-200">{runError}</p>
            </div>
          )}
          <div className="rounded border border-zinc-200 bg-zinc-50 p-2 dark:border-zinc-600 dark:bg-zinc-800/50">
            <p className="mb-2 text-xs font-medium text-zinc-500 dark:text-zinc-400">
              Live log
            </p>
            <div
              className="max-h-64 overflow-y-auto font-mono text-xs text-zinc-700 dark:text-zinc-300"
              role="log"
              aria-live="polite"
            >
              {streamLogs.length === 0 ? (
                <p className="text-zinc-400 dark:text-zinc-500">No output yet. Run tagging to see live progress.</p>
              ) : (
                streamLogs.map((line, i) => (
                  <div key={i} className="border-b border-zinc-100 py-0.5 dark:border-zinc-700">
                    {line}
                  </div>
                ))
              )}
            </div>
          </div>
        </section>
      </div>

      <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
            Entry/Exit logs (tbl_logs)
          </h2>
          <button
            type="button"
            onClick={loadEntryexitLogs}
            disabled={entryexitLogsLoading}
            className="rounded border border-zinc-300 px-2 py-1 text-xs font-medium text-zinc-600 hover:bg-zinc-100 dark:border-zinc-600 dark:text-zinc-400 dark:hover:bg-zinc-800"
          >
            {entryexitLogsLoading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
        <div className="overflow-x-auto">
          {entryexitLogs.length === 0 && !entryexitLogsLoading ? (
            <p className="text-sm text-zinc-500 dark:text-zinc-400">
              No entryexit logs yet. Run tagging to create logs.
            </p>
          ) : (
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th className="pb-2 pr-4 font-medium text-zinc-600 dark:text-zinc-400">Time</th>
                  <th className="pb-2 pr-4 font-medium text-zinc-600 dark:text-zinc-400">Device</th>
                  <th className="pb-2 pr-4 font-medium text-zinc-600 dark:text-zinc-400">Date</th>
                  <th className="pb-2 font-medium text-zinc-600 dark:text-zinc-400">Details</th>
                </tr>
              </thead>
              <tbody>
                {entryexitLogs.map((log) => {
                  const id = String(log.logid);
                  const expanded = expandedLogId === id;
                  let details: Record<string, unknown> = {};
                  try {
                    if (log.logdetails) details = JSON.parse(log.logdetails);
                  } catch {
                    details = { raw: log.logdetails };
                  }
                  return (
                    <tr
                      key={id}
                      className="border-b border-zinc-100 dark:border-zinc-800"
                    >
                      <td className="py-2 pr-4 text-zinc-600 dark:text-zinc-400">
                        {log.logdatetime}
                      </td>
                      <td className="py-2 pr-4 font-medium">{log.logcat1 ?? '—'}</td>
                      <td className="py-2 pr-4">{log.logcat2 ?? '—'}</td>
                      <td className="py-2">
                        <button
                          type="button"
                          onClick={() => setExpandedLogId(expanded ? null : id)}
                          className="text-xs text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-400"
                        >
                          {expanded ? 'Collapse' : 'Expand'}
                        </button>
                        {expanded && (
                          <pre className="mt-2 max-h-48 overflow-auto rounded bg-zinc-100 p-2 text-xs dark:bg-zinc-800">
                            {JSON.stringify(details, null, 2)}
                          </pre>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </div>
  );
}
