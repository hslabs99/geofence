'use client';

import { useCallback, useEffect, useState } from 'react';
import { runFetchStepsForJobs } from '@/lib/fetch-steps';
import { GPS_FENCE_SETTINGS_TYPE, GPS_STD_TIME_NAME } from '@/lib/gps-fence-settings-names';

type VworkFilterOptions = {
  vineyardNames: string[];
  deliveryWineries: string[];
  workers: string[];
};

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

/** Low / Med / High — see Fence tagging scope UI. */
type FenceTaggingScope = 'unattempted' | 'unattempted_and_missed' | 'all';

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

function getJobField(j: Record<string, unknown>, ...keys: string[]): string {
  for (const k of keys) {
    const v = j[k];
    if (v != null && v !== '') return String(v).trim();
  }
  return '';
}

/** Amber progress line: explicit Day / Job counts for Steps. */
function stepsProgressStage(
  date: string,
  dayIndex0: number,
  totalDays: number,
  jobCurrent: number,
  jobTotal: number
): string {
  const d = dayIndex0 + 1;
  if (jobTotal <= 0) {
    return `Progress: ${date}: Steps (Day ${d}/${totalDays})`;
  }
  return `Progress: ${date}: Steps (Day ${d}/${totalDays}) (Job ${jobCurrent}/${jobTotal})`;
}

export default function TaggingPage() {
  const [dateFrom, setDateFrom] = useState(DEFAULT_DATE);
  const [dateTo, setDateTo] = useState(DEFAULT_DATE);
  const [stepsForce, setStepsForce] = useState(true);
  const [stepsStartLessMinutes, setStepsStartLessMinutes] = useState(15);
  const [stepsEndPlusMinutes, setStepsEndPlusMinutes] = useState(60);
  /** Loaded from Settings (GPS Std time); null until loaded or if unset/invalid. */
  const [graceSeconds, setGraceSeconds] = useState<number | null>(null);
  const [graceSettingsLoading, setGraceSettingsLoading] = useState(true);
  const [graceSettingsError, setGraceSettingsError] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  /** When true, the in-progress run is Steps-only (not full tagging). */
  const [stepsOnlyRunActive, setStepsOnlyRunActive] = useState(false);
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
  const [fenceTaggingScope, setFenceTaggingScope] = useState<FenceTaggingScope>('unattempted');
  const [maxPositionTime, setMaxPositionTime] = useState<string | null>(null);
  const [maxPositionTimeFenceAttempted, setMaxPositionTimeFenceAttempted] = useState<string | null>(null);
  const [maxPositionTimeNz, setMaxPositionTimeNz] = useState<string | null>(null);
  const [maxPositionTimeNzFenceAttempted, setMaxPositionTimeNzFenceAttempted] = useState<string | null>(null);
  const [maxTimesLoading, setMaxTimesLoading] = useState(false);
  const [maxTimesError, setMaxTimesError] = useState<string | null>(null);
  const [maxActualStartAll, setMaxActualStartAll] = useState<string | null>(null);
  const [maxActualStartWithStepData, setMaxActualStartWithStepData] = useState<string | null>(null);
  const [vworkStartSummaryLoading, setVworkStartSummaryLoading] = useState(false);
  const [vworkStartSummaryError, setVworkStartSummaryError] = useState<string | null>(null);

  /** Optional Steps filters (tbl_vworkjobs): exact vineyard / winery / device (worker). */
  const [filterVineyard, setFilterVineyard] = useState('');
  const [filterWinery, setFilterWinery] = useState('');
  const [filterDevice, setFilterDevice] = useState('');
  const [vworkFilterOptions, setVworkFilterOptions] = useState<VworkFilterOptions>({
    vineyardNames: [],
    deliveryWineries: [],
    workers: [],
  });
  const [vworkjobsMatchCount, setVworkjobsMatchCount] = useState<number | null>(null);
  const [vworkjobsCountLoading, setVworkjobsCountLoading] = useState(false);
  const [vworkjobsCountError, setVworkjobsCountError] = useState<string | null>(null);
  const [filterOptionsLoadError, setFilterOptionsLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setFilterOptionsLoadError(null);
    fetch('/api/vworkjobs/filter-options', { cache: 'no-store' })
      .then(async (r) => {
        const d = (await r.json()) as Record<string, unknown>;
        if (!r.ok) {
          throw new Error(typeof d.error === 'string' ? d.error : `HTTP ${r.status}`);
        }
        return d;
      })
      .then((d) => {
        if (cancelled) return;
        setVworkFilterOptions({
          vineyardNames: Array.isArray(d.vineyardNames) ? (d.vineyardNames as string[]) : [],
          deliveryWineries: Array.isArray(d.deliveryWineries) ? (d.deliveryWineries as string[]) : [],
          workers: Array.isArray(d.workers) ? (d.workers as string[]) : [],
        });
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setVworkFilterOptions({ vineyardNames: [], deliveryWineries: [], workers: [] });
          setFilterOptionsLoadError(e instanceof Error ? e.message : String(e));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setGraceSettingsLoading(true);
    setGraceSettingsError(null);
    fetch(
      `/api/settings?${new URLSearchParams({ type: GPS_FENCE_SETTINGS_TYPE, name: GPS_STD_TIME_NAME })}`,
      { cache: 'no-store' }
    )
      .then((r) => r.json())
      .then((d: { settingvalue?: string | null }) => {
        if (cancelled) return;
        const v = d?.settingvalue;
        if (v != null && String(v).trim() !== '') {
          const n = parseInt(String(v).trim(), 10);
          if (!Number.isNaN(n) && n >= 0) {
            setGraceSeconds(Math.min(86400, n));
            setGraceSettingsError(null);
          } else {
            setGraceSeconds(null);
            setGraceSettingsError('GPS Std time in Settings is not a valid number.');
          }
        } else {
          setGraceSeconds(null);
          setGraceSettingsError('Set GPS Std time (seconds) in Admin → Settings.');
        }
      })
      .catch(() => {
        if (!cancelled) {
          setGraceSeconds(null);
          setGraceSettingsError('Could not load GPS Std time from Settings.');
        }
      })
      .finally(() => {
        if (!cancelled) setGraceSettingsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const from = dateFrom.trim();
    const to = dateTo.trim();
    if (!from || !to || dateRange(from, to).length === 0) {
      setVworkjobsMatchCount(null);
      setVworkjobsCountError(null);
      setVworkjobsCountLoading(false);
      return;
    }
    setVworkjobsCountLoading(true);
    setVworkjobsCountError(null);
    const ac = new AbortController();
    const t = setTimeout(() => {
      const u = new URL('/api/vworkjobs', window.location.origin);
      u.searchParams.set('dateFrom', from);
      u.searchParams.set('dateTo', to);
      u.searchParams.set('countOnly', 'true');
      if (!stepsForce) u.searchParams.set('stepsFetched', 'false');
      if (filterVineyard) u.searchParams.set('vineyard', filterVineyard);
      if (filterWinery) u.searchParams.set('winery', filterWinery);
      if (filterDevice) u.searchParams.set('device', filterDevice);
      fetch(u.toString(), { signal: ac.signal })
        .then(async (res) => {
          const j = (await res.json()) as { count?: number; error?: string };
          if (!res.ok) throw new Error(j?.error ?? `HTTP ${res.status}`);
          setVworkjobsMatchCount(typeof j.count === 'number' ? j.count : null);
        })
        .catch((e: unknown) => {
          if (e instanceof Error && e.name === 'AbortError') return;
          setVworkjobsCountError(e instanceof Error ? e.message : String(e));
          setVworkjobsMatchCount(null);
        })
        .finally(() => {
          if (!ac.signal.aborted) setVworkjobsCountLoading(false);
        });
    }, 280);
    return () => {
      clearTimeout(t);
      ac.abort();
    };
  }, [dateFrom, dateTo, stepsForce, filterVineyard, filterWinery, filterDevice]);

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

  const loadMaxPositionTimes = useCallback(async () => {
    setMaxTimesLoading(true);
    setMaxTimesError(null);
    try {
      const res = await fetch('/api/admin/tracking/max-position-times', { cache: 'no-store' });
      const data = (await res.json()) as {
        ok?: boolean;
        maxPositionTime?: string | null;
        maxPositionTimeFenceAttempted?: string | null;
        maxPositionTimeNz?: string | null;
        maxPositionTimeNzFenceAttempted?: string | null;
        error?: string;
      };
      if (!res.ok || !data.ok) {
        throw new Error(data.error ?? `HTTP ${res.status}`);
      }
      setMaxPositionTime(data.maxPositionTime ?? null);
      setMaxPositionTimeFenceAttempted(data.maxPositionTimeFenceAttempted ?? null);
      setMaxPositionTimeNz(data.maxPositionTimeNz ?? null);
      setMaxPositionTimeNzFenceAttempted(data.maxPositionTimeNzFenceAttempted ?? null);
    } catch (e) {
      setMaxTimesError(e instanceof Error ? e.message : String(e));
      setMaxPositionTime(null);
      setMaxPositionTimeFenceAttempted(null);
      setMaxPositionTimeNz(null);
      setMaxPositionTimeNzFenceAttempted(null);
    } finally {
      setMaxTimesLoading(false);
    }
  }, []);

  useEffect(() => {
    loadMaxPositionTimes();
  }, [loadMaxPositionTimes]);

  const loadVworkActualStartSummary = useCallback(async () => {
    setVworkStartSummaryLoading(true);
    setVworkStartSummaryError(null);
    try {
      const res = await fetch('/api/admin/vworkjobs/max-actual-start-summary', { cache: 'no-store' });
      const data = (await res.json()) as {
        ok?: boolean;
        maxActualStartTime?: string | null;
        maxActualStartWithStepData?: string | null;
        error?: string;
      };
      if (!res.ok || !data.ok) {
        throw new Error(data.error ?? `HTTP ${res.status}`);
      }
      setMaxActualStartAll(data.maxActualStartTime ?? null);
      setMaxActualStartWithStepData(data.maxActualStartWithStepData ?? null);
    } catch (e) {
      setVworkStartSummaryError(e instanceof Error ? e.message : String(e));
      setMaxActualStartAll(null);
      setMaxActualStartWithStepData(null);
    } finally {
      setVworkStartSummaryLoading(false);
    }
  }, []);

  useEffect(() => {
    loadVworkActualStartSummary();
  }, [loadVworkActualStartSummary]);

  useEffect(() => {
    if (runStatus === 'done') loadVworkActualStartSummary();
  }, [runStatus, loadVworkActualStartSummary]);

  /** NZ backfill only (no store_fences — fence prep runs in the SQL pipeline before this page). */
  const applyPositionTimeNzOnly = async (): Promise<{ ok: boolean; error?: string }> => {
    try {
      const res = await fetch('/api/admin/tracking/apply-position-time-nz', { method: 'POST' });
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
    const scopeLabel =
      fenceTaggingScope === 'all'
        ? ' [High: reprocess all]'
        : fenceTaggingScope === 'unattempted_and_missed'
          ? ' [Med: unattempted + missed]'
          : ' [Low: unattempted only]';
    setFenceTaggingLogs([
      `Fence tagging for ${from} → ${to} (${dates.length} day(s))${scopeLabel}…`,
    ]);
    setFenceTaggingError(null);
    try {
      const res = await fetch('/api/admin/tracking/store-fences-for-date-range', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: from,
          dateTo: to,
          fenceScope: fenceTaggingScope,
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
              loadMaxPositionTimes();
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
            loadMaxPositionTimes();
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
    if (graceSeconds === null) {
      setRunError(graceSettingsError ?? 'Configure GPS Std time in Settings.');
      setRunStatus('error');
      return;
    }
    setRunStatus('running');
    setStepsOnlyRunActive(false);
    setStreamLogs([]);
    setCurrentStage('Progress: Applying position_time_nz…');
    setSummary(null);
    setRunError(null);

    try {
      const prep = await applyPositionTimeNzOnly();
      if (!prep.ok) {
        setRunError(prep.error ?? 'position_time_nz failed');
        setRunStatus('error');
        setCurrentStage('');
        return;
      }
      setCurrentStage('Progress: Fetching devices for range…');
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
      setCurrentStage('Progress: Starting ENTER/EXIT tagging…');
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
            if (ev.stage === 'reset') setCurrentStage('Progress: Reset');
            if (ev.stage === 'device_start')
              setCurrentStage(
                `Progress: ENTER/EXIT (Device ${ev.index}/${ev.total}) — ${ev.deviceName}`
              );
            if (ev.stage === 'device_done')
              setCurrentStage(`Progress: Done device — ${ev.deviceName}`);
            if (ev.stage === 'done') {
              setSummary(ev.summary);
              setCurrentStage('Progress: Complete');
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
            setCurrentStage('Progress: Complete');
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

  /** Derived-steps for one calendar day (vworkjobs + runFetchStepsForJobs). */
  const runStepsForDate = async (
    date: string,
    dayIndex: number,
    totalDays: number,
    appendLog: (line: string) => void
  ): Promise<void> => {
    setCurrentStage(stepsProgressStage(date, dayIndex, totalDays, 0, 0));
    appendLog(`=== ${date}: Steps ===`);
    const stepsParams = new URLSearchParams();
    stepsParams.set('date', date);
    if (!stepsForce) stepsParams.set('stepsFetched', 'false');
    if (filterVineyard) stepsParams.set('vineyard', filterVineyard);
    if (filterWinery) stepsParams.set('winery', filterWinery);
    if (filterDevice) stepsParams.set('device', filterDevice);
    const stepsRes = await fetch(`/api/vworkjobs?${stepsParams.toString()}`);
    const stepsData = await stepsRes.json();
    if (!stepsRes.ok) {
      appendLog(`[Error] Steps job list: ${stepsData?.error ?? stepsRes.status}`);
      return;
    }
    const jobs: Record<string, unknown>[] = stepsData.rows ?? [];
    if (jobs.length > 0) {
      setCurrentStage(stepsProgressStage(date, dayIndex, totalDays, 0, jobs.length));
    }
    appendLog(`  Jobs for ${date}: ${jobs.length}`);
    jobs.forEach((job) => {
      const jobId = job.job_id != null ? String(job.job_id) : '?';
      const worker =
        getJobField(job, 'worker', 'Worker') || getJobField(job, 'truck_id', 'Truck_ID') || '—';
      appendLog(`  Job ${jobId} (${worker})`);
    });
    if (jobs.length > 0) {
      const result = await runFetchStepsForJobs({
        jobs,
        startLessMinutes: stepsStartLessMinutes,
        endPlusMinutes: stepsEndPlusMinutes,
        jobDateForLog: date,
        onProgress: (current, total, log) => {
          setCurrentStage(stepsProgressStage(date, dayIndex, totalDays, current, total));
          const last = log[log.length - 1];
          if (!last) return;
          appendLog(
            `    ${current}/${total} ${last.job_id} — ${last.status}${last.message ? `: ${last.message}` : ''}`
          );
          if (last.status === 'ok' && last.stepVias?.length === 5) {
            const day = last.jobDate ?? date;
            last.stepVias.forEach((via, si) => {
              appendLog(`      ${day} job ${last.job_id} step ${si + 1}: ${via}`);
            });
          }
        },
      });
      const okCount = result.log.filter((e) => e.status === 'ok').length;
      const errCount = result.log.filter((e) => e.status === 'error').length;
      appendLog(`  Steps set: ${okCount} ok, ${errCount} error`);
    }
  };

  /** After new GPS mappings only Steps need refreshing; skips ENTER/EXIT tagging. */
  const runStepsOnlyForRange = async () => {
    const from = dateFrom.trim();
    const to = dateTo.trim();
    if (!from || !to) return;
    const dates = dateRange(from, to);
    if (dates.length === 0) {
      setRunError('Date from must be ≤ date to');
      return;
    }
    setRunStatus('running');
    setStepsOnlyRunActive(true);
    setStreamLogs([]);
    setSummary(null);
    setRunError(null);

    const appendLog = (line: string) => {
      setStreamLogs((prev) => [...prev, line]);
    };

    try {
      appendLog(`Steps only (no ENTER/EXIT) for ${from} → ${to} (${dates.length} day(s))…`);
      for (let d = 0; d < dates.length; d++) {
        await runStepsForDate(dates[d]!, d, dates.length, appendLog);
      }
      appendLog('Steps-only run complete.');
      setCurrentStage('Progress: Complete');
      setRunStatus('done');
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err));
      setRunStatus('error');
      setCurrentStage('');
    } finally {
      setStepsOnlyRunActive(false);
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
    if (graceSeconds === null) {
      setRunError(graceSettingsError ?? 'Configure GPS Std time in Settings.');
      setRunStatus('error');
      return;
    }
    setRunStatus('running');
    setStepsOnlyRunActive(false);
    setStreamLogs([]);
    setSummary(null);
    setRunError(null);

    const appendLog = (line: string) => {
      setStreamLogs((prev) => [...prev, line]);
    };

    try {
      for (let d = 0; d < dates.length; d++) {
        const date = dates[d]!;
        const dayNum = d + 1;
        const dayTotal = dates.length;
        setCurrentStage(
          `Progress: ${date}: Tag Enter/Exit (Day ${dayNum}/${dayTotal})`
        );
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
                if (ev.stage === 'device_start') {
                  setCurrentStage(
                    `Progress: ${date}: Tag Enter/Exit (Day ${dayNum}/${dayTotal}) (Device ${ev.index}/${ev.total}) — ${ev.deviceName}`
                  );
                }
                if (ev.stage === 'device_done') {
                  setCurrentStage(
                    `Progress: ${date}: Tag Enter/Exit (Day ${dayNum}/${dayTotal}) — done ${ev.deviceName}`
                  );
                }
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
              if (ev.stage === 'device_start') {
                setCurrentStage(
                  `Progress: ${date}: Tag Enter/Exit (Day ${dayNum}/${dayTotal}) (Device ${ev.index}/${ev.total}) — ${ev.deviceName}`
                );
              }
              if (ev.stage === 'device_done') {
                setCurrentStage(
                  `Progress: ${date}: Tag Enter/Exit (Day ${dayNum}/${dayTotal}) — done ${ev.deviceName}`
                );
              }
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

        await runStepsForDate(date, d, dates.length, appendLog);
      }
      setCurrentStage('Progress: Complete');
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
          <strong>Once:</strong> set <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> for the range (rows where it is null).{' '}
          <strong>Then</strong> run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">store_fences_for_date</code> for each day in the range. Assigns{' '}
          <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">geofence_id</code> from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_geofences</code>.{' '}
          By default only rows not yet attempted; use scope to reprocess all or unattempted + missed (e.g. after adding a new geofence).
        </p>
        <div className="mb-3 rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-xs dark:border-zinc-600 dark:bg-zinc-800/50">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <span className="font-medium text-zinc-700 dark:text-zinc-300">Latest in tbl_tracking</span>
            <button
              type="button"
              onClick={() => loadMaxPositionTimes()}
              disabled={maxTimesLoading || fenceTaggingStatus === 'running'}
              className="rounded border border-zinc-300 bg-white px-2 py-0.5 text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
            >
              {maxTimesLoading ? 'Loading…' : 'Refresh'}
            </button>
          </div>
          {maxTimesError && <p className="mt-1 text-red-600 dark:text-red-400">{maxTimesError}</p>}
          {!maxTimesError && (
            <div className="mt-2 overflow-x-auto">
              <table className="w-full min-w-[20rem] border-collapse text-left text-xs">
                <thead>
                  <tr className="border-b border-zinc-200 dark:border-zinc-600">
                    <th className="py-1.5 pr-3 font-medium text-zinc-600 dark:text-zinc-400" />
                    <th className="py-1.5 pr-3 font-medium text-zinc-700 dark:text-zinc-300">All rows</th>
                    <th className="py-1.5 font-medium text-zinc-700 dark:text-zinc-300">
                      <code className="rounded bg-zinc-200/80 px-1 font-mono text-[0.7rem] dark:bg-zinc-700">geofence_attempted</code>
                    </th>
                  </tr>
                </thead>
                <tbody className="font-mono text-zinc-900 dark:text-zinc-100">
                  <tr className="border-b border-zinc-100 dark:border-zinc-700/80">
                    <th className="py-1.5 pr-3 text-left font-sans font-normal text-zinc-500 dark:text-zinc-400">
                      max(position_time)
                    </th>
                    <td className="py-1.5 pr-3">{maxTimesLoading ? '…' : maxPositionTime ?? '—'}</td>
                    <td className="py-1.5">{maxTimesLoading ? '…' : maxPositionTimeFenceAttempted ?? '—'}</td>
                  </tr>
                  <tr>
                    <th className="py-1.5 pr-3 text-left font-sans font-normal text-zinc-500 dark:text-zinc-400">
                      max(position_time_nz)
                    </th>
                    <td className="py-1.5 pr-3">{maxTimesLoading ? '…' : maxPositionTimeNz ?? '—'}</td>
                    <td className="py-1.5">{maxTimesLoading ? '…' : maxPositionTimeNzFenceAttempted ?? '—'}</td>
                  </tr>
                </tbody>
              </table>
              <p className="mt-1.5 text-[0.65rem] text-zinc-500 dark:text-zinc-400">
                Second column is the latest row fence tagging has processed (<code className="rounded bg-zinc-200/80 px-0.5 dark:bg-zinc-700">store_fences</code> set{' '}
                <code className="rounded bg-zinc-200/80 px-0.5 dark:bg-zinc-700">geofence_attempted</code>).
              </p>
            </div>
          )}
          <p className="mt-2 text-zinc-500 dark:text-zinc-400">
            Use these to see how far tracking data runs; pick a date range that covers the days you care about.
          </p>
        </div>
        <div className="mb-3 flex flex-wrap items-center gap-4">
          <fieldset className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:gap-4" disabled={fenceTaggingStatus === 'running'}>
            <legend className="mb-1 text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:mb-0 sm:w-full">
              Fence tagging scope
            </legend>
            <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
              <input
                type="radio"
                name="fenceTaggingScope"
                checked={fenceTaggingScope === 'unattempted'}
                onChange={() => setFenceTaggingScope('unattempted')}
                className="border-zinc-300 text-zinc-800 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700"
              />
              <span>
                <span className="font-medium text-zinc-800 dark:text-zinc-200">1 — Low:</span> Only unattempted
              </span>
            </label>
            <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
              <input
                type="radio"
                name="fenceTaggingScope"
                checked={fenceTaggingScope === 'unattempted_and_missed'}
                onChange={() => setFenceTaggingScope('unattempted_and_missed')}
                className="border-zinc-300 text-zinc-800 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700"
              />
              <span>
                <span className="font-medium text-zinc-800 dark:text-zinc-200">3 — Med:</span> Reprocess unattempted and missed only (1 + missed hits)
              </span>
            </label>
            <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
              <input
                type="radio"
                name="fenceTaggingScope"
                checked={fenceTaggingScope === 'all'}
                onChange={() => setFenceTaggingScope('all')}
                className="border-zinc-300 text-zinc-800 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700"
              />
              <span>
                <span className="font-medium text-zinc-800 dark:text-zinc-200">2 — High:</span> Reprocess all (include rows already attempted)
              </span>
            </label>
          </fieldset>
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
              <div className="mt-3 rounded-md border border-zinc-200 bg-white px-3 py-2 text-xs dark:border-zinc-600 dark:bg-zinc-900/40">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="font-medium text-zinc-700 dark:text-zinc-300">
                    Steps coverage (<code className="rounded bg-zinc-100 px-0.5 font-mono dark:bg-zinc-800">tbl_vworkjobs</code>)
                  </span>
                  <button
                    type="button"
                    onClick={() => loadVworkActualStartSummary()}
                    disabled={vworkStartSummaryLoading || runStatus === 'running'}
                    className="rounded border border-zinc-300 bg-zinc-50 px-2 py-0.5 text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-200 dark:hover:bg-zinc-700"
                  >
                    {vworkStartSummaryLoading ? 'Loading…' : 'Refresh'}
                  </button>
                </div>
                {vworkStartSummaryError && (
                  <p className="mt-1 text-red-600 dark:text-red-400">{vworkStartSummaryError}</p>
                )}
                {!vworkStartSummaryError && (
                  <dl className="mt-2 space-y-2">
                    <div>
                      <dt className="text-zinc-500 dark:text-zinc-400">max(actual_start_time) — any job</dt>
                      <dd className="font-mono text-zinc-900 dark:text-zinc-100">
                        {vworkStartSummaryLoading ? '…' : maxActualStartAll ?? '—'}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-zinc-500 dark:text-zinc-400">
                        max(actual_start_time) — has any{' '}
                        <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">step_*_actual_time</code>
                      </dt>
                      <dd className="font-mono text-zinc-900 dark:text-zinc-100">
                        {vworkStartSummaryLoading ? '…' : maxActualStartWithStepData ?? '—'}
                      </dd>
                    </div>
                  </dl>
                )}
                <p className="mt-2 text-[0.65rem] text-zinc-500 dark:text-zinc-400">
                  Second line is how far out you have at least one derived step time stored (logical “last” steps data by job start). No worker filter.
                </p>
              </div>
              <div className="mt-3 flex flex-wrap items-end gap-3 border-t border-zinc-200 pt-3 dark:border-zinc-600">
                <p className="w-full text-xs text-zinc-500 dark:text-zinc-400">
                  Optional: limit <strong>Steps</strong> jobs (same filters as <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs</code>). ENTER/EXIT tagging is unchanged.
                </p>
                {filterOptionsLoadError && (
                  <p className="w-full text-xs text-red-600 dark:text-red-400">
                    Could not load dropdown values: {filterOptionsLoadError}
                  </p>
                )}
                <label className="flex flex-col gap-1">
                  <span className="text-xs font-medium text-zinc-600 dark:text-zinc-400">Vineyard name</span>
                  <select
                    value={filterVineyard}
                    onChange={(e) => setFilterVineyard(e.target.value)}
                    disabled={runStatus === 'running'}
                    className="min-w-[10rem] max-w-[14rem] rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">Any</option>
                    {vworkFilterOptions.vineyardNames.map((v) => (
                      <option key={v} value={v}>
                        {v}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="flex flex-col gap-1">
                  <span className="text-xs font-medium text-zinc-600 dark:text-zinc-400">Delivery winery</span>
                  <select
                    value={filterWinery}
                    onChange={(e) => setFilterWinery(e.target.value)}
                    disabled={runStatus === 'running'}
                    className="min-w-[10rem] max-w-[14rem] rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">Any</option>
                    {vworkFilterOptions.deliveryWineries.map((w) => (
                      <option key={w} value={w}>
                        {w}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="flex flex-col gap-1">
                  <span className="text-xs font-medium text-zinc-600 dark:text-zinc-400">Device (worker)</span>
                  <select
                    value={filterDevice}
                    onChange={(e) => setFilterDevice(e.target.value)}
                    disabled={runStatus === 'running'}
                    className="min-w-[10rem] max-w-[14rem] rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">Any</option>
                    {vworkFilterOptions.workers.map((w) => (
                      <option key={w} value={w}>
                        {w}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="flex flex-col gap-0.5 text-sm">
                  <span className="text-xs font-medium text-zinc-600 dark:text-zinc-400">Matching jobs (Steps)</span>
                  <span className="font-mono text-zinc-800 dark:text-zinc-200">
                    {vworkjobsCountLoading
                      ? '…'
                      : vworkjobsCountError
                        ? <span className="text-red-600 dark:text-red-400">{vworkjobsCountError}</span>
                        : vworkjobsMatchCount !== null
                          ? vworkjobsMatchCount.toLocaleString()
                          : '—'}
                  </span>
                  <span className="text-[10px] text-zinc-500 dark:text-zinc-500">
                    Same filters as Steps run (incl. “force”){' '}
                    <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">actual_start_time</code> in range
                  </span>
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
                disabled={graceSettingsLoading}
                value={graceSeconds === null ? '' : graceSeconds}
                onChange={(e) => {
                  const t = e.target.value;
                  if (t === '') {
                    setGraceSeconds(null);
                    return;
                  }
                  const n = parseInt(t, 10);
                  setGraceSeconds(Number.isNaN(n) ? null : Math.max(0, Math.min(86400, n)));
                }}
                className="mt-1 block w-full rounded border border-zinc-300 bg-white px-3 py-2 text-sm disabled:opacity-60 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
                Loaded from Settings → GPS Std time (seconds). Override here for this page only; API calls without{' '}
                <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">graceSeconds</code> use that setting.
              </p>
              {graceSettingsError && (
                <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">{graceSettingsError}</p>
              )}
            </div>
            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={runTagging}
                disabled={
                  runStatus === 'running' ||
                  graceSettingsLoading ||
                  graceSeconds === null ||
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
                  graceSettingsLoading ||
                  graceSeconds === null ||
                  !dateFrom ||
                  !dateTo ||
                  dateRange(dateFrom, dateTo).length === 0
                }
                className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                title="For each date: get distinct device_name from tbl_tracking for that day, run Tag Enter/Exit for those devices, then run Steps (vworkjobs for that date). No device selection needed."
              >
                {runStatus === 'running' ? 'Running…' : 'Tag Enter/Exit and Steps'}
              </button>
              <button
                type="button"
                onClick={runStepsOnlyForRange}
                disabled={
                  runStatus === 'running' ||
                  !dateFrom ||
                  !dateTo ||
                  dateRange(dateFrom, dateTo).length === 0
                }
                className="rounded border border-blue-600 bg-white px-4 py-2 text-sm font-medium text-blue-700 hover:bg-blue-50 disabled:opacity-50 dark:border-blue-500 dark:bg-zinc-900 dark:text-blue-300 dark:hover:bg-blue-950/40"
                title="Same Steps pipeline as above, but skips ENTER/EXIT tagging. Use after new GPS fence mappings when only derived steps need refreshing."
              >
                {runStatus === 'running' && stepsOnlyRunActive ? 'Running steps…' : 'Rerun Steps only'}
              </button>
            </div>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              Run tagging: applies the NZ <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">position_time_nz</code> SQL only (not <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">store_fences</code>), then fetches devices for the range and tags ENTER/EXIT. Tag Enter/Exit and Steps: for each day, gets devices for that day from tbl_tracking and runs Enter/Exit then Steps (vworkjobs); the same derived-steps pipeline as Inspect, including <strong>Steps+</strong> (buffered vineyard) when step 2/3 are still missing. <strong>Rerun Steps only</strong> uses the same date range and Steps options (force, window) but does not call tagging — for example after new mappings when tracking tags are already correct. Device lists appear in the log. Run Fence tagging above first if needed.
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
