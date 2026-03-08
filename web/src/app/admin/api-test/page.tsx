'use client';

import { useState, useEffect, useRef } from 'react';
import { runFetchStepsForJobs } from '@/lib/fetch-steps';

const ENDPOINT_OPTIONS: { value: string; label: string; url: string }[] = [
  { value: 'global', label: 'Global', url: 'http://open.10000track.com/route/rest' },
  { value: 'hk', label: 'HK', url: 'https://hk-open.tracksolidpro.com/route/rest' },
  { value: 'eu', label: 'EU', url: 'https://eu-open.tracksolidpro.com/route/rest' },
  { value: 'us', label: 'US', url: 'https://us-open.tracksolidpro.com/route/rest' },
];

interface DeviceItem {
  imei: string;
  deviceName?: string;
}

interface TrackPoint {
  lat: number;
  lng: number;
  gpsTime: string;
}

interface TracksolidDebug {
  endpoint: string;
  method: string;
  requestParamsRedacted: Record<string, string>;
  requestBodyLength: number;
  httpStatus: number;
  responseBody: string;
  timestamp: string;
  fullResponse?: unknown;
  requestedDeviceName?: string;
}

type DevicesDebugPayload =
  | TracksolidDebug
  | { steps: Array<{ step: string; debug: TracksolidDebug | null }> };

type FleetRowStatus = 'idle' | 'loading' | 'saving' | 'merging' | 'done' | 'error';

interface FleetRow {
  deviceName: string;
  imei: string | null; // resolved from Step 2 (Tracksolid device list) when calling API
  points: TrackPoint[] | null;
  recordCount: number | null;
  getApiStatus: FleetRowStatus;
  saveStatus: FleetRowStatus;
  mergeStatus: FleetRowStatus;
  tagStatus: FleetRowStatus;
  tagMessage: string | null; // e.g. "12 tags" or error
  error: string | null;
}

function formatDebugPre(debug: TracksolidDebug): string {
  const params = debug.requestParamsRedacted;
  const bodySent = Object.keys(params)
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join('&');
  return [
    '========== What we sent ==========',
    ...(debug.requestedDeviceName ? [`device name: ${debug.requestedDeviceName}`] : []),
    `endpoint: ${debug.endpoint}`,
    `method: POST`,
    `timestamp (UTC): ${debug.timestamp}`,
    `request body length: ${debug.requestBodyLength}`,
    '',
    'All variables (body, secrets redacted):',
    bodySent,
    '',
    'Params as object:',
    JSON.stringify(debug.requestParamsRedacted, null, 2),
    '',
    '========== What we got back ==========',
    `HTTP status: ${debug.httpStatus}`,
    '',
    'Response body (raw):',
    debug.responseBody,
    ...(debug.fullResponse != null
      ? ['', 'Full response (parsed):', JSON.stringify(debug.fullResponse, null, 2)]
      : []),
  ].join('\n');
}

export default function ApiTestPage() {
  const [devices, setDevices] = useState<DeviceItem[] | null>(null);
  const [devicesError, setDevicesError] = useState<string | null>(null);
  const [devicesAppKeyHint, setDevicesAppKeyHint] = useState<string | null>(null);
  const [devicesDebug, setDevicesDebug] = useState<DevicesDebugPayload | null>(null);
  const [devicesLoading, setDevicesLoading] = useState(false);
  const [trackImei, setTrackImei] = useState('');
  const [trackPoints, setTrackPoints] = useState<TrackPoint[] | null>(null);
  const [trackError, setTrackError] = useState<string | null>(null);
  const [trackDebug, setTrackDebug] = useState<DevicesDebugPayload | null>(null);
  const [trackLoading, setTrackLoading] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [mergeStatus, setMergeStatus] = useState<'idle' | 'merging' | 'merged' | 'error'>('idle');
  const [mergeMessage, setMergeMessage] = useState<string | null>(null);
  const [mergeDebug, setMergeDebug] = useState<{ failedStep?: string; rawSql?: string } | null>(null);
  const [copyFeedback, setCopyFeedback] = useState(false);
  const [endpoint, setEndpoint] = useState<string>('hk');
  const [apiTestTab, setApiTestTab] = useState<'devices' | 'fences'>('devices');
  // Single-day fetch: YYYY-MM-DD (UTC 00:00:00–23:59:59). Default yesterday.
  const [trackDate, setTrackDate] = useState<string>(() => {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - 1);
    return d.toISOString().slice(0, 10);
  });
  const [step3Mode, setStep3Mode] = useState<'single' | 'fleet'>('single');
  const [fleetRows, setFleetRows] = useState<FleetRow[]>([]);
  const [fleetRowsLoading, setFleetRowsLoading] = useState(false);
  const [fleetRowsError, setFleetRowsError] = useState<string | null>(null);
  const [fleetRunAllLoading, setFleetRunAllLoading] = useState(false);
  const [refreshDevicesLoading, setRefreshDevicesLoading] = useState(false);
  const [refreshDevicesMessage, setRefreshDevicesMessage] = useState<string | null>(null);
  const [fencePrepStatus, setFencePrepStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const [fencePrepDurationMs, setFencePrepDurationMs] = useState<number | null>(null);
  const [fencePrepPositionTimeNzMs, setFencePrepPositionTimeNzMs] = useState<number | null>(null);
  const [fencePrepStoreFencesMs, setFencePrepStoreFencesMs] = useState<number | null>(null);
  const [fencePrepError, setFencePrepError] = useState<string | null>(null);
  const [maxDates, setMaxDates] = useState<{
    maxVworkjobsActualStart: string | null;
    maxTrackingPositionTimeNz: string | null;
  } | null>(null);
  const [maxDatesLoading, setMaxDatesLoading] = useState(false);
  const [maxDatesError, setMaxDatesError] = useState<string | null>(null);
  // Step 4: Fetch GPS steps (only for jobs where steps_fetched = false)
  const [step4StartLessMinutes, setStep4StartLessMinutes] = useState(15);
  const [step4EndPlusMinutes, setStep4EndPlusMinutes] = useState(60);
  const [step4Force, setStep4Force] = useState(false);
  const [step4Running, setStep4Running] = useState(false);
  const [step4Progress, setStep4Progress] = useState<{ current: number; total: number } | null>(null);
  const [step4Log, setStep4Log] = useState<{ job_id: string; status: 'ok' | 'skip' | 'error'; message?: string }[]>([]);
  const [step4Error, setStep4Error] = useState<string | null>(null);
  const [step4Debug, setStep4Debug] = useState<{
    jobCount: number;
    date?: string;
    stepsFetchedFilter?: string;
    stepsFetchedFilterApplied?: boolean;
    stepsFetchedFilterSkipped?: boolean;
    whereHint?: string;
  } | null>(null);
  const [resetStepsLoading, setResetStepsLoading] = useState(false);
  const [resetStepsMessage, setResetStepsMessage] = useState<string | null>(null);
  const [fenceSyncLoading, setFenceSyncLoading] = useState(false);
  const [fenceSyncLog, setFenceSyncLog] = useState<Array<{ stage: string; message?: string; current?: number; total?: number; fetched?: number; inserted?: number; updated?: number; deleted?: number; action?: 'inserted' | 'updated'; fence_name?: string }>>([]);
  const [fenceSyncResult, setFenceSyncResult] = useState<{ fetched: number; inserted: number; updated: number; deleted: number } | null>(null);
  const [fenceSyncError, setFenceSyncError] = useState<string | null>(null);
  const fenceSyncLogScrollRef = useRef<HTMLDivElement | null>(null);
  const [fenceListFetchLoading, setFenceListFetchLoading] = useState(false);
  const [fenceListFetched, setFenceListFetched] = useState<Array<{ fenceId: string; name: string; type: string }> | null>(null);
  const [fenceListFetchError, setFenceListFetchError] = useState<string | null>(null);
  const [fenceListFetchDebug, setFenceListFetchDebug] = useState<{
    requestDebug?: { endpointSource?: string | null; endpointValue?: string | null; resolvedBaseUrl?: string; note?: string };
    requestParamsRedacted?: Record<string, string>;
    endpoint?: string;
    method?: string;
    httpStatus?: number;
    responseBody?: string;
    fullResponse?: unknown;
  } | null>(null);

  const fetchMaxDates = async () => {
    setMaxDatesError(null);
    setMaxDatesLoading(true);
    try {
      const res = await fetch('/api/admin/import-max-dates');
      const data = await res.json();
      if (!data.ok) {
        setMaxDatesError(data.error ?? 'Failed to load max dates');
        setMaxDates(null);
        return;
      }
      setMaxDates({
        maxVworkjobsActualStart: data.maxVworkjobsActualStart ?? null,
        maxTrackingPositionTimeNz: data.maxTrackingPositionTimeNz ?? null,
      });
    } catch (e) {
      setMaxDatesError(e instanceof Error ? e.message : String(e));
      setMaxDates(null);
    } finally {
      setMaxDatesLoading(false);
    }
  };

  const saveToDatabase = async () => {
    if (!trackPoints?.length || !trackImei.trim()) return;
    const deviceName = devices?.find((d) => d.imei === trackImei)?.deviceName ?? trackImei;
    setSaveStatus('saving');
    setSaveMessage(null);
    try {
      const res = await fetch('/api/apifeed/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ deviceName, imei: trackImei.trim() || undefined, points: trackPoints }),
      });
      const data = await res.json();
      if (!data.ok) {
        setSaveStatus('error');
        setSaveMessage(data.error ?? 'Save failed');
        return;
      }
      setSaveStatus('saved');
      const parts = [`Inserted ${data.inserted} rows into tbl_apifeed`];
      if (data.deleted != null && data.deleted > 0) parts.push(`${data.deleted} existing rows deleted for range`);
      if (data.skipped) parts.push(`${data.skipped} skipped`);
      setSaveMessage(parts.join(' · ') + '.');
      setTimeout(() => {
        setSaveStatus('idle');
        setSaveMessage(null);
      }, 4000);
    } catch (e) {
      setSaveStatus('error');
      setSaveMessage(e instanceof Error ? e.message : String(e));
    }
  };

  const mergeToTracking = async () => {
    if (!trackPoints?.length || !trackImei.trim()) return;
    const times = trackPoints.map((p) => p.gpsTime).filter(Boolean) as string[];
    if (times.length === 0) {
      setMergeMessage('No valid times to merge');
      setMergeStatus('error');
      return;
    }
    const sorted = [...times].sort();
    const minTime = sorted[0];
    const maxTime = sorted[sorted.length - 1];
    const deviceName = devices?.find((d) => d.imei === trackImei)?.deviceName ?? trackImei;
    setMergeStatus('merging');
    setMergeMessage(null);
    setMergeDebug(null);
    try {
      const res = await fetch('/api/apifeed/merge-tracking', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ deviceName, minTime, maxTime }),
      });
      const data = await res.json();
      if (!data.ok) {
        setMergeStatus('error');
        setMergeMessage(data.error ?? 'Merge failed');
        setMergeDebug(
          data.failedStep || data.rawSql
            ? { failedStep: data.failedStep, rawSql: data.rawSql }
            : null
        );
        return;
      }
      setMergeStatus('merged');
      setMergeMessage(`${data.inserted} rows merged into tbl_tracking${data.deleted ? ` (${data.deleted} old rows deleted)` : ''}.`);
      setMergeDebug(null);
      setTimeout(() => {
        setMergeStatus('idle');
        setMergeMessage(null);
      }, 4000);
    } catch (e) {
      setMergeStatus('error');
      setMergeMessage(e instanceof Error ? e.message : String(e));
      setMergeDebug(null);
    }
  };

  const updateFleetRow = (deviceName: string, upd: Partial<FleetRow>) => {
    setFleetRows((prev) => prev.map((r) => (r.deviceName === deviceName ? { ...r, ...upd } : r)));
  };

  const resolveImei = (deviceName: string): string | null =>
    devices?.find((d) => d.deviceName === deviceName)?.imei ?? null;

  const fetchTrackForDevice = async (deviceName: string): Promise<TrackPoint[] | null> => {
    const imei = fleetRows.find((r) => r.deviceName === deviceName)?.imei ?? resolveImei(deviceName);
    if (!imei?.trim()) {
      updateFleetRow(deviceName, {
        getApiStatus: 'error',
        error: 'Run Step 2 (Fetch devices) first to resolve IMEI for this device.',
      });
      return null;
    }
    updateFleetRow(deviceName, { getApiStatus: 'loading', error: null, imei: imei || null });
    try {
      const params = new URLSearchParams({
        imei,
        endpoint,
        date: trackDate,
        deviceName,
      });
      const res = await fetch(`/api/tracksolid/track?${params}`);
      const data = await res.json();
      if (!data.ok) {
        updateFleetRow(deviceName, {
          getApiStatus: 'error',
          error: data.error ?? 'Fetch failed',
          points: null,
          recordCount: null,
        });
        return null;
      }
      const points: TrackPoint[] = data.points ?? [];
      updateFleetRow(deviceName, {
        points,
        recordCount: points.length,
        getApiStatus: 'done',
        error: null,
        imei,
      });
      return points;
    } catch (e) {
      updateFleetRow(deviceName, {
        getApiStatus: 'error',
        error: e instanceof Error ? e.message : String(e),
      });
      return null;
    }
  };

  const saveForDevice = async (deviceName: string, pointsOverride?: TrackPoint[] | null) => {
    const row = fleetRows.find((r) => r.deviceName === deviceName);
    const points = pointsOverride ?? row?.points;
    if (!points?.length) return;
    const imei = row?.imei ?? resolveImei(deviceName) ?? '';
    updateFleetRow(deviceName, { saveStatus: 'saving', error: null });
    try {
      const res = await fetch('/api/apifeed/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          deviceName,
          imei: imei || undefined,
          points,
        }),
      });
      const data = await res.json();
      if (!data.ok) {
        updateFleetRow(deviceName, { saveStatus: 'error', error: data.error ?? 'Save failed' });
        return;
      }
      updateFleetRow(deviceName, { saveStatus: 'done', error: null });
    } catch (e) {
      updateFleetRow(deviceName, {
        saveStatus: 'error',
        error: e instanceof Error ? e.message : String(e),
      });
    }
  };

  const mergeForDevice = async (deviceName: string, pointsOverride?: TrackPoint[] | null) => {
    const row = fleetRows.find((r) => r.deviceName === deviceName);
    const points = pointsOverride ?? row?.points;
    if (!points?.length) return;
    const times = points.map((p) => p.gpsTime).filter(Boolean) as string[];
    if (times.length === 0) return;
    const sorted = [...times].sort();
    const minTime = sorted[0];
    const maxTime = sorted[sorted.length - 1];
    updateFleetRow(deviceName, { mergeStatus: 'merging', error: null });
    try {
      const res = await fetch('/api/apifeed/merge-tracking', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          deviceName,
          minTime,
          maxTime,
        }),
      });
      const data = await res.json();
      if (!data.ok) {
        updateFleetRow(deviceName, {
          mergeStatus: 'error',
          error: data.error ?? 'Merge failed',
        });
        return;
      }
      updateFleetRow(deviceName, { mergeStatus: 'done', error: null });
    } catch (e) {
      updateFleetRow(deviceName, {
        mergeStatus: 'error',
        error: e instanceof Error ? e.message : String(e),
      });
    }
  };

  const TAG_GRACE_SECONDS = 300;

  const tagForDevice = async (deviceName: string) => {
    if (!trackDate?.trim()) return;
    updateFleetRow(deviceName, { tagStatus: 'merging', tagMessage: null });
    try {
      const res = await fetch('/api/admin/tagging/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateX: trackDate.trim(),
          deviceNames: [deviceName],
          graceSeconds: TAG_GRACE_SECONDS,
        }),
      });
      if (!res.ok || !res.body) {
        const text = await res.text();
        let err = text;
        try {
          const j = JSON.parse(text);
          if (j?.error) err = j.error;
        } catch {
          if (text) err = text;
        }
        updateFleetRow(deviceName, { tagStatus: 'error', tagMessage: err || `HTTP ${res.status}` });
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let lastError: string | null = null;
      let totalTags = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const ev = JSON.parse(line) as { stage?: string; message?: string; summary?: { totalUpdatesWritten?: number } };
            if (ev.stage === 'error' && ev.message) lastError = ev.message;
            if (ev.stage === 'done' && ev.summary != null) totalTags = ev.summary.totalUpdatesWritten ?? 0;
          } catch {
            /* ignore parse */
          }
        }
      }
      if (buffer.trim()) {
        try {
          const ev = JSON.parse(buffer) as { stage?: string; message?: string; summary?: { totalUpdatesWritten?: number } };
          if (ev.stage === 'error' && ev.message) lastError = ev.message;
          if (ev.stage === 'done' && ev.summary != null) totalTags = ev.summary.totalUpdatesWritten ?? 0;
        } catch {
          /* ignore */
        }
      }
      if (lastError) {
        updateFleetRow(deviceName, { tagStatus: 'error', tagMessage: lastError });
      } else {
        updateFleetRow(deviceName, { tagStatus: 'done', tagMessage: `${totalTags} tags` });
      }
    } catch (e) {
      updateFleetRow(deviceName, {
        tagStatus: 'error',
        tagMessage: e instanceof Error ? e.message : String(e),
      });
    }
  };

  const runAllForDate = async () => {
    if (!fleetRows.length) return;
    setFleetRunAllLoading(true);
    for (const row of fleetRows) {
      const points = await fetchTrackForDevice(row.deviceName);
      if (points?.length) {
        await saveForDevice(row.deviceName, points);
        await mergeForDevice(row.deviceName, points);
      }
    }
    setFencePrepStatus('running');
    setFencePrepError(null);
    setFencePrepDurationMs(null);
    setFencePrepPositionTimeNzMs(null);
    setFencePrepStoreFencesMs(null);
    const fencePrepStart = Date.now();
    try {
      const globalRes = await fetch('/api/admin/tracking/apply-position-time-nz-and-fences', { method: 'POST' });
      const globalData = await globalRes.json();
      const fencePrepDuration = Date.now() - fencePrepStart;
      setFencePrepDurationMs(fencePrepDuration);
      if (globalData.positionTimeNzMs != null) setFencePrepPositionTimeNzMs(globalData.positionTimeNzMs);
      if (globalData.storeFencesMs != null) setFencePrepStoreFencesMs(globalData.storeFencesMs);
      if (!globalData.ok) {
        setFencePrepStatus('error');
        setFencePrepError(globalData.error ?? 'Fence_Prep failed');
        console.error('[runAll] position_time_nz + store_fences failed:', globalData.error);
      } else {
        setFencePrepStatus('done');
      }
    } catch (e) {
      setFencePrepStatus('error');
      setFencePrepError(e instanceof Error ? e.message : String(e));
      setFencePrepDurationMs(Date.now() - fencePrepStart);
    }
    for (const row of fleetRows) {
      await tagForDevice(row.deviceName);
    }
    setFleetRunAllLoading(false);
  };

  const refreshDevicesFromVworkjobs = async () => {
    setRefreshDevicesMessage(null);
    setRefreshDevicesLoading(true);
    try {
      const res = await fetch('/api/admin/devices/sync-from-vworkjobs', { method: 'POST' });
      const data = await res.json();
      if (!data.ok) {
        setRefreshDevicesMessage(data.error ?? 'Sync failed');
        return;
      }
      const msg = data.inserted > 0
        ? `Added ${data.inserted} device(s) from vworkjobs. Total tbl_devices: ${data.totalDevices}.`
        : `No new devices. All ${data.totalWorkers} worker(s) from vworkjobs already in tbl_devices.`;
      setRefreshDevicesMessage(msg);
      await loadFleetDevices();
      await fetchMaxDates();
    } catch (e) {
      setRefreshDevicesMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshDevicesLoading(false);
    }
  };

  /** Fetch GPS steps for jobs on trackDate. If !step4Force, only jobs with steps_fetched=false; if step4Force, all jobs for date. Uses single runFetchStepsForJobs from lib. */
  const runStep4FetchGpsSteps = async () => {
    if (!trackDate?.trim()) return;
    setStep4Error(null);
    setStep4Debug(null);
    setStep4Running(true);
    setStep4Progress({ current: 0, total: 0 });
    setStep4Log([]);
    try {
      const url = step4Force
        ? `/api/vworkjobs?date=${encodeURIComponent(trackDate)}`
        : `/api/vworkjobs?date=${encodeURIComponent(trackDate)}&stepsFetched=false`;
      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok) {
        setStep4Error(data?.error ?? 'Failed to load jobs');
        setStep4Running(false);
        return;
      }
      const jobs: Record<string, unknown>[] = data.rows ?? [];
      const apiDebug = data.debug ?? {};
      setStep4Debug({
        jobCount: jobs.length,
        date: apiDebug.date ?? trackDate,
        stepsFetchedFilter: apiDebug.stepsFetchedFilter,
        stepsFetchedFilterApplied: apiDebug.stepsFetchedFilterApplied,
        stepsFetchedFilterSkipped: apiDebug.stepsFetchedFilterSkipped,
        whereHint: apiDebug.whereHint,
      });
      setStep4Progress({ current: 0, total: jobs.length });
      const result = await runFetchStepsForJobs({
        jobs,
        startLessMinutes: step4StartLessMinutes,
        endPlusMinutes: step4EndPlusMinutes,
        onProgress: (current, total, log) => {
          setStep4Progress({ current, total });
          setStep4Log(log);
        },
      });
      setStep4Log(result.log);
      setStep4Error(null);
    } catch (e) {
      setStep4Error(e instanceof Error ? e.message : String(e));
    } finally {
      setStep4Running(false);
    }
  };

  const resetStepsAll = async () => {
    setResetStepsMessage(null);
    setResetStepsLoading(true);
    try {
      const res = await fetch('/api/admin/vworkjobs/reset-steps', { method: 'POST' });
      const data = await res.json();
      if (!data.ok) {
        setResetStepsMessage(data.error ?? 'Reset failed');
        return;
      }
      setResetStepsMessage(`Set steps_fetched = false for ${data.updated ?? 0} row(s) in tbl_vworkjobs.`);
      setTimeout(() => setResetStepsMessage(null), 6000);
    } catch (e) {
      setResetStepsMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setResetStepsLoading(false);
    }
  };

  const loadFleetDevices = async () => {
    setFleetRowsError(null);
    setFleetRowsLoading(true);
    try {
      const res = await fetch('/api/devices');
      const data = await res.json();
      if (!data.ok) {
        setFleetRowsError(data.error ?? 'Failed to load tbl_devices');
        setFleetRows([]);
        return;
      }
      const list = data.devices ?? [];
      setFleetRows(
        list.map((d: { deviceName?: string }) => ({
          deviceName: d.deviceName ?? '',
          imei: null as string | null,
          points: null,
          recordCount: null,
          getApiStatus: 'idle' as FleetRowStatus,
          saveStatus: 'idle' as FleetRowStatus,
          mergeStatus: 'idle' as FleetRowStatus,
          tagStatus: 'idle' as FleetRowStatus,
          tagMessage: null as string | null,
          error: null,
        }))
      );
    } catch (e) {
      setFleetRowsError(e instanceof Error ? e.message : String(e));
      setFleetRows([]);
    } finally {
      setFleetRowsLoading(false);
    }
  };

  useEffect(() => {
    if (step3Mode === 'fleet') loadFleetDevices();
  }, [step3Mode]);

  useEffect(() => {
    void fetchMaxDates();
  }, []);

  useEffect(() => {
    if (!devices?.length || !fleetRows.length) return;
    setFleetRows((prev) =>
      prev.map((r) => ({
        ...r,
        imei: r.imei ?? devices?.find((d) => d.deviceName === r.deviceName)?.imei ?? null,
      }))
    );
  }, [devices?.length, fleetRows.length]);

  useEffect(() => {
    fenceSyncLogScrollRef.current?.scrollTo({ top: fenceSyncLogScrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [fenceSyncLog.length]);

  const copyDebugPayload = (payload: DevicesDebugPayload) => {
    const parts: string[] = ['--- Tracksolid API debug (for tech support) ---'];
    if ('steps' in payload) {
      for (const { step, debug } of payload.steps) {
        parts.push('', `=== Step: ${step} ===`, debug != null ? formatDebugPre(debug) : '(no request – e.g. cached)');
      }
    } else {
      parts.push('', formatDebugPre(payload));
    }
    void navigator.clipboard.writeText(parts.join('\n')).then(() => {
      setCopyFeedback(true);
      setTimeout(() => setCopyFeedback(false), 2000);
    });
  };

  const fetchDevices = async (forceNewToken = false) => {
    setDevicesError(null);
    setDevicesAppKeyHint(null);
    setDevicesDebug(null);
    setDevices(null);
    setDevicesLoading(true);
    try {
      const params = new URLSearchParams({ endpoint });
      if (forceNewToken) params.set('refreshToken', '1');
      const res = await fetch(`/api/tracksolid/devices?${params}`);
      const data = await res.json();
      if (!data.ok) {
        setDevicesError(data.error ?? 'Request failed');
        setDevicesAppKeyHint(data.appKeyHint ?? null);
        setDevicesDebug(data.debug ?? null);
        return;
      }
      const list = data.devices ?? [];
      setDevices(list);
      setDevicesDebug(data.debug ?? null);
      if (list.length) {
        setTrackImei((prev) => (prev ? prev : list[0].imei ?? ''));
      }
    } catch (e) {
      setDevicesError(e instanceof Error ? e.message : String(e));
    } finally {
      setDevicesLoading(false);
    }
  };

  const fetchTrack = async () => {
    const imei = trackImei.trim();
    if (!imei) return;
    setTrackError(null);
    setTrackDebug(null);
    setTrackPoints(null);
    setSaveStatus('idle');
    setSaveMessage(null);
    setMergeStatus('idle');
    setMergeMessage(null);
    setMergeDebug(null);
    setTrackLoading(true);
    const deviceName = devices?.find((d) => d.imei === imei)?.deviceName ?? '';
    try {
      const params = new URLSearchParams({
        imei,
        endpoint,
        ...(trackDate ? { date: trackDate } : { period: 'last24h' }),
        ...(deviceName && { deviceName }),
      });
      const res = await fetch(`/api/tracksolid/track?${params}`);
      const data = await res.json();
      if (!data.ok) {
        let errMsg = data.error ?? 'Request failed';
        if (data.rateLimitHint) {
          errMsg += ' Token rate limit: wait ~1 minute then try again (or run Step 1 first to cache a token).';
        }
        setTrackError(errMsg);
        setTrackDebug(data.debug ?? null);
        return;
      }
      setTrackPoints(data.points ?? []);
      setTrackDebug(data.debug ?? null);
    } catch (e) {
      setTrackError(e instanceof Error ? e.message : String(e));
    } finally {
      setTrackLoading(false);
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-1 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">API GPS Import</h1>
      <p className="mb-4 text-sm text-zinc-500 dark:text-zinc-400">
        Tracksolid Pro OpenAPI (raw GPS vendor). Choose endpoint to test.
      </p>

      <div className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <label className="mb-2 block text-sm font-medium text-zinc-700 dark:text-zinc-300">
          Endpoint (base URL)
        </label>
        <div className="flex flex-wrap gap-2">
          {ENDPOINT_OPTIONS.map((opt) => (
            <label
              key={opt.value}
              className="flex cursor-pointer items-center gap-2 rounded border border-zinc-300 px-3 py-2 has-[:checked]:border-zinc-800 has-[:checked]:bg-zinc-100 dark:border-zinc-600 has-[:checked]:dark:border-zinc-400 has-[:checked]:dark:bg-zinc-800"
            >
              <input
                type="radio"
                name="endpoint"
                value={opt.value}
                checked={endpoint === opt.value}
                onChange={() => setEndpoint(opt.value)}
                className="sr-only"
              />
              <span className="font-medium">{opt.label}</span>
              <span className="text-xs text-zinc-500 dark:text-zinc-400">{opt.url}</span>
            </label>
          ))}
        </div>
      </div>

      <div className="mb-4 flex gap-1 rounded-lg border border-zinc-200 bg-zinc-100 p-1 dark:border-zinc-700 dark:bg-zinc-800">
        <button
          type="button"
          onClick={() => setApiTestTab('devices')}
          className={`rounded-md px-4 py-2 text-sm font-medium transition-colors ${
            apiTestTab === 'devices'
              ? 'bg-white text-zinc-900 shadow dark:bg-zinc-700 dark:text-zinc-100'
              : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100'
          }`}
        >
          Devices
        </button>
        <button
          type="button"
          onClick={() => setApiTestTab('fences')}
          className={`rounded-md px-4 py-2 text-sm font-medium transition-colors ${
            apiTestTab === 'fences'
              ? 'bg-white text-zinc-900 shadow dark:bg-zinc-700 dark:text-zinc-100'
              : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100'
          }`}
        >
          Fences
        </button>
      </div>

      {apiTestTab === 'devices' && (
      <>
      <section className="mb-8 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">Devices</h2>
        <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
          Fetch list of device names (and IMEI) for the configured account. Use “Force new token” if the cached token is expired or you rotated credentials.
        </p>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => fetchDevices(false)}
            disabled={devicesLoading}
            className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
          >
            {devicesLoading ? 'Fetching…' : 'Fetch devices'}
          </button>
          <button
            type="button"
            onClick={() => fetchDevices(true)}
            disabled={devicesLoading}
            className="rounded border border-zinc-400 bg-white px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-500 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
          >
            Force new token
          </button>
        </div>
        {devicesError && (
          <div className="mt-3 space-y-1">
            <p className="text-sm text-red-600 dark:text-red-400">{devicesError}</p>
            {devicesAppKeyHint && (
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                {devicesAppKeyHint} Restart the dev server after changing <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">web/.env.local</code> (Ctrl+C, then <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">npm run dev</code>).
              </p>
            )}
          </div>
        )}
        {devices && (
          <div className="mt-4">
            <p className="mb-2 text-sm font-medium text-zinc-700 dark:text-zinc-300">
              Count: {devices.length}
            </p>
            <div className="max-h-96 overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
              <table className="w-full min-w-[320px] border-collapse text-sm">
                <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
                  <tr>
                    <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                      Device name
                    </th>
                    <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                      IMEI
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {devices.map((d, i) => (
                    <tr
                      key={d.imei || i}
                      className="border-b border-zinc-100 dark:border-zinc-800"
                    >
                      <td className="px-3 py-2 font-medium text-zinc-800 dark:text-zinc-200">
                        {d.deviceName ?? '—'}
                      </td>
                      <td className="px-3 py-2 font-mono text-zinc-600 dark:text-zinc-400">
                        {d.imei || '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
        {devicesDebug && (
          <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 dark:border-zinc-700 dark:bg-zinc-800/50">
            <p className="mb-2 text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">
              Debug – request & response (every step)
            </p>
            {'steps' in devicesDebug ? (
              <>
                {devicesDebug.steps.map(({ step, debug }, i) => (
                  <div key={i} className="mb-4 last:mb-0">
                    <p className="mb-1 font-medium text-zinc-700 dark:text-zinc-300">
                      Step {i + 1}: {step}
                    </p>
                    <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-all rounded bg-white p-2 font-mono text-xs dark:bg-zinc-900">
                      {debug != null ? formatDebugPre(debug) : '(cached – no request sent)'}
                    </pre>
                  </div>
                ))}
                <button
                  type="button"
                  onClick={() => copyDebugPayload(devicesDebug)}
                  className="mt-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                >
                  {copyFeedback ? 'Copied!' : 'Copy for tech support'}
                </button>
              </>
            ) : (
              <>
                <pre className="max-h-96 overflow-auto whitespace-pre-wrap break-all rounded bg-white p-2 font-mono text-xs dark:bg-zinc-900">
                  {formatDebugPre(devicesDebug)}
                </pre>
                <button
                  type="button"
                  onClick={() => copyDebugPayload(devicesDebug)}
                  className="mt-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                >
                  {copyFeedback ? 'Copied!' : 'Copy for tech support'}
                </button>
              </>
            )}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">
          Step 3: GPS for day
        </h2>
        <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
          Pick a device and date, or run all devices (fleet) for one date. A row is written to tbl_logs per fetch (logtype=APIfetch). Run Step 2 first so the token is cached.
        </p>
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Do this first:</span>
          <button
            type="button"
            onClick={refreshDevicesFromVworkjobs}
            disabled={refreshDevicesLoading || fleetRowsLoading}
            className="rounded border border-zinc-400 bg-white px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-500 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
            title="Sync workers from tbl_vworkjobs into tbl_devices (with !group=Newt), then load the device list"
          >
            {refreshDevicesLoading ? 'Syncing…' : fleetRowsLoading ? 'Loading…' : 'Harvest and Refresh Devices'}
          </button>
          {refreshDevicesMessage && (
            <span className="text-sm text-zinc-600 dark:text-zinc-400">{refreshDevicesMessage}</span>
          )}
          <span className="ml-2 text-zinc-400 dark:text-zinc-500">|</span>
          {maxDatesLoading ? (
            <span className="text-sm text-zinc-500 dark:text-zinc-400">Loading max dates…</span>
          ) : maxDatesError ? (
            <span className="text-sm text-red-600 dark:text-red-400" title={maxDatesError}>
              Max dates: error
            </span>
          ) : maxDates ? (
            <span className="text-sm text-zinc-600 dark:text-zinc-400">
              <span title={`tbl_vworkjobs.actual_start_time: ${maxDates.maxVworkjobsActualStart ?? '—'}`}>
                vworkjobs max: <strong className="font-mono">{maxDates.maxVworkjobsActualStart ? maxDates.maxVworkjobsActualStart.slice(0, 10) : '—'}</strong>
              </span>
              {' · '}
              <span title={`tbl_tracking.position_time_nz: ${maxDates.maxTrackingPositionTimeNz ?? '—'}`}>
                tracking max: <strong className="font-mono">{maxDates.maxTrackingPositionTimeNz ? maxDates.maxTrackingPositionTimeNz.slice(0, 10) : '—'}</strong>
              </span>
              <button
                type="button"
                onClick={() => void fetchMaxDates()}
                className="ml-1.5 rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                title="Refresh max dates"
              >
                Refresh
              </button>
            </span>
          ) : null}
        </div>
        <div className="mb-3 flex flex-wrap items-center gap-4">
          <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Mode</span>
          <label className="flex cursor-pointer items-center gap-2">
            <input
              type="radio"
              name="step3Mode"
              checked={step3Mode === 'single'}
              onChange={() => setStep3Mode('single')}
              className="rounded-full"
            />
            <span className="text-sm">Single device</span>
          </label>
          <label className="flex cursor-pointer items-center gap-2">
            <input
              type="radio"
              name="step3Mode"
              checked={step3Mode === 'fleet'}
              onChange={() => setStep3Mode('fleet')}
              className="rounded-full"
            />
            <span className="text-sm">Fleet (all devices)</span>
          </label>
        </div>

        {step3Mode === 'fleet' ? (
          <>
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <label className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Date</label>
              <input
                type="date"
                value={trackDate}
                onChange={(e) => setTrackDate(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              />
              <button
                type="button"
                onClick={loadFleetDevices}
                disabled={fleetRowsLoading}
                className="rounded border border-zinc-400 bg-white px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-500 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
              >
                {fleetRowsLoading ? 'Loading…' : 'Refresh from tbl_devices'}
              </button>
              <button
                type="button"
                onClick={runAllForDate}
                disabled={fleetRunAllLoading || !fleetRows.length}
                className="rounded bg-emerald-700 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-800 disabled:opacity-50"
              >
                {fleetRunAllLoading ? 'Running all…' : 'Run all (Fetch → apifeed → Merge → position_time_nz + store_fences → Tag)'}
              </button>
            </div>
            {fleetRowsLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading devices from tbl_devices…</p>
            ) : fleetRowsError ? (
              <div className="space-y-2">
                <p className="text-sm text-red-600 dark:text-red-400">{fleetRowsError}</p>
                <button
                  type="button"
                  onClick={loadFleetDevices}
                  className="rounded bg-zinc-600 px-3 py-1.5 text-sm text-white hover:bg-zinc-700"
                >
                  Retry
                </button>
              </div>
            ) : !fleetRows.length ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">
                No rows in tbl_devices. Add devices you want to track to that table.
              </p>
            ) : (
              <>
                {!devices?.length && (
                  <p className="mb-2 text-sm text-amber-600 dark:text-amber-400">
                    Run Step 2 (Fetch devices) first so IMEI is resolved for Get API.
                  </p>
                )}
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[560px] border-collapse text-sm">
                  <thead className="bg-zinc-100 dark:bg-zinc-800">
                    <tr>
                      <th colSpan={4} className="border-b border-zinc-200 px-3 py-1 text-left text-xs font-normal text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                        {' '}
                      </th>
                      <th className="border-b border-zinc-200 px-3 py-1 text-left text-xs font-normal text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                        {fencePrepStatus === 'running' && 'Mid-loop prep: running…'}
                        {fencePrepStatus === 'done' && (
                          <>
                            position_time_nz: {fencePrepPositionTimeNzMs != null ? `${(fencePrepPositionTimeNzMs / 1000).toFixed(1)}s` : '—'}
                            {' · '}
                            store_fences: {fencePrepStoreFencesMs != null ? `${(fencePrepStoreFencesMs / 1000).toFixed(1)}s` : '—'}
                            {fencePrepDurationMs != null && ` (total ${(fencePrepDurationMs / 1000).toFixed(1)}s)`}
                          </>
                        )}
                        {fencePrepStatus === 'error' && (fencePrepError ? `Mid-loop prep: ✗ ${fencePrepError}` : 'Mid-loop prep: failed')}
                        {fencePrepStatus === 'idle' && 'Mid-loop prep: —'}
                      </th>
                    </tr>
                    <tr>
                      <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium dark:border-zinc-700">
                        device_name
                      </th>
                      <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium dark:border-zinc-700">
                        Fetched API
                      </th>
                      <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium dark:border-zinc-700">
                        DB tbl_apifeed
                      </th>
                      <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium dark:border-zinc-700">
                        Merged tbl_tracking
                      </th>
                      <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium dark:border-zinc-700">
                        Tagged
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {fleetRows.map((row) => (
                      <tr key={row.deviceName} className="border-b border-zinc-100 dark:border-zinc-800">
                        <td className="px-3 py-2 font-medium">{row.deviceName}</td>
                        <td className="px-3 py-2">
                          <button
                            type="button"
                            onClick={() => fetchTrackForDevice(row.deviceName)}
                            disabled={row.getApiStatus === 'loading'}
                            className="rounded bg-zinc-600 px-2 py-1 text-xs font-medium text-white hover:bg-zinc-700 disabled:opacity-50"
                          >
                            {row.getApiStatus === 'loading' ? '…' : 'Get API'}
                          </button>
                          <span className="ml-2 text-zinc-600 dark:text-zinc-400">
                            {row.getApiStatus === 'done' && row.recordCount != null
                              ? `${row.recordCount} rows`
                              : row.getApiStatus === 'error'
                                ? <span className="text-red-600 dark:text-red-400" title={row.error ?? ''}>✗</span>
                                : '—'}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <button
                            type="button"
                            onClick={() => saveForDevice(row.deviceName)}
                            disabled={!row.points?.length || row.saveStatus === 'saving'}
                            className="rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                          >
                            {row.saveStatus === 'saving' ? '…' : 'Save'}
                          </button>
                          <span className="ml-2 text-zinc-600 dark:text-zinc-400">
                            {row.saveStatus === 'done' ? 'Done' : row.saveStatus === 'error' ? '✗' : '—'}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <button
                            type="button"
                            onClick={() => mergeForDevice(row.deviceName)}
                            disabled={!row.points?.length || row.mergeStatus === 'merging'}
                            className="rounded bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-50"
                          >
                            {row.mergeStatus === 'merging' ? '…' : 'Merge'}
                          </button>
                          <span className="ml-2 text-zinc-600 dark:text-zinc-400">
                            {row.mergeStatus === 'done' ? 'Done' : row.mergeStatus === 'error' ? '✗' : '—'}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <button
                            type="button"
                            onClick={() => tagForDevice(row.deviceName)}
                            disabled={row.tagStatus === 'merging'}
                            className="rounded bg-violet-600 px-2 py-1 text-xs font-medium text-white hover:bg-violet-700 disabled:opacity-50"
                            title={`Run ENTER/EXIT tagging for this device for date ${trackDate}`}
                          >
                            {row.tagStatus === 'merging' ? '…' : 'Tag'}
                          </button>
                          <span className="ml-2 text-zinc-600 dark:text-zinc-400" title={row.tagMessage ?? undefined}>
                            {row.tagStatus === 'done' ? row.tagMessage ?? 'Done' : row.tagStatus === 'error' ? (row.tagMessage ? `✗ ${row.tagMessage}` : '✗') : '—'}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              </>
            )}
          </>
        ) : (
          <>
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <label className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
            Device
          </label>
          <select
            value={trackImei}
            onChange={(e) => setTrackImei(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          >
            <option value="">{devices?.length ? 'Select device…' : 'Fetch devices first'}</option>
            {devices?.map((d) => (
              <option key={d.imei} value={d.imei}>
                {d.deviceName ?? d.imei}
              </option>
            ))}
          </select>
          <label className="ml-2 text-sm font-medium text-zinc-700 dark:text-zinc-300">
            Date
          </label>
          <input
            type="date"
            value={trackDate}
            onChange={(e) => setTrackDate(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          <button
            type="button"
            onClick={fetchTrack}
            disabled={trackLoading || !trackImei.trim()}
            className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
          >
            {trackLoading ? 'Fetching…' : `Fetch GPS for ${trackDate || 'day'}`}
          </button>
        </div>
        {trackError && (
          <p className="mb-2 text-sm text-red-600 dark:text-red-400">{trackError}</p>
        )}
        {trackPoints && (() => {
          const times = trackPoints.map((p) => p.gpsTime).filter(Boolean) as string[];
          const sorted = [...times].sort();
          const minTime = sorted[0] ?? '—';
          const maxTime = sorted[sorted.length - 1] ?? '—';
          return (
          <div className="mt-4">
            <div className="mb-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
              <span className="font-medium text-zinc-700 dark:text-zinc-300">
                Total rows: {trackPoints.length}
              </span>
              <span className="text-zinc-600 dark:text-zinc-400">
                Min datetime: <span className="font-mono">{minTime}</span>
              </span>
              <span className="text-zinc-600 dark:text-zinc-400">
                Max datetime: <span className="font-mono">{maxTime}</span>
              </span>
              <button
                type="button"
                onClick={saveToDatabase}
                disabled={saveStatus === 'saving'}
                className="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {saveStatus === 'saving' ? 'Saving…' : 'Save to database'}
              </button>
              <button
                type="button"
                onClick={mergeToTracking}
                disabled={mergeStatus === 'merging'}
                className="rounded bg-blue-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-50"
              >
                {mergeStatus === 'merging' ? 'Merging…' : 'Merge to tbl_tracking'}
              </button>
            </div>
            {(saveMessage || mergeMessage) && (
              <p className={`mb-2 text-sm ${(saveStatus === 'error' || mergeStatus === 'error') ? 'text-red-600 dark:text-red-400' : 'text-zinc-600 dark:text-zinc-400'}`}>
                {saveMessage ?? mergeMessage}
              </p>
            )}
            {mergeDebug && (mergeDebug.failedStep || mergeDebug.rawSql) && (
              <details className="mb-2 text-xs">
                <summary className="cursor-pointer text-zinc-500 dark:text-zinc-400">Debug: failed step &amp; raw SQL</summary>
                {mergeDebug.failedStep && (
                  <p className="mt-1 font-medium">Failed step: {mergeDebug.failedStep}</p>
                )}
                {mergeDebug.rawSql && (
                  <pre className="mt-1 max-h-48 overflow-auto rounded bg-zinc-100 p-2 font-mono dark:bg-zinc-800">
                    {mergeDebug.rawSql}
                  </pre>
                )}
              </details>
            )}
            <div className="max-h-96 overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
              <table className="w-full min-w-[360px] border-collapse text-sm">
                <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
                  <tr>
                    <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                      Device name
                    </th>
                    <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                      Location (lat, lng)
                    </th>
                    <th className="border-b border-zinc-200 px-3 py-2 text-left font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                      Time
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {trackPoints.map((p, i) => (
                    <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-3 py-2 font-medium text-zinc-800 dark:text-zinc-200">
                        {devices?.find((d) => d.imei === trackImei)?.deviceName ?? trackImei}
                      </td>
                      <td className="px-3 py-2 font-mono text-zinc-600 dark:text-zinc-400">
                        {p.lat}, {p.lng}
                      </td>
                      <td className="px-3 py-2 text-zinc-600 dark:text-zinc-400">
                        {p.gpsTime || '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          );
        })()}
        {trackDebug && (
          <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 dark:border-zinc-700 dark:bg-zinc-800/50">
            <p className="mb-2 text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">
              Debug – request & response (every step)
            </p>
            {'steps' in trackDebug ? (
              <>
                {trackDebug.steps.map(({ step, debug }, i) => (
                  <div key={i} className="mb-4 last:mb-0">
                    <p className="mb-1 font-medium text-zinc-700 dark:text-zinc-300">
                      Step {i + 1}: {step}
                    </p>
                    <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-all rounded bg-white p-2 font-mono text-xs dark:bg-zinc-900">
                      {debug != null ? formatDebugPre(debug) : '(cached – no request sent)'}
                    </pre>
                  </div>
                ))}
                <button
                  type="button"
                  onClick={() => copyDebugPayload(trackDebug)}
                  className="mt-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                >
                  {copyFeedback ? 'Copied!' : 'Copy for tech support'}
                </button>
              </>
            ) : (
              <>
                <pre className="max-h-96 overflow-auto whitespace-pre-wrap break-all rounded bg-white p-2 font-mono text-xs dark:bg-zinc-900">
                  {formatDebugPre(trackDebug)}
                </pre>
                <button
                  type="button"
                  onClick={() => copyDebugPayload(trackDebug)}
                  className="mt-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                >
                  {copyFeedback ? 'Copied!' : 'Copy for tech support'}
                </button>
              </>
            )}
          </div>
        )}
          </>
        )}
      </section>

      <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">
          Step 4: Fetch GPS steps
        </h2>
        <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
          For jobs on the selected date where <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">steps_fetched = false</code>, derive step 2–4 from tbl_tracking and write to tbl_vworkjobs. Each job is then marked <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">steps_fetched = true</code>, <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">steps_fetched_when = now()</code>. Use &quot;Reset Steps&quot; to clear the flag so jobs can be re-run.
        </p>
        <div className="mb-3 flex flex-wrap items-center gap-4">
          <label className="flex items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            Date
            <input
              type="date"
              value={trackDate}
              onChange={(e) => setTrackDate(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            Start −
            <input
              type="number"
              min={0}
              max={120}
              value={step4StartLessMinutes}
              onChange={(e) => setStep4StartLessMinutes(Math.max(0, Math.min(120, parseInt(e.target.value, 10) || 0)))}
              className="w-14 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
            min
          </label>
          <label className="flex items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            End +
            <input
              type="number"
              min={0}
              max={120}
              value={step4EndPlusMinutes}
              onChange={(e) => setStep4EndPlusMinutes(Math.max(0, Math.min(120, parseInt(e.target.value, 10) || 0)))}
              className="w-14 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
            min
          </label>
          <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
            <input
              type="checkbox"
              checked={step4Force}
              onChange={(e) => setStep4Force(e.target.checked)}
              className="rounded"
            />
            Force (include jobs already with steps_fetched=true)
          </label>
          <button
            type="button"
            onClick={() => void runStep4FetchGpsSteps()}
            disabled={step4Running}
            className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            title={step4Force ? 'Fetch GPS steps for all jobs on this date' : 'Fetch GPS steps for jobs on this date where steps_fetched = false'}
          >
            {step4Running ? `Fetching… ${step4Progress?.current ?? 0}/${step4Progress?.total ?? 0}` : 'Fetch GPS steps for date'}
          </button>
          <button
            type="button"
            onClick={() => void resetStepsAll()}
            disabled={resetStepsLoading}
            className="rounded border border-amber-500 bg-white px-4 py-2 text-sm font-medium text-amber-700 hover:bg-amber-50 disabled:opacity-50 dark:border-amber-600 dark:bg-zinc-800 dark:text-amber-400 dark:hover:bg-zinc-700"
            title="Set steps_fetched = false for all rows in tbl_vworkjobs (re-open for next run)"
          >
            {resetStepsLoading ? 'Resetting…' : 'Reset Steps'}
          </button>
        </div>
        {step4Error && (
          <p className="mb-2 text-sm text-red-600 dark:text-red-400">{step4Error}</p>
        )}
        {resetStepsMessage && (
          <p className="mb-2 text-sm text-zinc-600 dark:text-zinc-400">{resetStepsMessage}</p>
        )}
        {step4Debug != null && (
          <div className="mb-2 rounded border border-zinc-200 bg-zinc-50 px-3 py-2 font-mono text-xs dark:border-zinc-600 dark:bg-zinc-800/50">
            <div className="font-semibold text-zinc-700 dark:text-zinc-300">Debug (job list)</div>
            <div>Fetched <strong>{step4Debug.jobCount}</strong> job(s) for date <strong>{step4Debug.date ?? trackDate}</strong>.</div>
            {step4Force ? (
              <div className="text-zinc-600 dark:text-zinc-400">Force: all jobs for date (no steps_fetched filter).</div>
            ) : step4Debug.stepsFetchedFilter != null ? (
              <div>
                Filter <code>stepsFetched={step4Debug.stepsFetchedFilter}</code>:
                {step4Debug.stepsFetchedFilterSkipped ? (
                  <span className="text-amber-600 dark:text-amber-400"> skipped (column missing in DB — run add_steps_fetched_tbl_vworkjobs.sql)</span>
                ) : step4Debug.stepsFetchedFilterApplied ? (
                  <span className="text-green-600 dark:text-green-400"> applied</span>
                ) : (
                  <span> not applied</span>
                )}
              </div>
            ) : null}
            {step4Debug.whereHint && (
              <div className="mt-1 truncate text-zinc-500 dark:text-zinc-400" title={step4Debug.whereHint}>WHERE: {step4Debug.whereHint}</div>
            )}
            {step4Debug.jobCount === 0 && (
              <p className="mt-2 text-amber-700 dark:text-amber-400">No jobs to process. Use <strong>Reset Steps</strong> to set steps_fetched=false for all, or enable <strong>Force</strong> to include jobs already fetched.</p>
            )}
          </div>
        )}
        {step4Running && step4Progress && (
          <p className="mb-2 text-sm text-zinc-500 dark:text-zinc-400">
            Running: {step4Progress.current} / {step4Progress.total} jobs (window: start −{step4StartLessMinutes} min, end +{step4EndPlusMinutes} min).
          </p>
        )}
        {!step4Running && step4Log.length > 0 && (
          <div className="max-h-40 overflow-auto rounded border border-zinc-200 bg-zinc-50 px-2 py-1.5 font-mono text-xs dark:border-zinc-600 dark:bg-zinc-800/50">
            <div className="font-medium text-zinc-600 dark:text-zinc-400">Last run: {step4Log.length} jobs</div>
            {step4Log.slice(-20).map((entry, idx) => (
              <div key={idx} className={entry.status === 'error' ? 'text-red-600 dark:text-red-400' : entry.status === 'skip' ? 'text-amber-600 dark:text-amber-400' : 'text-zinc-700 dark:text-zinc-300'}>
                {entry.job_id} — {entry.status}{entry.message ? `: ${entry.message}` : ''}
              </div>
            ))}
          </div>
        )}
      </section>
      </>
      )}

      {apiTestTab === 'fences' && (
      <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">
          Import/merge Fences (Tracksolid platform)
        </h2>
        <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
          Two stages: (1) Fetch fence list from the API and review; (2) Process and merge into <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">tbl_geofences</code> — each fence is inserted or updated and shown in the log. Endpoint: <strong>{endpoint}</strong>.
        </p>

        <div className="mb-4 flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">Stage 1</span>
          <button
            type="button"
            onClick={async () => {
              setFenceListFetchError(null);
              setFenceListFetched(null);
              setFenceListFetchDebug(null);
              setFenceListFetchLoading(true);
              try {
                const res = await fetch('/api/admin/tracksolid/fences/fetch', {
                  method: 'POST',
                  headers: {
                    'Content-Type': 'application/json',
                    'X-Tracksolid-Endpoint': endpoint,
                  },
                  body: JSON.stringify({ endpoint }),
                });
                const data = await res.json();
                const requestDebug = data?.requestDebug ?? data?.debug?.requestDebug;
                const debugPayload = requestDebug || data?.debug ? {
                  requestDebug: requestDebug ?? data?.debug?.requestDebug,
                  requestParamsRedacted: data?.debug?.requestParamsRedacted,
                  endpoint: data?.debug?.endpoint,
                  method: data?.debug?.method,
                  httpStatus: data?.debug?.httpStatus,
                  responseBody: data?.debug?.responseBody,
                  fullResponse: data?.debug?.fullResponse,
                } : null;
                if (!res.ok) {
                  setFenceListFetchError(data?.error ?? res.statusText ?? 'Fetch failed');
                  setFenceListFetchDebug(debugPayload ?? (data?.requestDebug ? { requestDebug: data.requestDebug } : null));
                  return;
                }
                setFenceListFetched(data.fences ?? []);
                setFenceListFetchDebug(debugPayload ?? (data?.requestDebug ? { requestDebug: data.requestDebug } : null));
              } catch (e) {
                setFenceListFetchError(e instanceof Error ? e.message : String(e));
              } finally {
                setFenceListFetchLoading(false);
              }
            }}
            disabled={fenceListFetchLoading}
            className="rounded bg-zinc-600 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
          >
            {fenceListFetchLoading ? 'Fetching…' : '1. Fetch fence list'}
          </button>
        </div>
        {fenceListFetchError && (
          <div className="mb-3">
            <p className="text-sm text-red-600 dark:text-red-400">{fenceListFetchError}</p>
            {fenceListFetchDebug && (
              <details className="mt-2 rounded border border-zinc-200 bg-zinc-50 p-2 dark:border-zinc-700 dark:bg-zinc-800/50">
                <summary className="cursor-pointer text-xs font-medium text-zinc-600 dark:text-zinc-400">Request / response debug</summary>
                <pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-words font-mono text-xs text-zinc-700 dark:text-zinc-300">
                  {[
                    fenceListFetchDebug.requestDebug && [
                      '——— Endpoint resolution ———',
                      `Source: ${fenceListFetchDebug.requestDebug.endpointSource ?? 'none'}`,
                      `Value: ${fenceListFetchDebug.requestDebug.endpointValue ?? 'none'}`,
                      `Resolved base URL: ${fenceListFetchDebug.requestDebug.resolvedBaseUrl ?? '—'}`,
                      fenceListFetchDebug.requestDebug.note ? `Note: ${fenceListFetchDebug.requestDebug.note}` : null,
                    ].filter(Boolean).join('\n'),
                    fenceListFetchDebug.requestParamsRedacted && [
                      '',
                      '——— Params sent (redacted) ———',
                      JSON.stringify(fenceListFetchDebug.requestParamsRedacted, null, 2),
                    ].join('\n'),
                    fenceListFetchDebug.responseBody != null && [
                      '',
                      '——— Response body ———',
                      fenceListFetchDebug.responseBody,
                    ].join('\n'),
                    fenceListFetchDebug.fullResponse != null && [
                      '',
                      '——— Full response (parsed) ———',
                      JSON.stringify(fenceListFetchDebug.fullResponse, null, 2),
                    ].join('\n'),
                  ].filter(Boolean).join('\n') || 'No debug data'}
                </pre>
              </details>
            )}
          </div>
        )}
        {fenceListFetchDebug && !fenceListFetchError && (
          <details className="mb-3 rounded border border-zinc-200 bg-zinc-50 p-2 dark:border-zinc-700 dark:bg-zinc-800/50">
            <summary className="cursor-pointer text-xs font-medium text-zinc-600 dark:text-zinc-400">Request debug (endpoint used)</summary>
            <pre className="mt-2 font-mono text-xs text-zinc-700 dark:text-zinc-300">
              {fenceListFetchDebug.requestDebug
                ? `Source: ${fenceListFetchDebug.requestDebug.endpointSource ?? 'none'}\nValue: ${fenceListFetchDebug.requestDebug.endpointValue ?? 'none'}\nResolved: ${fenceListFetchDebug.requestDebug.resolvedBaseUrl ?? '—'}`
                : '—'}
            </pre>
          </details>
        )}
        {fenceListFetched && (
          <div className="mb-4 rounded border border-zinc-200 bg-zinc-50 p-3 dark:border-zinc-700 dark:bg-zinc-800/50">
            <p className="mb-2 text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">
              Fence list ({fenceListFetched.length} fence{fenceListFetched.length !== 1 ? 's' : ''})
            </p>
            <div className="max-h-48 overflow-y-auto font-mono text-sm text-zinc-700 dark:text-zinc-300">
              {fenceListFetched.map((f, i) => (
                <div key={f.fenceId ?? i}>{f.name ?? f.fenceId ?? '—'}</div>
              ))}
            </div>
          </div>
        )}

        <div className="mb-4 flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">Stage 2</span>
          <button
            type="button"
            onClick={async () => {
              setFenceSyncError(null);
              setFenceSyncResult(null);
              setFenceSyncLog([]);
              setFenceSyncLoading(true);
              try {
                const res = await fetch('/api/admin/tracksolid/fences/sync', {
                  method: 'POST',
                  headers: {
                    'Content-Type': 'application/json',
                    'X-Tracksolid-Endpoint': endpoint,
                  },
                  body: JSON.stringify({ endpoint }),
                });
                if (!res.ok || !res.body) {
                  setFenceSyncError(res.statusText || 'Sync failed');
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
                      const e = JSON.parse(line) as { stage: string; message?: string; current?: number; total?: number; fetched?: number; inserted?: number; updated?: number; deleted?: number; error?: string; action?: 'inserted' | 'updated'; fence_name?: string };
                      setFenceSyncLog((prev) => [...prev, e]);
                      if (e.stage === 'done' && e.fetched != null) {
                        setFenceSyncResult({
                          fetched: e.fetched,
                          inserted: e.inserted ?? 0,
                          updated: e.updated ?? 0,
                          deleted: e.deleted ?? 0,
                        });
                      }
                      if (e.stage === 'error') {
                        setFenceSyncError(e.error ?? e.message ?? 'Sync failed');
                      }
                    } catch {
                      /* ignore parse */
                    }
                  }
                }
                if (buffer.trim()) {
                  try {
                    const e = JSON.parse(buffer) as { stage: string; message?: string; fetched?: number; inserted?: number; updated?: number; deleted?: number; error?: string };
                    setFenceSyncLog((prev) => [...prev, e]);
                    if (e.stage === 'done' && e.fetched != null) {
                      setFenceSyncResult({
                        fetched: e.fetched,
                        inserted: e.inserted ?? 0,
                        updated: e.updated ?? 0,
                        deleted: e.deleted ?? 0,
                      });
                    }
                    if (e.stage === 'error') setFenceSyncError(e.error ?? e.message ?? 'Sync failed');
                  } catch {
                    /* ignore */
                  }
                }
              } catch (e) {
                setFenceSyncError(e instanceof Error ? e.message : String(e));
              } finally {
                setFenceSyncLoading(false);
              }
            }}
            disabled={fenceSyncLoading}
            className="rounded bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50 dark:bg-indigo-700 dark:hover:bg-indigo-600"
          >
            {fenceSyncLoading ? 'Processing…' : '2. Process (insert/update)'}
          </button>
        </div>
        {fenceSyncError && (
          <p className="mt-3 text-sm text-red-600 dark:text-red-400">{fenceSyncError}</p>
        )}
        {fenceSyncLog.length > 0 && (
          <div className="mt-3 rounded border border-zinc-200 bg-zinc-50 p-3 dark:border-zinc-700 dark:bg-zinc-800/50">
            <p className="mb-2 text-xs font-medium uppercase text-zinc-500 dark:text-zinc-400">
              Progress (live)
            </p>
            <div
              ref={fenceSyncLogScrollRef}
              className="max-h-48 overflow-y-auto font-mono text-sm"
              style={{ scrollBehavior: 'smooth' }}
            >
              {fenceSyncLog.map((e, i) => (
                <div
                  key={i}
                  className={
                    e.stage === 'error' ? 'text-red-600 dark:text-red-400' :
                    e.stage === 'done' ? 'font-semibold text-emerald-700 dark:text-emerald-400' :
                    e.stage === 'debug_result_keys' ? 'text-amber-700 dark:text-amber-400' :
                    e.stage === 'fence' && e.action === 'inserted' ? 'text-emerald-600 dark:text-emerald-400' :
                    e.stage === 'fence' && e.action === 'updated' ? 'text-blue-600 dark:text-blue-400' :
                    'text-zinc-700 dark:text-zinc-300'
                  }
                >
                  {e.stage === 'fence' ? (e.message ?? `${e.action}: ${e.fence_name ?? '—'}`) : (e.message ?? e.stage)}
                </div>
              ))}
            </div>
          </div>
        )}
        {fenceSyncResult && (
          <div className="mt-3 rounded border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-800 dark:bg-emerald-900/20">
            <p className="text-sm font-medium text-emerald-800 dark:text-emerald-200">Summary</p>
            <p className="mt-1 text-sm text-emerald-700 dark:text-emerald-300">
              Fetched <strong>{fenceSyncResult.fetched}</strong> · inserted <strong>{fenceSyncResult.inserted}</strong> · updated <strong>{fenceSyncResult.updated}</strong> · deleted <strong>{fenceSyncResult.deleted}</strong>
            </p>
          </div>
        )}
      </section>
      )}
    </div>
  );
}
