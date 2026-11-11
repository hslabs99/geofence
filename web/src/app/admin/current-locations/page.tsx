'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import GateSpeechPanel from '@/components/admin/GateSpeechPanel';
import { VworkApiPanel } from '@/components/admin/VworkApiPanel';
import { VworkFeedPanel } from '@/components/admin/VworkFeedPanel';
import { VworkGateUpdatesPanel } from '@/components/admin/VworkGateUpdatesPanel';
import { tracksolidDayCallCost } from '@/lib/tracksolid-call-math';

type Row = {
  id: number;
  truck_id: string | null;
  imei: string | null;
  location: string | null;
  gpstime_nz: string | null;
  gpstime: string | null;
  polltime: string | null;
  coordinates: string | null;
  fencename: string | null;
  softfencename: string | null;
};

type LocationColumn = 'truck_id' | 'imei' | 'location' | 'gpstime_nz' | 'gpstime' | 'polltime' | 'coordinates' | 'fencename' | 'softfencename';

const LOCATION_COLUMNS: { key: LocationColumn; label: string; mono?: boolean }[] = [
  { key: 'truck_id', label: 'Truck' },
  { key: 'imei', label: 'IMEI', mono: true },
  { key: 'location', label: 'Location' },
  { key: 'gpstime_nz', label: 'NZ GPS time', mono: true },
  { key: 'gpstime', label: 'GPS time', mono: true },
  { key: 'polltime', label: 'Poll time', mono: true },
  { key: 'coordinates', label: 'Coordinates', mono: true },
  { key: 'fencename', label: 'Fence' },
  { key: 'softfencename', label: 'Soft fence' },
];

function compareLocationRows(a: Row, b: Row, key: LocationColumn, dir: 'asc' | 'desc'): number {
  const av = (a[key] ?? '').trim();
  const bv = (b[key] ?? '').trim();
  if (!av && !bv) return b.id - a.id;
  if (!av) return 1;
  if (!bv) return -1;
  const cmp = av.localeCompare(bv, undefined, { numeric: true, sensitivity: 'base' });
  if (cmp === 0) return b.id - a.id;
  return dir === 'asc' ? cmp : -cmp;
}

type LogRow = {
  logid: number;
  logdatetime: string | null;
  logcat2: string | null;
  logdetails: string | null;
};

type Settings = {
  pollIntervalSeconds: number;
  flushOlderThanHours: number;
  flushEveryMinutes: number;
  driverAppPollSeconds: number;
  confirmAwayMeters: number;
};

function pollSummary(log: LogRow): { polltime: string; fetched: string; written: string; trucks: string } | null {
  if (log.logcat2 !== 'ok' && log.logcat2 !== 'empty' && log.logcat2 !== 'error') return null;
  let details: Record<string, unknown> = {};
  try {
    details = JSON.parse(log.logdetails ?? '{}') as Record<string, unknown>;
  } catch {
    details = {};
  }
  const polltime = typeof details.polltime === 'string' && details.polltime ? details.polltime : log.logdatetime || '—';
  if (log.logcat2 === 'error') {
    const message = typeof details.error === 'string' ? details.error : 'Poll failed';
    return { polltime, fetched: '—', written: '—', trucks: message };
  }
  const fetched = details.fetched ?? details.deviceCount;
  const written = details.written ?? details.inserted;
  const trucks = Array.isArray(details.trucks)
    ? details.trucks
        .map((truck) => (truck && typeof truck === 'object' && 'truck_id' in truck ? String((truck as { truck_id?: unknown }).truck_id ?? '') : ''))
        .filter(Boolean)
        .join(', ')
    : '';
  return {
    polltime,
    fetched: fetched == null ? '—' : String(fetched),
    written: written == null ? '—' : String(written),
    trucks: trucks || '—',
  };
}

type Alerts = {
  alertEmails: string;
  checkerIntervalSeconds: number;
  checkerRunning: boolean;
  sendGridConfigured: boolean;
  lastCheckAt: string | null;
  lastResult: string | null;
  lastEmailAt: string | null;
  lastEmailError: string | null;
};

type TokenInfo = {
  endpoint: string;
  token: string | null;
  valid: boolean;
  requestedAt: string | null;
  expiresAt: string | null;
  expiresAtMs: number | null;
  secondsRemaining: number | null;
  requestCount: number;
  lastUse: 'reused' | 'requested' | null;
  lastUseAt: string | null;
  lastError: string | null;
};

type CallBudget = {
  limit: number;
  used: number;
  forecast: number;
  fullDayAtInterval: number;
  pollIntervalSeconds: number;
  pollRunning: boolean;
  callsPerPoll: number;
  windowStartedAt: string | null;
  resetsAt: string | null;
  resetsAtMs: number | null;
  byMethod: { method: string; count: number }[];
};

type Status = {
  running: boolean;
  intervalMs: number;
  settings?: Settings;
  alerts?: Alerts;
  token?: TokenInfo;
  calls?: CallBudget;
  nextPollAtMs?: number | null;
  serverNowMs?: number;
  receivedAt?: number;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  lastFlushAt?: string | null;
  lastFlushDeleted?: number | null;
  email?: { ok: boolean; error?: string };
  rows: Row[];
  logs: LogRow[];
  error?: string;
};

function stamp(data: Status): Status {
  return { ...data, receivedAt: Date.now() };
}

function estimatedServerNow(status: Status | null): number {
  if (status?.serverNowMs == null || status.receivedAt == null) return Date.now();
  return status.serverNowMs + (Date.now() - status.receivedAt);
}

function pollCountdown(status: Status | null): number | null {
  if (!status?.running || status.nextPollAtMs == null) return null;
  return Math.max(0, Math.ceil((status.nextPollAtMs - estimatedServerNow(status)) / 1000));
}

function tokenCountdown(status: Status | null): number | null {
  if (status?.token?.expiresAtMs == null) return null;
  return Math.max(0, Math.ceil((status.token.expiresAtMs - estimatedServerNow(status)) / 1000));
}

function formatDuration(total: number): string {
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function shareOfLimit(count: number, limit: number): string {
  if (limit <= 0) return '0%';
  const pct = (count / limit) * 100;
  const rounded = pct >= 10 ? Math.round(pct) : Math.round(pct * 10) / 10;
  return `${rounded}%`;
}

function quotaShare(count: number, limit: number, overLimit: boolean): string {
  const share = shareOfLimit(count, limit);
  if (!overLimit || limit <= 0) return share;
  const pct = (count / limit) * 100;
  if (Math.round(pct) <= 100) return `${Math.round(pct * 10) / 10}%`;
  return share;
}

function pollDayLine(rawSeconds: string): { text: string; overLimit: boolean } | null {
  const seconds = Number(rawSeconds);
  const cost = tracksolidDayCallCost(seconds);
  if (!cost) return null;
  const polls = cost.polls.toLocaleString();
  const calls = cost.calls.toLocaleString();
  const limit = cost.limit.toLocaleString();
  const share = quotaShare(cost.calls, cost.limit, cost.overLimit);
  const rule = `${polls} polls a day · ${calls} TrackSolid calls · ${share} of the ${limit} daily limit (${cost.callsPerPoll} per poll, plus ${cost.tokenCalls} token calls). This interval ${cost.overLimit ? 'goes past it' : 'stays inside it'}.`;
  return { text: rule, overLimit: cost.overLimit };
}

type ServiceTab = 'services' | 'rows' | 'summary' | 'log' | 'vwork-api' | 'gate-updates' | 'vwork' | 'gate-speech';

const SERVICE_GROUPS: { id: 'services' | 'tracksolid' | 'vwork' | 'gate-speech'; label: string; tabs: readonly [ServiceTab, string][] }[] = [
  {
    id: 'services',
    label: 'Services',
    tabs: [],
  },
  {
    id: 'tracksolid',
    label: 'Track Solid API',
    tabs: [
      ['rows', 'Location data'],
      ['summary', 'Poll summary'],
      ['log', 'Poll log'],
    ],
  },
  {
    id: 'vwork',
    label: 'VWork API',
    tabs: [
      ['vwork-api', 'VWork API'],
      ['gate-updates', 'Gate updates'],
      ['vwork', 'VWork feed'],
    ],
  },
  {
    id: 'gate-speech',
    label: 'Gate Speech',
    tabs: [],
  },
];

function groupTab(group: (typeof SERVICE_GROUPS)[number]): ServiceTab {
  return group.tabs[0]?.[0] ?? (group.id === 'gate-speech' ? 'gate-speech' : 'services');
}

export default function CurrentLocationsAdminPage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [pollSeconds, setPollSeconds] = useState('30');
  const [driverPollSeconds, setDriverPollSeconds] = useState('30');
  const [confirmAwayMeters, setConfirmAwayMeters] = useState('100');
  const [flushHours, setFlushHours] = useState('72');
  const [flushEvery, setFlushEvery] = useState('60');
  const [dirty, setDirty] = useState(false);
  const [alertDirty, setAlertDirty] = useState(false);
  const [alertEmails, setAlertEmails] = useState('');
  const [checkerSeconds, setCheckerSeconds] = useState('60');
  const [emailNotice, setEmailNotice] = useState<string | null>(null);
  const [tab, setTab] = useState<ServiceTab>('services');
  const [vworkRunning, setVworkRunning] = useState<boolean | null>(null);
  const [gateRunning, setGateRunning] = useState<boolean | null>(null);
  const [confirmEmpty, setConfirmEmpty] = useState(false);
  const [tick, setTick] = useState(0);
  const [locationSort, setLocationSort] = useState<LocationColumn>('polltime');
  const [locationSortDir, setLocationSortDir] = useState<'asc' | 'desc'>('desc');
  const [locationFilters, setLocationFilters] = useState<Partial<Record<LocationColumn, string>>>({});

  const load = useCallback(() => {
    fetch('/api/admin/current-locations', { cache: 'no-store' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        return data as Status;
      })
      .then((data) => {
        setStatus(stamp(data));
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, [load]);

  useEffect(() => {
    if (tab !== 'services') return;
    const id = setInterval(() => setTick((n) => n + 1), 1000);
    return () => clearInterval(id);
  }, [tab]);

  useEffect(() => {
    if (dirty || !status?.settings) return;
    setPollSeconds(String(status.settings.pollIntervalSeconds));
    setDriverPollSeconds(String(status.settings.driverAppPollSeconds ?? 30));
    setConfirmAwayMeters(String(status.settings.confirmAwayMeters ?? 100));
    setFlushHours(String(status.settings.flushOlderThanHours));
    setFlushEvery(String(status.settings.flushEveryMinutes));
  }, [dirty, status]);

  useEffect(() => {
    if (alertDirty || !status?.alerts) return;
    setAlertEmails(status.alerts.alertEmails);
    setCheckerSeconds(String(status.alerts.checkerIntervalSeconds));
  }, [alertDirty, status]);

  function settingsBody() {
    return {
      pollIntervalSeconds: Number(pollSeconds),
      driverAppPollSeconds: Number(driverPollSeconds),
      confirmAwayMeters: Number(confirmAwayMeters),
      flushOlderThanHours: Number(flushHours),
      flushEveryMinutes: Number(flushEvery),
    };
  }

  async function act(action: 'start' | 'stop' | 'once' | 'settings' | 'empty') {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/admin/current-locations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, ...settingsBody() }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setDirty(false);
      const next = stamp(data as Status);
      setStatus(next);
      if (next.email && next.email.ok === false) setEmailNotice(next.email.error ?? 'Alert email was not sent');
      else if (action === 'start' || action === 'stop') setEmailNotice(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function actAlerts(action: 'alert-settings' | 'checker-start' | 'checker-stop') {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/admin/current-locations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action,
          alertEmails,
          checkerIntervalSeconds: Number(checkerSeconds),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setAlertDirty(false);
      setStatus(stamp(data as Status));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const locationRows = useMemo(() => {
    const source = status?.rows ?? [];
    const filtered = source.filter((row) =>
      LOCATION_COLUMNS.every((column) => {
        const needle = (locationFilters[column.key] ?? '').trim().toLowerCase();
        if (!needle) return true;
        return (row[column.key] ?? '').toLowerCase().includes(needle);
      }),
    );
    return [...filtered].sort((a, b) => compareLocationRows(a, b, locationSort, locationSortDir));
  }, [status?.rows, locationFilters, locationSort, locationSortDir]);

  function toggleLocationSort(key: LocationColumn) {
    if (locationSort === key) {
      setLocationSortDir((dir) => (dir === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setLocationSort(key);
    setLocationSortDir('asc');
  }

  const pollDay = pollDayLine(pollSeconds);

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Service management</h1>
      <p className="mt-1 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
        Services is the list of what runs on this server. Each one says what it is for. Closing the browser leaves a running service going. Stopping the server stops it.
      </p>

      {error && (
        <div className="mt-4 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}
      {emailNotice && (
        <div className="mt-4 rounded bg-amber-100 p-3 text-sm text-amber-900 dark:bg-amber-900/30 dark:text-amber-100">{emailNotice}</div>
      )}

      <div className="mt-6">
        <div className="inline-flex rounded-lg border border-zinc-200 bg-zinc-100 p-1 dark:border-zinc-700 dark:bg-zinc-800">
          {SERVICE_GROUPS.map((group) => {
            const active = group.tabs.length === 0 ? tab === group.id : group.tabs.some(([id]) => id === tab);
            return (
              <button
                key={group.id}
                type="button"
                onClick={() => {
                  if (active) return;
                  setTab(groupTab(group));
                }}
                className={`rounded-md px-3 py-1.5 text-sm font-semibold ${
                  active
                    ? 'bg-white text-zinc-900 shadow-sm dark:bg-zinc-900 dark:text-zinc-100'
                    : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-zinc-100'
                }`}
              >
                {group.label}
              </button>
            );
          })}
        </div>
        {SERVICE_GROUPS.some((group) => group.tabs.some(([id]) => id === tab)) && (
          <div className="mt-3 flex gap-2 border-b border-zinc-200 dark:border-zinc-700">
            {SERVICE_GROUPS.find((group) => group.tabs.some(([id]) => id === tab))!.tabs.map(([id, label]) => (
              <button
                key={id}
                type="button"
                onClick={() => setTab(id)}
                className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium ${
                  tab === id
                    ? 'border-zinc-900 text-zinc-900 dark:border-zinc-100 dark:text-zinc-100'
                    : 'border-transparent text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        )}
      </div>

      {tab === 'services' && (
      <>
      <nav aria-label="Service status" className="mt-6 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        {(
          [
            ['service-track-solid', 'Track Solid locations', status == null ? null : status.running, status?.running ? 'Running' : 'Paused'],
            ['service-location-alerts', 'Location alerts', status?.alerts == null ? null : status.alerts.checkerRunning, status?.alerts?.checkerRunning ? 'Running' : 'Stopped'],
            ['service-vwork-jobs', 'VWork jobs', vworkRunning, vworkRunning ? 'Running' : 'Stopped'],
            ['service-gate-updates', 'Gate updates', gateRunning, gateRunning ? 'Running' : 'Stopped'],
          ] as const
        ).map(([id, label, running, stateLabel]) => (
          <a
            key={id}
            href={`#${id}`}
            onClick={(event) => {
              const target = document.getElementById(id);
              if (!target) return;
              event.preventDefault();
              target.scrollIntoView({ block: 'start' });
              history.replaceState(null, '', `#${id}`);
            }}
            className="flex items-center justify-between gap-3 rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900 dark:hover:bg-zinc-800"
          >
            <span className="font-medium text-zinc-900 underline decoration-zinc-300 underline-offset-2 dark:text-zinc-100 dark:decoration-zinc-600">{label}</span>
            <span className={running == null ? 'text-zinc-400' : running ? 'font-medium text-emerald-700 dark:text-emerald-400' : 'font-medium text-amber-700 dark:text-amber-400'}>
              {running == null ? '…' : stateLabel}
            </span>
          </a>
        ))}
      </nav>
      <section id="service-track-solid" className="mt-8 scroll-mt-6">
        <h2 className="text-lg font-semibold text-zinc-900 dark:text-zinc-100">Track Solid locations</h2>
        <p className="mt-1 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          Polls every truck on the account in one TrackSolid location call. A row is written only when that truck’s GPS time is newer than the one already stored. Save applies the poll seconds straight away while the service is running. A flush removes rows older than the hour limit, and runs on its own minute interval. Location data, the poll summary, and the poll log are under Track Solid API.
        </p>
      <div className="mt-4 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <button
          type="button"
          disabled={busy || status?.running}
          onClick={() => void act('start')}
          className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Start
        </button>
        <div className="text-sm text-zinc-700 dark:text-zinc-200">
          <label>
            Poll every
            <input
              type="number"
              min={5}
              max={3600}
              step={1}
              value={pollSeconds}
              onChange={(e) => {
                setDirty(true);
                setPollSeconds(e.target.value);
              }}
              className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
            seconds
          </label>
          {pollDay && (
            <p className={`mt-1 max-w-xl text-xs ${pollDay.overLimit ? 'font-medium text-amber-700 dark:text-amber-400' : 'text-zinc-500 dark:text-zinc-400'}`}>
              {pollDay.text}
            </p>
          )}
        </div>
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('settings')}
          className="rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          Save
        </button>
        <button
          type="button"
          disabled={busy || !status?.running}
          onClick={() => void act('stop')}
          className="rounded bg-zinc-700 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Stop
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('once')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
        >
          Poll once
        </button>
        <div className="pb-1">
          <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
            <span className={status?.running ? 'text-emerald-700 dark:text-emerald-400' : 'text-amber-700 dark:text-amber-400'}>
              {status?.running ? 'Running' : 'Paused'}
            </span>
            {status?.running && (
              <span className="font-normal text-zinc-600 dark:text-zinc-300">
                {' '}
                · {(() => {
                  void tick;
                  const left = pollCountdown(status);
                  if (left == null) return 'next poll time pending';
                  if (left === 0) return 'polling now';
                  return `${left} seconds to the next poll`;
                })()}
              </span>
            )}
          </p>
          {status?.lastPolltime && (
            <p className="text-xs text-zinc-500">Last poll {status.lastPolltime}</p>
          )}
        </div>
        <button
          type="button"
          disabled={busy}
          onClick={() => setConfirmEmpty(true)}
          className="ml-auto rounded border border-red-300 px-3 py-1.5 text-sm font-medium text-red-700 hover:bg-red-50 disabled:opacity-50 dark:border-red-800 dark:text-red-300 dark:hover:bg-red-950/40"
        >
          Empty table
        </button>
      </div>

      <div className="mt-3 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <div className="w-full">
          <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">Driver app</p>
          <p className="mt-0.5 max-w-3xl text-xs text-zinc-500">
            The phone reads the latest stored location this often. A parked truck keeps the same GPS time, so that time can be much older than the fetch.
            An arrival click farther than the metre limit asks the driver to confirm. Leaving the vineyard asks when the truck is still inside that fence.
          </p>
        </div>
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Check location every
          <input
            type="number"
            min={5}
            max={3600}
            step={1}
            value={driverPollSeconds}
            onChange={(e) => {
              setDirty(true);
              setDriverPollSeconds(e.target.value);
            }}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          seconds
        </label>
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Confirm arrival beyond
          <input
            type="number"
            min={1}
            max={100000}
            step={1}
            value={confirmAwayMeters}
            onChange={(e) => {
              setDirty(true);
              setConfirmAwayMeters(e.target.value);
            }}
            className="mx-2 w-24 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          metres
        </label>
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('settings')}
          className="rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          Save
        </button>
      </div>

      <div className="mt-3 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Flush rows older than
          <input
            type="number"
            min={1}
            max={8760}
            step={1}
            value={flushHours}
            onChange={(e) => {
              setDirty(true);
              setFlushHours(e.target.value);
            }}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          hours
        </label>
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Flush every
          <input
            type="number"
            min={1}
            max={1440}
            step={1}
            value={flushEvery}
            onChange={(e) => {
              setDirty(true);
              setFlushEvery(e.target.value);
            }}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          minutes
        </label>
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('settings')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
        >
          Save
        </button>
        <p className="pb-1 text-sm text-zinc-600 dark:text-zinc-300">
          {status?.lastFlushAt
            ? `Last flush ${status.lastFlushAt} · deleted ${status.lastFlushDeleted ?? 0}`
            : 'Flush runs while the service is started'}
        </p>
      </div>

      {status?.lastSummary && <p className="mt-3 text-sm text-zinc-700 dark:text-zinc-300">{status.lastSummary}</p>}
      {status?.lastError && <p className="mt-2 text-sm text-red-700 dark:text-red-300">{status.lastError}</p>}

      <div className="mt-4 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Token</h2>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
          The poll reuses one TrackSolid token for about 2 hours. A new token is requested only when this one is inside 2 minutes of expiry, or when TrackSolid rejects it. That refresh happens once, then the poll continues.
        </p>
        {status?.token?.token ? (
          <dl className="mt-3 grid gap-2 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-zinc-500">Status</dt>
              <dd className={status.token.valid ? 'text-emerald-700 dark:text-emerald-400' : 'text-amber-700 dark:text-amber-400'}>
                {status.token.valid ? 'Valid' : 'Due for refresh'}
                {(() => {
                  void tick;
                  const left = tokenCountdown(status);
                  return left == null ? '' : ` · ${formatDuration(left)} remaining`;
                })()}
              </dd>
            </div>
            <div>
              <dt className="text-zinc-500">Requested</dt>
              <dd className="font-mono">{status.token.requestedAt || 'Not recorded'}</dd>
            </div>
            <div>
              <dt className="text-zinc-500">Expires</dt>
              <dd className="font-mono">{status.token.expiresAt || '—'}</dd>
            </div>
            <div>
              <dt className="text-zinc-500">Token requests</dt>
              <dd>{status.token.requestCount}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-zinc-500">Last use</dt>
              <dd>
                {status.token.lastUse === 'reused' && `Reused the stored token${status.token.lastUseAt ? ` at ${status.token.lastUseAt}` : ''}`}
                {status.token.lastUse === 'requested' && `Requested a new token${status.token.lastUseAt ? ` at ${status.token.lastUseAt}` : ''}`}
                {status.token.lastUse == null && 'Not used in this server process yet'}
              </dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-zinc-500">Current token</dt>
              <dd className="mt-1 break-all font-mono text-xs text-zinc-800 dark:text-zinc-200">{status.token.token}</dd>
            </div>
          </dl>
        ) : (
          <p className="mt-3 text-sm text-zinc-600 dark:text-zinc-300">No token stored yet. The next poll requests one, then later polls reuse it.</p>
        )}
        {status?.token?.lastError && <p className="mt-2 text-sm text-red-700 dark:text-red-300">{status.token.lastError}</p>}
      </div>

      {status?.calls && (
      <div className="mt-4 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">API consumption</h2>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
          The Hong Kong node allows {status.calls.limit.toLocaleString()} calls in each 24-hour window. The window starts at the first call and resets 24 hours later. Each poll is {status.calls.callsPerPoll} calls, and a token call is added about every 2 hours.
        </p>
        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          <div>
            <p className="text-xs uppercase tracking-wide text-zinc-500">Consumed this window</p>
            <p className="mt-1 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">{status.calls.used.toLocaleString()}</p>
            <p className="text-sm text-zinc-600 dark:text-zinc-300">{shareOfLimit(status.calls.used, status.calls.limit)} of {status.calls.limit.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-xs uppercase tracking-wide text-zinc-500">24-hour forecast</p>
            <p className={`mt-1 text-2xl font-semibold ${status.calls.forecast > status.calls.limit ? 'text-amber-700 dark:text-amber-400' : 'text-zinc-900 dark:text-zinc-100'}`}>
              {status.calls.forecast.toLocaleString()}
            </p>
            <p className="text-sm text-zinc-600 dark:text-zinc-300">
              {shareOfLimit(status.calls.forecast, status.calls.limit)} of {status.calls.limit.toLocaleString()}
              {status.calls.forecast > status.calls.limit
                ? ` · ${(status.calls.forecast - status.calls.limit).toLocaleString()} over the cap`
                : ''}
            </p>
          </div>
        </div>
        <div className="mt-3 h-2 overflow-hidden rounded bg-zinc-200 dark:bg-zinc-800">
          <div
            className={`h-full ${status.calls.used > status.calls.limit ? 'bg-red-600' : status.calls.forecast > status.calls.limit ? 'bg-amber-500' : 'bg-emerald-600'}`}
            style={{ width: `${Math.min(100, (status.calls.used / status.calls.limit) * 100)}%` }}
          />
        </div>
        <p className="mt-3 text-sm text-zinc-600 dark:text-zinc-300">
          {status.calls.windowStartedAt
            ? `Window started ${status.calls.windowStartedAt} · resets ${status.calls.resetsAt}`
            : 'This window has not started. The count begins at the next TrackSolid call.'}
        </p>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
          {status.calls.pollRunning
            ? `If the poll keeps running every ${status.calls.pollIntervalSeconds} seconds until the window resets, this window finishes at ${status.calls.forecast.toLocaleString()} calls.`
            : `The poll is paused, so the forecast stays at the ${status.calls.used.toLocaleString()} calls already made.`}
          {' '}A full 24 hours at {status.calls.pollIntervalSeconds} seconds is {status.calls.fullDayAtInterval.toLocaleString()} calls, {shareOfLimit(status.calls.fullDayAtInterval, status.calls.limit)} of the limit.
        </p>
        {status.calls.byMethod.length > 0 && (
          <ul className="mt-3 space-y-1 text-sm text-zinc-700 dark:text-zinc-200">
            {status.calls.byMethod.map((row) => (
              <li key={row.method} className="flex justify-between gap-4 font-mono">
                <span>{row.method}</span>
                <span>{row.count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
      )}

      <div id="service-location-alerts" className="mt-4 max-w-3xl scroll-mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Location alerts</h2>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
          Stop the service checker before you stop the location poll. A planned stop then sends only the stop email. If the checker is still running and finds the poll stopped, for any reason, it sends one alert and does not send again until the poll is started.
        </p>
        <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-300">
          The checker runs in this same server. If the server process stops, the checker stops with it and cannot send that alert.
        </p>
        <label className="mt-4 block text-sm text-zinc-700 dark:text-zinc-200">
          Alert emails
          <input
            type="text"
            value={alertEmails}
            placeholder="name@example.com, other@example.com"
            onChange={(e) => {
              setAlertDirty(true);
              setAlertEmails(e.target.value);
            }}
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
        </label>
        <p className="mt-1 text-xs text-zinc-500">Separate addresses with a comma. Each start, stop, or checker alert sends one email to the whole list.</p>
        <div className="mt-4 flex flex-wrap items-end gap-3">
          <label className="text-sm text-zinc-700 dark:text-zinc-200">
            Check every
            <input
              type="number"
              min={10}
              max={3600}
              step={1}
              value={checkerSeconds}
              onChange={(e) => {
                setAlertDirty(true);
                setCheckerSeconds(e.target.value);
              }}
              className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
            seconds
          </label>
          <button
            type="button"
            disabled={busy}
            onClick={() => void actAlerts('alert-settings')}
            className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
          >
            Save
          </button>
          <button
            type="button"
            disabled={busy || status?.alerts?.checkerRunning}
            onClick={() => void actAlerts('checker-start')}
            className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
          >
            Start checker
          </button>
          <button
            type="button"
            disabled={busy || !status?.alerts?.checkerRunning}
            onClick={() => void actAlerts('checker-stop')}
            className="rounded bg-zinc-700 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
          >
            Stop checker
          </button>
        </div>
        <p className="mt-3 text-sm text-zinc-600 dark:text-zinc-300">
          {status?.alerts?.checkerRunning
            ? `Checker running every ${status.alerts.checkerIntervalSeconds} sec`
            : 'Checker stopped'}
          {status?.alerts?.sendGridConfigured ? '' : ' · SendGrid is not configured'}
          {status?.alerts?.lastCheckAt ? ` · last check ${status.alerts.lastCheckAt}` : ''}
        </p>
        {status?.alerts?.lastResult && <p className="mt-1 text-sm text-zinc-700 dark:text-zinc-200">{status.alerts.lastResult}</p>}
        {status?.alerts?.lastEmailAt && (
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Last email {status.alerts.lastEmailAt}</p>
        )}
        {status?.alerts?.lastEmailError && (
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{status.alerts.lastEmailError}</p>
        )}
      </div>
      </section>

      <section id="service-vwork-jobs" className="mt-10 scroll-mt-6 border-t border-zinc-200 pt-8 dark:border-zinc-700">
        <h2 className="text-lg font-semibold text-zinc-900 dark:text-zinc-100">VWork jobs</h2>
        <VworkApiPanel view="service" onRunningChange={setVworkRunning} />
      </section>

      <section id="service-gate-updates" className="mt-10 scroll-mt-6 border-t border-zinc-200 pt-8 dark:border-zinc-700">
        <h2 className="text-lg font-semibold text-zinc-900 dark:text-zinc-100">Gate updates</h2>
        <VworkGateUpdatesPanel view="service" onRunningChange={setGateRunning} />
      </section>
      <div className="h-[70vh]" aria-hidden="true" />
      </>
      )}

      {tab === 'rows' && (
      <div className="mt-3 max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-xs">
          <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
            <tr>
              {LOCATION_COLUMNS.map((column) => (
                <th key={column.key} className="px-2 py-2 align-top font-medium text-zinc-500">
                  <button
                    type="button"
                    onClick={() => toggleLocationSort(column.key)}
                    className="flex items-center gap-1 text-left font-medium text-zinc-600 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-zinc-100"
                  >
                    {column.label}
                    {locationSort === column.key ? (locationSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                  </button>
                  <input
                    type="text"
                    value={locationFilters[column.key] ?? ''}
                    placeholder="Filter"
                    onChange={(event) => {
                      const value = event.target.value;
                      setLocationFilters((current) => ({ ...current, [column.key]: value }));
                    }}
                    className="mt-1 w-full min-w-[6rem] rounded border border-zinc-300 bg-white px-1.5 py-1 text-xs font-normal text-zinc-800 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-100"
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {locationRows.length === 0 ? (
              <tr>
                <td colSpan={LOCATION_COLUMNS.length} className="px-2 py-4 text-zinc-500">
                  {(status?.rows ?? []).length === 0 ? 'No rows yet.' : 'No rows match these filters.'}
                </td>
              </tr>
            ) : (
              locationRows.map((row) => (
                <tr key={row.id} className="border-t border-zinc-100 dark:border-zinc-800">
                  {LOCATION_COLUMNS.map((column) => (
                    <td key={column.key} className={`px-2 py-1.5 ${column.mono ? 'font-mono' : ''}`}>
                      {row[column.key] || '—'}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}

      {tab === 'summary' && (
      <div className="mt-3 max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-sm">
          <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
            <tr>
              {['Poll time', 'Fetched', 'Written', 'Trucks written'].map((h) => (
                <th key={h} className="px-3 py-2 font-medium text-zinc-500">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(() => {
              const lines = (status?.logs ?? []).map((log) => ({ log, summary: pollSummary(log) })).filter((line) => line.summary);
              if (lines.length === 0) {
                return (
                  <tr>
                    <td colSpan={4} className="px-3 py-4 text-zinc-500">No polls yet.</td>
                  </tr>
                );
              }
              return lines.map(({ log, summary }) => (
                <tr key={log.logid} className="border-t border-zinc-100 dark:border-zinc-800">
                  <td className="whitespace-nowrap px-3 py-2 font-mono">{summary?.polltime}</td>
                  <td className="px-3 py-2">{summary?.fetched}</td>
                  <td className="px-3 py-2">{summary?.written}</td>
                  <td className="px-3 py-2">{summary?.trucks}</td>
                </tr>
              ));
            })()}
          </tbody>
        </table>
      </div>
      )}

      {tab === 'log' && (
      <div className="mt-3 max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-xs">
          <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
            <tr>
              <th className="px-2 py-2 font-medium text-zinc-500">When</th>
              <th className="px-2 py-2 font-medium text-zinc-500">Stage</th>
              <th className="px-2 py-2 font-medium text-zinc-500">Details</th>
            </tr>
          </thead>
          <tbody>
            {(status?.logs ?? []).length === 0 ? (
              <tr>
                <td colSpan={3} className="px-2 py-4 text-zinc-500">No poll logs yet.</td>
              </tr>
            ) : (
              status?.logs.map((log) => (
                <tr key={log.logid} className="border-t border-zinc-100 align-top dark:border-zinc-800">
                  <td className="whitespace-nowrap px-2 py-1.5 font-mono">{log.logdatetime || '—'}</td>
                  <td className="px-2 py-1.5">{log.logcat2 || '—'}</td>
                  <td className="px-2 py-1.5">
                    <pre className="max-w-3xl whitespace-pre-wrap break-all font-mono text-[11px] text-zinc-600 dark:text-zinc-400">
                      {log.logdetails}
                    </pre>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}

      {tab === 'vwork-api' && <VworkApiPanel view="log" />}

      {tab === 'gate-updates' && <VworkGateUpdatesPanel view="queue" />}

      {tab === 'vwork' && <VworkFeedPanel />}

      {tab === 'gate-speech' && <GateSpeechPanel />}

      {confirmEmpty && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" role="dialog" aria-modal="true" aria-labelledby="empty-locations-title">
          <div className="w-full max-w-md rounded-lg bg-white p-5 shadow-xl dark:bg-zinc-900">
            <h2 id="empty-locations-title" className="text-lg font-semibold text-zinc-900 dark:text-zinc-100">
              Empty current locations
            </h2>
            <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-300">
              Remove all rows from the current locations table. The poll keeps running, and the next reading starts from an empty table.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                disabled={busy}
                onClick={() => setConfirmEmpty(false)}
                className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => {
                  setConfirmEmpty(false);
                  void act('empty');
                }}
                className="rounded bg-red-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
              >
                Empty table
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
