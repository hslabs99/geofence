'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

type Row = {
  id: string;
  driver_job_id: number;
  job_id: string | null;
  vwork_account: string | null;
  step: number;
  step_title: string;
  stage_at: string;
  received_at: string;
  status: string;
  executed_at: string | null;
  error: string | null;
  attempts: number;
  winery_name: string | null;
  vineyard_name: string | null;
  truck_id: string | null;
};

const GATE_STATUSES = [
  ['queued', 'Queued'],
  ['sending', 'Sending'],
  ['sent', 'Sent'],
  ['failed', 'Failed'],
  ['skipped', 'Skipped'],
] as const;

const API_OPTIONS = [
  ['delegat', 'Delegat'],
  ['others', 'Others'],
] as const;

const QUEUE_COLUMNS = 9;

type Counts = {
  queued: number;
  sending: number;
  sent: number;
  failed: number;
  skipped: number;
};

type Status = {
  running: boolean;
  intervalSeconds: number;
  lastRunAt: string | null;
  lastError: string | null;
  lastSummary: string | null;
  nextRunAtMs: number | null;
  serverNowMs: number;
  receivedAt?: number;
  rows: Row[];
  counts: Counts;
  error?: string;
};

function stamp(data: Status): Status {
  return { ...data, receivedAt: Date.now() };
}

function countdown(status: Status | null, tick: number): number | null {
  void tick;
  if (!status?.running || status.nextRunAtMs == null || status.serverNowMs == null || status.receivedAt == null) return null;
  const now = status.serverNowMs + (Date.now() - status.receivedAt);
  return Math.max(0, Math.ceil((status.nextRunAtMs - now) / 1000));
}

function statusLabel(status: string): string {
  return GATE_STATUSES.find(([value]) => value === status)?.[1] ?? status;
}

function accountLabel(account: string | null): string {
  if (account === 'others') return 'Others';
  if (account === 'delegat') return 'Delegat';
  return '—';
}

const filterSelectClass =
  'mt-1 w-full min-w-[6.5rem] rounded border border-zinc-300 bg-white px-1.5 py-1 text-xs font-normal text-zinc-800 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-100';

export function VworkGateUpdatesPanel({
  view = 'service',
  onRunningChange,
}: {
  view?: 'service' | 'queue';
  onRunningChange?: (running: boolean) => void;
}) {
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [acting, setActing] = useState<string | null>(null);
  const [intervalSeconds, setIntervalSeconds] = useState('60');
  const [dirty, setDirty] = useState(false);
  const [tick, setTick] = useState(0);
  const [statusFilter, setStatusFilter] = useState('');
  const [apiFilter, setApiFilter] = useState('');

  const load = useCallback(() => {
    fetch('/api/admin/vwork-gate-updates', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as Status;
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setStatus(stamp(data));
        setError(null);
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)));
  }, []);

  useEffect(() => {
    if (status) onRunningChange?.(status.running);
  }, [status, onRunningChange]);

  useEffect(() => {
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, [load]);

  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (dirty || !status) return;
    setIntervalSeconds(String(status.intervalSeconds));
  }, [dirty, status]);

  async function act(action: 'start' | 'stop' | 'once' | 'settings' | 'retry-failed') {
    setBusy(true);
    setActing(action);
    setError(null);
    try {
      const response = await fetch('/api/admin/vwork-gate-updates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, intervalSeconds: Number(intervalSeconds) }),
      });
      const data = (await response.json()) as Status;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setDirty(false);
      setStatus(stamp(data));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setActing(null);
      setBusy(false);
    }
  }

  const counts = status?.counts;
  const waiting = (counts?.queued ?? 0) + (counts?.sending ?? 0);
  const needsRetry = (counts?.failed ?? 0) + (counts?.skipped ?? 0);
  const nextIn = countdown(status, tick);
  const rows = useMemo(() => {
    return (status?.rows ?? []).filter((row) => {
      if (statusFilter && row.status !== statusFilter) return false;
      if (apiFilter && (row.vwork_account ?? '') !== apiFilter) return false;
      return true;
    });
  }, [status?.rows, statusFilter, apiFilter]);

  return (
    <div className="mt-4">
      <p className="max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
        {view === 'queue'
          ? 'Queue of driver stage taps. Start and stop this service on Services. Process queue now sends every waiting row straight away.'
          : 'Sends a driver stage tap to the VWork job it came from. The tap itself only writes a row in this queue. Process queue now sends every waiting row straight away. The timer is the automatic cycle, and once a minute is enough for that. The queue itself is under VWork API → Gate updates.'}
      </p>
      {error && (
        <div className="mt-3 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}
      <div className="mt-3 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        {view === 'service' && (
        <button
          type="button"
          disabled={busy || status?.running}
          onClick={() => void act('start')}
          className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Start
        </button>
        )}
        {view === 'service' && (
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Process every
          <input
            type="number"
            min={5}
            max={3600}
            step={1}
            value={intervalSeconds}
            onChange={(event) => {
              setDirty(true);
              setIntervalSeconds(event.target.value);
            }}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          seconds
        </label>
        )}
        {view === 'service' && (
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('settings')}
          className="rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          Save
        </button>
        )}
        {view === 'service' && (
        <button
          type="button"
          disabled={busy || !status?.running}
          onClick={() => void act('stop')}
          className="rounded bg-zinc-700 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Stop
        </button>
        )}
        <button
          type="button"
          disabled={busy}
          onClick={() => void act('once')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm font-medium disabled:opacity-50 dark:border-zinc-600"
        >
          {acting === 'once' ? 'Processing…' : 'Process queue now'}
        </button>
        <button
          type="button"
          disabled={busy || needsRetry === 0}
          onClick={() => void act('retry-failed')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-zinc-600"
        >
          Retry failed
        </button>
      </div>
      <p className="mt-3 text-sm text-zinc-700 dark:text-zinc-200">
        <span className={status?.running ? 'font-medium text-emerald-700 dark:text-emerald-400' : 'font-medium text-amber-700 dark:text-amber-400'}>
          {status?.running ? 'Running' : 'Stopped'}
        </span>
        {status?.running ? (
          <span>
            {' '}
            · {nextIn == null ? 'next run time pending' : nextIn === 0 ? 'processing now' : `${nextIn} seconds to the next run`}
          </span>
        ) : (
          <span> · queued rows stay here until you process the queue or start this.</span>
        )}
        {status?.lastRunAt ? ` Last run ${status.lastRunAt}.` : ''}
      </p>
      <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
        {waiting} waiting, {counts?.sent ?? 0} sent, {counts?.failed ?? 0} failed, {counts?.skipped ?? 0} skipped.
      </p>
      {status?.lastSummary && <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">{status.lastSummary}</p>}
      {status?.lastError && <p className="mt-1 text-sm text-red-700 dark:text-red-300">{status.lastError}</p>}
      {view === 'queue' && (
      <div className="mt-3 max-h-[60vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-xs">
          <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
            <tr>
              {['Stage', 'Job', 'Winery', 'Vineyard', 'Truck'].map((heading) => (
                <th key={heading} className="px-2 py-2 align-top font-medium text-zinc-500">{heading}</th>
              ))}
              <th className="px-2 py-2 align-top font-medium text-zinc-500">
                API
                <select
                  aria-label="API"
                  value={apiFilter}
                  onChange={(event) => setApiFilter(event.target.value)}
                  className={filterSelectClass}
                >
                  <option value="">All</option>
                  {API_OPTIONS.map(([value, label]) => (
                    <option key={value} value={value}>{label}</option>
                  ))}
                </select>
              </th>
              {['Received from driver app', 'Executed in VWork'].map((heading) => (
                <th key={heading} className="px-2 py-2 align-top font-medium text-zinc-500">{heading}</th>
              ))}
              <th className="px-2 py-2 align-top font-medium text-zinc-500">
                Status
                <select
                  aria-label="Status"
                  value={statusFilter}
                  onChange={(event) => setStatusFilter(event.target.value)}
                  className={filterSelectClass}
                >
                  <option value="">All</option>
                  {GATE_STATUSES.map(([value, label]) => (
                    <option key={value} value={value}>{label}</option>
                  ))}
                </select>
              </th>
            </tr>
          </thead>
          <tbody>
            {(status?.rows ?? []).length === 0 ? (
              <tr>
                <td colSpan={QUEUE_COLUMNS} className="px-2 py-4 text-zinc-500">
                  No gate updates yet. A driver stage tap will appear here, then this service sends it to VWork.
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={QUEUE_COLUMNS} className="px-2 py-4 text-zinc-500">
                  No rows match these filters.
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <tr key={row.id} className="border-t border-zinc-100 align-top dark:border-zinc-800">
                  <td className="px-2 py-1.5">
                    <div>Step {row.step} · {row.step_title}</div>
                    <div className="mt-0.5 font-mono text-[11px] text-zinc-500">Stage time {row.stage_at}</div>
                  </td>
                  <td className="px-2 py-1.5 font-mono">{row.job_id || '—'}</td>
                  <td className="px-2 py-1.5">{row.winery_name?.trim() || '—'}</td>
                  <td className="px-2 py-1.5">{row.vineyard_name?.trim() || '—'}</td>
                  <td className="px-2 py-1.5 font-mono">{row.truck_id?.trim() || '—'}</td>
                  <td className="px-2 py-1.5">{accountLabel(row.vwork_account)}</td>
                  <td className="whitespace-nowrap px-2 py-1.5 font-mono">{row.received_at}</td>
                  <td className="whitespace-nowrap px-2 py-1.5 font-mono">{row.executed_at || '—'}</td>
                  <td className="px-2 py-1.5">
                    <div>{statusLabel(row.status)}</div>
                    {row.error ? <div className="mt-0.5 max-w-xl whitespace-pre-wrap text-[11px] leading-snug text-red-700 dark:text-red-300">{row.error}</div> : null}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}
    </div>
  );
}
