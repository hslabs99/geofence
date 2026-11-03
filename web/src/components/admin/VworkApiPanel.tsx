'use client';

import { useCallback, useEffect, useState } from 'react';

type LogRow = {
  logid: number;
  logdatetime: string | null;
  logcat2: string | null;
  logdetails: string | null;
};

type Settings = {
  pollIntervalSeconds: number;
  lookbackDays: number;
};

type Status = {
  running: boolean;
  settings?: Settings;
  keyState?: 'missing' | 'placeholder' | 'ready';
  configured?: boolean;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  nextPollAtMs?: number | null;
  serverNowMs?: number;
  receivedAt?: number;
  logs: LogRow[];
  error?: string;
};

function stamp(data: Status): Status {
  return { ...data, receivedAt: Date.now() };
}

function countdown(status: Status | null, tick: number): number | null {
  void tick;
  if (!status?.running || status.nextPollAtMs == null || status.serverNowMs == null || status.receivedAt == null) return null;
  const now = status.serverNowMs + (Date.now() - status.receivedAt);
  return Math.max(0, Math.ceil((status.nextPollAtMs - now) / 1000));
}

export function VworkApiPanel() {
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [pollSeconds, setPollSeconds] = useState('60');
  const [lookbackDays, setLookbackDays] = useState('2');
  const [dirty, setDirty] = useState(false);
  const [tick, setTick] = useState(0);

  const load = useCallback(() => {
    fetch('/api/admin/vwork-api', { cache: 'no-store' })
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
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, [load]);

  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (dirty || !status?.settings) return;
    setPollSeconds(String(status.settings.pollIntervalSeconds));
    setLookbackDays(String(status.settings.lookbackDays));
  }, [dirty, status]);

  async function act(action: 'start' | 'stop' | 'once' | 'settings') {
    setBusy(true);
    setError(null);
    try {
      const response = await fetch('/api/admin/vwork-api', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action,
          pollIntervalSeconds: Number(pollSeconds),
          lookbackDays: Number(lookbackDays),
        }),
      });
      const data = (await response.json()) as Status;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setDirty(false);
      setStatus(stamp(data));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const nextIn = countdown(status, tick);
  const keyState = status?.keyState ?? 'missing';

  return (
    <div className="mt-4">
      <p className="max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
        Polls the VWork jobs API and stores jobs that are not already in VWork jobs. Open jobs from that feed are added to the Driver Visibility job queue. Completed and cancelled jobs stay in the feed. Planned times are stored as the API sent them. Closing the browser leaves the poll running.
      </p>
      {keyState !== 'ready' && (
        <p className="mt-3 text-sm text-amber-800 dark:text-amber-200">
          {keyState === 'placeholder'
            ? 'VWORK_API_KEY is still the placeholder. The poll can run, and it will queue jobs already in the feed, but it will not call VWork until the key is replaced.'
            : 'VWORK_API_KEY is not set. The poll can queue jobs already in the feed, but it will not call VWork until the key is added.'}
        </p>
      )}
      {error && (
        <div className="mt-3 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}
      <div className="mt-3 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <button
          type="button"
          disabled={busy || status?.running}
          onClick={() => void act('start')}
          className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Start
        </button>
        <button
          type="button"
          disabled={busy || !status?.running}
          onClick={() => void act('stop')}
          className="rounded bg-zinc-700 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Stop
        </button>
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Poll every
          <input
            type="number"
            min={30}
            max={3600}
            step={1}
            value={pollSeconds}
            onChange={(event) => {
              setDirty(true);
              setPollSeconds(event.target.value);
            }}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          seconds
        </label>
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Updated in the last
          <input
            type="number"
            min={1}
            max={30}
            step={1}
            value={lookbackDays}
            onChange={(event) => {
              setDirty(true);
              setLookbackDays(event.target.value);
            }}
            className="mx-2 w-16 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          days
        </label>
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
          disabled={busy}
          onClick={() => void act('once')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
        >
          Run once
        </button>
      </div>
      <p className="mt-3 text-sm text-zinc-700 dark:text-zinc-200">
        {status?.running ? `Running. Next poll in ${nextIn ?? '—'}s.` : 'Stopped.'}
        {status?.lastPolltime ? ` Last poll ${status.lastPolltime}.` : ''}
      </p>
      {status?.lastSummary && <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-300">{status.lastSummary}</p>}
      {status?.lastError && <p className="mt-1 text-sm text-red-700 dark:text-red-300">{status.lastError}</p>}
      <div className="mt-3 max-h-[50vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-xs">
          <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
            <tr>
              {['When', 'Stage', 'Details'].map((heading) => (
                <th key={heading} className="px-2 py-2 font-medium text-zinc-500">{heading}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(status?.logs ?? []).length === 0 ? (
              <tr>
                <td colSpan={3} className="px-2 py-4 text-zinc-500">No VWork API polls yet.</td>
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
    </div>
  );
}
