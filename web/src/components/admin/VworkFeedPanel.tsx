'use client';

import { useCallback, useEffect, useState } from 'react';

type FeedRow = {
  job_id: string;
  customer: string | null;
  template: string | null;
  worker: string | null;
  status: string | null;
  planned_start_time: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  truck_id: string | null;
  truck_rego: string | null;
  fetched_at: string | null;
};

type FeedResponse = {
  configured?: boolean;
  keyState?: 'missing' | 'placeholder' | 'ready';
  total?: number;
  rows?: FeedRow[];
  fetched?: number;
  alreadyInVworkJobs?: number;
  alreadyInFeed?: number;
  inserted?: number;
  truncated?: boolean;
  lookbackDays?: number;
  error?: string;
};

export function VworkFeedPanel() {
  const [rows, setRows] = useState<FeedRow[]>([]);
  const [total, setTotal] = useState(0);
  const [configured, setConfigured] = useState(false);
  const [keyState, setKeyState] = useState<'missing' | 'placeholder' | 'ready'>('missing');
  const [lookbackDays, setLookbackDays] = useState('14');
  const [summary, setSummary] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    fetch('/api/admin/vwork-feed', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as FeedResponse;
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setTotal(data.total ?? 0);
        setConfigured(data.configured === true);
        setKeyState(data.keyState ?? 'missing');
        setError(null);
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function run(action: 'check' | 'test') {
    setBusy(true);
    setError(null);
    try {
      const response = await fetch('/api/admin/vwork-feed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, lookbackDays: Number(lookbackDays) }),
      });
      const data = (await response.json()) as FeedResponse;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setRows(data.rows ?? []);
      setTotal(data.total ?? 0);
      setConfigured(data.configured === true);
      const added = data.inserted ?? 0;
      setSummary(
        action === 'test'
          ? `Added ${added} test jobs for Mike, registration ABC123.`
          : `Checked jobs updated in the last ${data.lookbackDays ?? lookbackDays} days. Fetched ${data.fetched ?? 0}, already in VWork jobs ${data.alreadyInVworkJobs ?? 0}, already in this feed ${data.alreadyInFeed ?? 0}, added ${added}.${data.truncated ? ' The page limit was reached before the list finished.' : ''}`,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mt-4">
      <p className="max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
        Checks the VWork jobs API for jobs updated in the last few days. A job is added here only when its id is not already in VWork jobs and not already in this feed. Planned times are stored as the API sent them.
      </p>
      {!configured && (
        <p className="mt-3 text-sm text-amber-800 dark:text-amber-200">
          {keyState === 'placeholder'
            ? 'VWORK_API_KEY is still the placeholder in the environment. Live checks stay off until it is replaced.'
            : 'VWORK_API_KEY is not set. Live checks stay off until it is added.'}
        </p>
      )}
      {error && (
        <div className="mt-3 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}
      <div className="mt-3 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <label className="text-sm text-zinc-700 dark:text-zinc-200">
          Updated in the last
          <input
            type="number"
            min={1}
            max={90}
            step={1}
            value={lookbackDays}
            onChange={(event) => setLookbackDays(event.target.value)}
            className="mx-2 w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
          days
        </label>
        <button
          type="button"
          disabled={busy || !configured}
          onClick={() => void run('check')}
          className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          Check for new jobs
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void run('test')}
          className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
        >
          Add 5 test jobs
        </button>
        <p className="pb-1 text-sm text-zinc-600 dark:text-zinc-300">{total.toLocaleString()} in the feed</p>
      </div>
      {summary && <p className="mt-3 text-sm text-zinc-700 dark:text-zinc-200">{summary}</p>}
      <div className="mt-3 overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full text-left text-xs">
          <thead className="bg-zinc-100 dark:bg-zinc-800">
            <tr>
              {['Job', 'Customer', 'Worker', 'Status', 'Planned start', 'Vineyard', 'Winery', 'Registration', 'Fetched'].map((heading) => (
                <th key={heading} className="px-2 py-2 font-medium text-zinc-500">{heading}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-2 py-4 text-zinc-500">No new jobs in the feed yet.</td>
              </tr>
            ) : (
              rows.map((row) => (
                <tr key={row.job_id} className="border-t border-zinc-100 dark:border-zinc-800">
                  <td className="px-2 py-1.5 font-mono">{row.job_id}</td>
                  <td className="px-2 py-1.5">{row.customer || '—'}</td>
                  <td className="px-2 py-1.5">{row.worker || '—'}</td>
                  <td className="px-2 py-1.5">{row.status || '—'}</td>
                  <td className="px-2 py-1.5 font-mono">{row.planned_start_time || '—'}</td>
                  <td className="px-2 py-1.5">{row.vineyard_name || '—'}</td>
                  <td className="px-2 py-1.5">{row.delivery_winery || '—'}</td>
                  <td className="px-2 py-1.5">{row.truck_rego || row.truck_id || '—'}</td>
                  <td className="px-2 py-1.5 font-mono">{row.fetched_at || '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
