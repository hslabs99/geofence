'use client';

import { useCallback, useEffect, useState } from 'react';

type IssueJob = { id: number; jobId: string; driver: string | null; plannedStart: string | null };

type IssueRow = {
  id: number;
  kind: 'Winery' | 'Vineyard' | 'Driver';
  vworkName: string;
  candidates: string[];
  createdAt: string;
  jobs: IssueJob[];
};

type DriverChoice = { label: string; imei: string };

type IssuesResponse = {
  open?: number;
  fences?: string[];
  drivers?: DriverChoice[];
  rows?: IssueRow[];
  error?: string;
};

export function JobQueueIssues({
  onChange,
  onResolved,
}: {
  onChange?: (open: number) => void;
  onResolved?: (saved: { kind: 'Winery' | 'Vineyard' | 'Driver'; vworkName: string; fenceName: string }) => void;
}) {
  const [rows, setRows] = useState<IssueRow[]>([]);
  const [fences, setFences] = useState<string[]>([]);
  const [drivers, setDrivers] = useState<DriverChoice[]>([]);
  const [picks, setPicks] = useState<Record<number, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(() => {
    fetch('/api/driver-jobs/queue-issues', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as IssuesResponse;
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setFences(data.fences ?? []);
        setDrivers(data.drivers ?? []);
        onChange?.(data.open ?? data.rows?.length ?? 0);
        setError(null);
        setLoaded(true);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : String(err));
        setLoaded(true);
      });
  }, [onChange]);

  useEffect(() => {
    load();
    const timer = setInterval(load, 15000);
    return () => clearInterval(timer);
  }, [load]);

  async function resolve(issue: IssueRow) {
    const fenceName = picks[issue.id] ?? '';
    if (!fenceName) return;
    setBusyId(issue.id);
    setError(null);
    try {
      const response = await fetch('/api/driver-jobs/queue-issues', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: issue.id, fenceName }),
      });
      const data = (await response.json()) as IssuesResponse;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setRows(data.rows ?? []);
      setFences(data.fences ?? []);
      setDrivers(data.drivers ?? []);
      onChange?.(data.open ?? data.rows?.length ?? 0);
      onResolved?.({ kind: issue.kind, vworkName: issue.vworkName, fenceName });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div>
      <p className="mb-4 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
        A VWork winery, vineyard, or driver name is stored as typed. A fence is filled only when the place name maps to one fence, and an IMEI is filled only when the driver name maps to one driver in the TrackSolid driver table. Anything else stays here until you pick the match. That choice is kept for this VWork import.
      </p>
      {error && <div className="mb-4 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>}
      {!loaded ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">Checking fence names…</p>
      ) : rows.length === 0 ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">No open fence names.</p>
      ) : (
        <div className="space-y-3">
          {rows.map((issue) => (
            <div key={issue.id} className="rounded-lg border border-amber-300 bg-white p-3 dark:border-amber-700 dark:bg-zinc-900">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                  {issue.kind}: {issue.vworkName}
                </h2>
                <p className="text-xs text-zinc-500">{issue.createdAt}</p>
              </div>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                {issue.kind === 'Driver'
                  ? 'This driver name does not match one driver ID with a TrackSolid IMEI.'
                  : issue.candidates.length > 0
                    ? `Mapped fences: ${issue.candidates.join(', ')}`
                    : 'No fence mapping for this name.'}
              </p>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                Jobs waiting: {issue.jobs.length === 0 ? '—' : issue.jobs.map((job) => job.jobId).join(', ')}
              </p>
              <div className="mt-3 flex flex-wrap items-end gap-2">
                <label className="text-xs text-zinc-600 dark:text-zinc-300">
                  {issue.kind === 'Driver' ? 'Driver' : `${issue.kind} geo fence`}
                  <select
                    value={picks[issue.id] ?? ''}
                    onChange={(event) => setPicks((current) => ({ ...current, [issue.id]: event.target.value }))}
                    className="mt-1 block w-72 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  >
                    <option value="">{issue.kind === 'Driver' ? 'Select driver' : 'Select fence'}</option>
                    {issue.kind === 'Driver' ? (
                      drivers.map((driver) => (
                        <option key={driver.label} value={driver.label}>
                          {driver.label} · {driver.imei}
                        </option>
                      ))
                    ) : (
                      <>
                        {issue.candidates.length > 0 && (
                          <optgroup label="Mapped fences">
                            {issue.candidates.map((name) => (
                              <option key={`mapped-${name}`} value={name}>
                                {name}
                              </option>
                            ))}
                          </optgroup>
                        )}
                        <optgroup label="All geo fences">
                          {fences.map((name) => (
                            <option key={name} value={name}>
                              {name}
                            </option>
                          ))}
                        </optgroup>
                      </>
                    )}
                  </select>
                </label>
                <button
                  type="button"
                  disabled={busyId === issue.id || !(picks[issue.id] ?? '')}
                  onClick={() => void resolve(issue)}
                  className="rounded bg-blue-600 px-3 py-1 text-xs font-medium text-white disabled:opacity-50"
                >
                  {busyId === issue.id ? 'Saving…' : 'Save mapping'}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
