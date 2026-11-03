'use client';

import { useEffect, useState } from 'react';

type DiversionType = { id: number; name: string; sort_order: number };

/** Driver app setup: the diversion reasons a driver can start on a job. */
export default function DriverDiversionSetup() {
  const [types, setTypes] = useState<DiversionType[]>([]);
  const [drafts, setDrafts] = useState<Record<number, string>>({});
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function load() {
    const res = await fetch('/api/admin/driver-diversions', { cache: 'no-store' });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.error ?? res.statusText);
    const rows = Array.isArray(data?.types) ? (data.types as DiversionType[]) : [];
    setTypes(rows);
    setDrafts(Object.fromEntries(rows.map((row) => [row.id, row.name])));
  }

  useEffect(() => {
    let cancelled = false;
    load()
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  async function addType() {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/admin/driver-diversions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setName('');
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function saveType(id: number) {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`/api/admin/driver-diversions/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: drafts[id] ?? '' }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function removeType(id: number) {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`/api/admin/driver-diversions/${id}`, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mt-3 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
      <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">Diversions</p>
      <p className="mt-0.5 max-w-3xl text-xs text-zinc-500">
        Reasons a driver can start on a job, such as a breakdown, a break, or a refuel. The clock is not tied to a gate. Past clocks keep the name they were started with.
      </p>
      {error && <p className="mt-2 text-sm text-red-700 dark:text-red-300">{error}</p>}
      <ul className="mt-3 flex max-w-xl flex-col gap-2">
        {types.map((type) => (
          <li key={type.id} className="flex flex-wrap items-center gap-2">
            <input
              value={drafts[type.id] ?? ''}
              onChange={(event) => setDrafts((current) => ({ ...current, [type.id]: event.target.value }))}
              className="min-w-[12rem] flex-1 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
            <button
              type="button"
              disabled={busy || (drafts[type.id] ?? '') === type.name}
              onClick={() => void saveType(type.id)}
              className="rounded border border-zinc-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-zinc-600"
            >
              Save
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => void removeType(type.id)}
              className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-700 disabled:opacity-50 dark:border-red-800 dark:text-red-300"
            >
              Delete
            </button>
          </li>
        ))}
      </ul>
      <form
        className="mt-3 flex max-w-xl flex-wrap items-center gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          void addType();
        }}
      >
        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="New diversion"
          className="min-w-[12rem] flex-1 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
        />
        <button
          type="submit"
          disabled={busy || !name.trim()}
          className="rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          Add
        </button>
      </form>
    </div>
  );
}
