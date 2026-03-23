'use client';

import { useState } from 'react';

type AutoResult = { ok: boolean; error?: string; [k: string]: unknown } | null;

const AUTO_1 = { name: 'Auto 1: VW import', path: '/api/auto/vwimport' };
const AUTO_2 = { name: 'Auto 2: GPS import (3-day)', path: '/api/auto/gpsimport' };
const AUTO_3 = { name: 'Fence tagging (3-day)', path: '/api/auto/fencetags' };
const AUTO_4 = { name: 'Entry/exit + steps (3-day)', path: '/api/auto/entryexitsteps' };

export default function AutoPage() {
  const [running, setRunning] = useState<string | null>(null);
  const [result, setResult] = useState<AutoResult>(null);
  const [lastRun, setLastRun] = useState<string | null>(null);

  const run = async (label: string, path: string) => {
    setRunning(path);
    setResult(null);
    setLastRun(label);
    try {
      const res = await fetch(path, { method: 'POST' });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setResult({ ok: false, error: data.error ?? `HTTP ${res.status}` });
      } else {
        setResult(data);
      }
    } catch (e) {
      setResult({ ok: false, error: e instanceof Error ? e.message : 'Network error' });
    } finally {
      setRunning(null);
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Auto runs</h1>
      <p className="mb-6 text-sm text-zinc-500">
        Trigger pipeline steps. APIs live under <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">/api/auto/...</code> (e.g.{' '}
        <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">/api/auto/gpsimport</code>).
      </p>

      <div className="mb-8 flex flex-wrap gap-3">
        {[AUTO_1, AUTO_2, AUTO_3, AUTO_4].map(({ name, path }) => (
          <button
            key={path}
            type="button"
            onClick={() => run(name, path)}
            disabled={running !== null}
            className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {running === path ? 'Running…' : name}
          </button>
        ))}
      </div>

      {lastRun && (
        <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">{lastRun}</h2>
          {result === null ? (
            <p className="text-sm text-zinc-500">Waiting…</p>
          ) : (
            <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
              {JSON.stringify(result, null, 2)}
            </pre>
          )}
        </section>
      )}
    </div>
  );
}
