'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { GEODATA_DB_CONNECTION_CHANGED } from '@/lib/db-connection-events';

type DbTarget = 'cloudsql' | 'supabase';

interface VerifyRow {
  target: DbTarget;
  configured: boolean;
  connected: boolean;
  error: string | null;
  durationMs: number;
  database: string | null;
  host: string | null;
  label: string | null;
}

interface DbTargetResponse {
  ok: boolean;
  target?: DbTarget;
  activeConnection?: { database: string; host: string; label: string } | null;
  message?: string;
  error?: string;
  hint?: string | null;
  verify?: { cloudsql: VerifyRow; supabase: VerifyRow };
}

function VerifyCard({ row }: { row: VerifyRow }) {
  return (
    <div
      className={`rounded border p-3 text-sm ${
        row.connected
          ? 'border-emerald-300 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/30'
          : 'border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-950/30'
      }`}
    >
      <div className="font-semibold">{row.label ?? row.target}</div>
      <div className="text-xs text-zinc-600 dark:text-zinc-400">
        {row.database ?? '—'} @ {row.host ?? '—'}
      </div>
      <div className={`mt-1 text-xs font-medium ${row.connected ? 'text-emerald-700 dark:text-emerald-400' : 'text-red-700 dark:text-red-400'}`}>
        {row.connected ? `Connected (${row.durationMs}ms)` : `Not connected (${row.durationMs}ms)`}
      </div>
      {!row.connected && row.error && (
        <div className="mt-1 text-xs text-red-800 dark:text-red-300">{row.error}</div>
      )}
    </div>
  );
}

/** Emergency DB switch — no login; verifies live before switching. */
export default function DbSwitchPage() {
  const [info, setInfo] = useState<DbTargetResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [switching, setSwitching] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/celgps-migration/db-target?verify=1', { cache: 'no-store' });
      const data = (await res.json()) as DbTargetResponse;
      setInfo(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const switchTo = async (target: DbTarget) => {
    setSwitching(true);
    setMessage(null);
    setError(null);
    try {
      const res = await fetch('/api/celgps-migration/db-target', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target }),
      });
      const data = (await res.json()) as DbTargetResponse;
      if (data.ok) {
        setMessage(data.message ?? `Switched to ${target}.`);
        window.dispatchEvent(new Event(GEODATA_DB_CONNECTION_CHANGED));
        await load();
      } else {
        setError([data.error, data.hint].filter(Boolean).join(' — '));
        if (data.verify) setInfo((prev) => ({ ...prev, ok: prev?.ok ?? false, verify: data.verify, target: prev?.target } as DbTargetResponse));
        await load();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSwitching(false);
    }
  };

  const verify = info?.verify;

  return (
    <div className="mx-auto max-w-lg p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Emergency DB switch</h1>
      <p className="mb-6 text-sm text-zinc-600 dark:text-zinc-400">
        Verifies each database with a 5-second timeout before switching. Google Cloud SQL must be running to select it;
        Supabase is used when Cloud SQL is stopped.
      </p>

      {loading && <p className="text-sm text-zinc-500">Testing connections…</p>}

      {!loading && verify && (
        <div className="mb-6 grid gap-3 sm:grid-cols-2">
          <VerifyCard row={verify.cloudsql} />
          <VerifyCard row={verify.supabase} />
        </div>
      )}

      {!loading && info && (
        <div className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 text-sm dark:border-zinc-700 dark:bg-zinc-900">
          <div className="mb-1 text-xs font-semibold uppercase tracking-wider text-zinc-500">App target (configured)</div>
          <div className="font-mono text-lg font-bold">{info.target ?? 'unknown'}</div>
        </div>
      )}

      <div className="mb-6 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={switching || loading}
          onClick={() => switchTo('supabase')}
          className="rounded bg-emerald-700 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-600 disabled:opacity-50"
        >
          Switch to Supabase
        </button>
        <button
          type="button"
          disabled={switching || loading}
          onClick={() => switchTo('cloudsql')}
          className="rounded border border-zinc-300 px-4 py-2 text-sm text-zinc-700 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:text-zinc-300"
        >
          Switch to Google Cloud SQL
        </button>
        <button
          type="button"
          disabled={loading}
          onClick={() => load()}
          className="rounded border border-zinc-300 px-4 py-2 text-sm text-zinc-700 dark:border-zinc-600 dark:text-zinc-300"
        >
          Re-test
        </button>
      </div>

      {message && (
        <p className="mb-4 rounded border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200">
          {message}{' '}
          <Link href="/" className="font-medium underline">
            Go to Home
          </Link>
        </p>
      )}
      {error && (
        <p className="mb-4 rounded border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
          {error}
        </p>
      )}
    </div>
  );
}
