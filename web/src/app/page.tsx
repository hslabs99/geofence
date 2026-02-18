'use client';

import { useEffect, useState } from 'react';

type Row = Record<string, unknown>;

export default function Home() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/vworkjobs')
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText);
        return res.json();
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <div className="min-h-screen bg-zinc-50 p-6 dark:bg-zinc-950">
      <main className="mx-auto max-w-6xl">
        <h1 className="mb-6 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">
          tbl_vworkjobs
        </h1>
        {loading && <p className="text-zinc-600">Loading…</p>}
        {error && (
          <p className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
            {error}
          </p>
        )}
        {!loading && !error && rows.length === 0 && (
          <p className="text-zinc-600">No rows.</p>
        )}
        {!loading && !error && rows.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                  {columns.map((col) => (
                    <th key={col} className="px-4 py-2 font-medium text-zinc-900 dark:text-zinc-100">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => (
                  <tr
                    key={i}
                    className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                  >
                    {columns.map((col) => (
                      <td key={col} className="max-w-xs truncate px-4 py-2 text-zinc-700 dark:text-zinc-300">
                        {row[col] != null ? String(row[col]) : '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading && rows.length > 0 && (
          <p className="mt-3 text-zinc-500">
            {rows.length} row{rows.length !== 1 ? 's' : ''} (max 500)
          </p>
        )}
      </main>
    </div>
  );
}
