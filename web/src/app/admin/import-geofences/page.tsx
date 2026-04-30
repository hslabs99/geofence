'use client';

import { useState, useRef, useEffect } from 'react';
import Link from 'next/link';

type GeofenceRow = {
  fence_id: number;
  fence_name: string;
  map_lat: number | null;
  map_lon: number | null;
};

export default function GeoFencesPage() {
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'done' | 'error'>('idle');
  const [message, setMessage] = useState<string>('');
  const [result, setResult] = useState<{ imported: number; total: number; names: string[] } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const [geofences, setGeofences] = useState<GeofenceRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortBy, setSortBy] = useState<'fence_name' | 'fence_id'>('fence_name');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  const fetchGeofences = async () => {
    setLoading(true);
    try {
      const res = await fetch(
        `/api/admin/geofences?sort=${sortBy}&order=${sortOrder}`
      );
      if (res.ok) {
        const data = await res.json();
        setGeofences(Array.isArray(data) ? data : []);
      } else {
        setGeofences([]);
      }
    } catch {
      setGeofences([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchGeofences();
  }, [sortBy, sortOrder]);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    setFile(f ?? null);
    setStatus('idle');
    setMessage('');
    setResult(null);
  };

  const handleImport = async () => {
    if (!file) {
      setMessage('Please select a KML file.');
      setStatus('error');
      return;
    }
    setStatus('uploading');
    setMessage('');
    setResult(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const res = await fetch('/api/admin/import-geofences', {
        method: 'POST',
        body: formData,
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setMessage(data?.error ?? `Import failed (${res.status})`);
        setStatus('error');
        return;
      }

      setResult({
        imported: data.imported ?? 0,
        total: data.total ?? 0,
        names: Array.isArray(data.names) ? data.names : [],
      });
      setMessage(`Imported ${data.imported ?? 0} of ${data.total ?? 0} geofences.`);
      setStatus('done');
      setFile(null);
      if (inputRef.current) inputRef.current.value = '';
      await fetchGeofences();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : 'Upload failed');
      setStatus('error');
    }
  };

  const toggleSort = (col: 'fence_name' | 'fence_id') => {
    if (sortBy === col) {
      setSortOrder((o) => (o === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortBy(col);
      setSortOrder('asc');
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">
        GeoFences
      </h1>

      {/* Import section at top */}
      <section className="mb-8">
        <h2 className="mb-2 text-lg font-medium text-zinc-800 dark:text-zinc-200">
          Import geofences
        </h2>
        <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
          Upload a KML file containing &lt;Placemark&gt; elements with &lt;name&gt;
          and &lt;Polygon&gt;&lt;coordinates&gt;. Each polygon is imported as one
          geofence.
        </p>

        <div className="max-w-lg rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
          <label className="block">
            <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
              KML file
            </span>
            <div className="mt-2 flex items-center gap-2">
              <input
                ref={inputRef}
                type="file"
                accept=".kml,.xml,application/vnd.google-earth.kml+xml,application/xml,text/xml"
                onChange={handleFileChange}
                className="block w-full text-sm text-zinc-600 file:mr-2 file:rounded file:border-0 file:bg-zinc-200 file:px-3 file:py-2 file:text-sm file:font-medium file:text-zinc-800 hover:file:bg-zinc-300 dark:text-zinc-400 dark:file:bg-zinc-700 dark:file:text-zinc-200 dark:hover:file:bg-zinc-600"
              />
            </div>
          </label>

          <div className="mt-4 flex items-center gap-2">
            <button
              type="button"
              onClick={handleImport}
              disabled={status === 'uploading' || !file}
              className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
            >
              {status === 'uploading' ? 'Importing…' : 'Import'}
            </button>
            {file && (
              <span className="text-sm text-zinc-500 dark:text-zinc-400">
                {file.name}
              </span>
            )}
          </div>

          {message && (
            <p
              className={`mt-3 text-sm ${
                status === 'error'
                  ? 'text-red-600 dark:text-red-400'
                  : 'text-zinc-600 dark:text-zinc-400'
              }`}
            >
              {message}
            </p>
          )}

          {result && result.names.length > 0 && (
            <details className="mt-3 text-sm text-zinc-600 dark:text-zinc-400">
              <summary>Imported names ({result.names.length})</summary>
              <ul className="mt-1 list-inside list-disc">
                {result.names.slice(0, 20).map((n, i) => (
                  <li key={i}>{n}</li>
                ))}
                {result.names.length > 20 && (
                  <li>… and {result.names.length - 20} more</li>
                )}
              </ul>
            </details>
          )}
        </div>
      </section>

      {/* Table: tbl_geofences */}
      <section>
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">
          tbl_geofences
        </h2>
        {loading ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-zinc-200 dark:border-zinc-700">
            <table className="min-w-full text-left text-sm">
              <thead className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/50">
                <tr>
                  <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                    <button
                      type="button"
                      onClick={() => toggleSort('fence_id')}
                      className="hover:underline"
                    >
                      fence_id
                      {sortBy === 'fence_id' && (
                        <span className="ml-1">{sortOrder === 'asc' ? '↑' : '↓'}</span>
                      )}
                    </button>
                  </th>
                  <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                    <button
                      type="button"
                      onClick={() => toggleSort('fence_name')}
                      className="hover:underline"
                    >
                      fence_name
                      {sortBy === 'fence_name' && (
                        <span className="ml-1">{sortOrder === 'asc' ? '↑' : '↓'}</span>
                      )}
                    </button>
                  </th>
                  <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                    Map
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-200 dark:divide-zinc-700">
                {geofences.map((row) => (
                  <tr
                    key={row.fence_id}
                    className="bg-white dark:bg-zinc-900"
                  >
                    <td className="px-3 py-2 text-zinc-700 dark:text-zinc-300">
                      {row.fence_id}
                    </td>
                    <td className="px-3 py-2 text-zinc-700 dark:text-zinc-300">
                      {row.fence_name || '—'}
                    </td>
                    <td className="px-3 py-2">
                      {row.map_lat != null && row.map_lon != null ? (
                        <a
                          href={`https://www.google.com/maps?q=${row.map_lat},${row.map_lon}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:underline dark:text-blue-400"
                        >
                          View on map
                        </a>
                      ) : (
                        <span className="text-zinc-400 dark:text-zinc-500" title="No coordinates for this geometry">
                          View on map
                        </span>
                      )}
                      {' · '}
                      <a
                        href={`/api/admin/geofences/${row.fence_id}/kml`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-blue-600 hover:underline dark:text-blue-400"
                        title="Download KML to import into Google My Maps if the outline doesn’t show"
                      >
                        KML
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading && geofences.length === 0 && (
          <p className="mt-2 text-sm text-zinc-500 dark:text-zinc-400">
            No geofences in tbl_geofences.
          </p>
        )}
      </section>
    </div>
  );
}
