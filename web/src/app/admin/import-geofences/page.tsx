'use client';

import { useState, useRef } from 'react';

export default function ImportGeofencesPage() {
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'done' | 'error'>('idle');
  const [message, setMessage] = useState<string>('');
  const [result, setResult] = useState<{ imported: number; total: number; names: string[] } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

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
    } catch (err) {
      setMessage(err instanceof Error ? err.message : 'Upload failed');
      setStatus('error');
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Import Geofences</h1>
      <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
        Upload a KML file containing &lt;Placemark&gt; elements with &lt;name&gt; and &lt;Polygon&gt;&lt;coordinates&gt;. Each polygon is imported as one geofence.
      </p>

      <div className="max-w-lg rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <label className="block">
          <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">KML file</span>
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
              status === 'error' ? 'text-red-600 dark:text-red-400' : 'text-zinc-600 dark:text-zinc-400'
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
              {result.names.length > 20 && <li>… and {result.names.length - 20} more</li>}
            </ul>
          </details>
        )}
      </div>
    </div>
  );
}
