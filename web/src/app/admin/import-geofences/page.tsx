'use client';

import { useState, useRef, useEffect } from 'react';

type GeofenceRow = {
  fence_id: number;
  fence_name: string;
  map_lat: number | null;
  map_lon: number | null;
  /** Approximate polygon area (m²), PostGIS ST_Area(geom::geography). */
  area_m2: number | null;
  /** WGS84 axis-aligned bbox (deg): min lon, max lon, min lat, max lat. */
  bbox_west: number | null;
  bbox_east: number | null;
  bbox_south: number | null;
  bbox_north: number | null;
};

function fmtDeg5(n: number | null): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n.toFixed(5);
}

/** Midpoint lat/lon for opening Maps on a bbox edge. */
function edgeMapHref(
  edge: 'west' | 'east' | 'north' | 'south',
  w: number | null,
  e: number | null,
  s: number | null,
  n: number | null
): string | null {
  if (w == null || e == null || s == null || n == null) return null;
  const midLat = (n + s) / 2;
  const midLon = (e + w) / 2;
  let lat: number;
  let lon: number;
  if (edge === 'west') {
    lat = midLat;
    lon = w;
  } else if (edge === 'east') {
    lat = midLat;
    lon = e;
  } else if (edge === 'north') {
    lat = n;
    lon = midLon;
  } else {
    lat = s;
    lon = midLon;
  }
  return `https://www.google.com/maps?q=${lat},${lon}`;
}

function formatAreaM2(m2: number | null): string {
  if (m2 == null || !Number.isFinite(m2)) return '—';
  const rounded = m2 >= 100 ? Math.round(m2) : Math.round(m2 * 10) / 10;
  return `${rounded.toLocaleString()} m²`;
}

const GEO_TH_STICKY =
  'sticky top-0 z-10 align-top bg-zinc-50 px-3 py-2 font-medium text-zinc-700 shadow-[inset_0_-1px_0_0_rgb(228_228_231)] dark:bg-zinc-800 dark:text-zinc-300 dark:shadow-[inset_0_-1px_0_0_rgb(63_63_70)]';

function parseGeofenceApiRow(raw: unknown): GeofenceRow | null {
  if (raw == null || typeof raw !== 'object') return null;
  const r = raw as Record<string, unknown>;
  const fenceId = Number(r.fence_id);
  if (!Number.isFinite(fenceId)) return null;
  const asNum = (v: unknown): number | null => {
    if (v == null || v === '') return null;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
    const n = parseFloat(String(v));
    return Number.isFinite(n) ? n : null;
  };
  const strOrEmpty = (v: unknown) => (v != null ? String(v) : '');
  return {
    fence_id: fenceId,
    fence_name: strOrEmpty(r.fence_name),
    map_lat: asNum(r.map_lat),
    map_lon: asNum(r.map_lon),
    area_m2: asNum(r.area_m2),
    bbox_west: asNum(r.bbox_west),
    bbox_east: asNum(r.bbox_east),
    bbox_south: asNum(r.bbox_south),
    bbox_north: asNum(r.bbox_north),
  };
}

export default function GeoFencesPage() {
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'done' | 'error'>('idle');
  const [message, setMessage] = useState<string>('');
  const [result, setResult] = useState<{ imported: number; total: number; names: string[] } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const [geofences, setGeofences] = useState<GeofenceRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [geofencesLoadError, setGeofencesLoadError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'fence_name' | 'fence_id'>('fence_name');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  const fetchGeofences = async () => {
    setLoading(true);
    setGeofencesLoadError(null);
    try {
      const res = await fetch(
        `/api/admin/geofences?sort=${sortBy}&order=${sortOrder}`
      );
      if (res.ok) {
        const data = await res.json();
        const list = Array.isArray(data) ? data : [];
        setGeofences(
          list.map(parseGeofenceApiRow).filter((row): row is GeofenceRow => row != null)
        );
      } else {
        setGeofences([]);
        const body = await res.json().catch(() => ({}));
        const msg =
          typeof body?.error === 'string' && body.error.trim()
            ? body.error.trim()
            : `Could not load geofences (${res.status})`;
        setGeofencesLoadError(msg);
      }
    } catch (e) {
      setGeofences([]);
      setGeofencesLoadError(e instanceof Error ? e.message : 'Network error loading geofences');
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
        <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
          Rows from <code className="rounded bg-zinc-100 px-1 font-mono text-xs dark:bg-zinc-800">tbl_geofences</code>{' '}
          with area, bounding box, and map links from PostGIS <code className="rounded bg-zinc-100 px-1 font-mono text-xs dark:bg-zinc-800">geom</code>.
        </p>
        {geofencesLoadError && (
          <p className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
            {geofencesLoadError}
          </p>
        )}
        {loading ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
        ) : (
          <div className="max-h-[min(75vh,42rem)] overflow-auto overscroll-contain rounded-lg border border-zinc-200 dark:border-zinc-700">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th className={`${GEO_TH_STICKY} whitespace-nowrap`}>
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
                  <th className={GEO_TH_STICKY}>
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
                  <th
                    className={`${GEO_TH_STICKY} whitespace-nowrap`}
                    title="Approximate area in square metres (PostGIS ST_Area on geography)"
                  >
                    M2
                  </th>
                  <th
                    className={`${GEO_TH_STICKY} min-w-[11rem] text-xs leading-tight`}
                    title="Bounding box in WGS84 (PostGIS ST_XMin/ST_XMax/ST_YMin/ST_YMax). Empty when geom is null or empty."
                  >
                    BBox (°)
                  </th>
                  <th className={`${GEO_TH_STICKY} whitespace-nowrap`}>Map</th>
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
                    <td
                      className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-700 dark:text-zinc-300"
                      title={
                        row.area_m2 != null
                          ? `≈ ${row.area_m2.toLocaleString(undefined, { maximumFractionDigits: 1 })} m²`
                          : undefined
                      }
                    >
                      {formatAreaM2(row.area_m2 ?? null)}
                    </td>
                    <td className="max-w-[14rem] px-3 py-2 align-top font-mono text-[11px] leading-snug text-zinc-700 dark:text-zinc-300">
                      {row.bbox_west != null &&
                      row.bbox_east != null &&
                      row.bbox_south != null &&
                      row.bbox_north != null ? (
                        <>
                          <div className="tabular-nums">
                            W {fmtDeg5(row.bbox_west)} · E {fmtDeg5(row.bbox_east)}
                            <br />
                            N {fmtDeg5(row.bbox_north)} · S {fmtDeg5(row.bbox_south)}
                          </div>
                          <div className="mt-1 flex flex-wrap gap-x-2 gap-y-0.5 font-sans text-[11px]">
                            {(['west', 'east', 'north', 'south'] as const).map((edge) => {
                              const href = edgeMapHref(
                                edge,
                                row.bbox_west,
                                row.bbox_east,
                                row.bbox_south,
                                row.bbox_north
                              );
                              if (!href) return null;
                              const lab =
                                edge === 'west' ? 'W' : edge === 'east' ? 'E' : edge === 'north' ? 'N' : 'S';
                              return (
                                <a
                                  key={edge}
                                  href={href}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-blue-600 hover:underline dark:text-blue-400"
                                  title={`Maps: middle of ${edge} edge of bbox`}
                                >
                                  {lab}
                                </a>
                              );
                            })}
                          </div>
                        </>
                      ) : (
                        <span
                          className="font-sans text-zinc-500 dark:text-zinc-400"
                          title="PostGIS geom is null or empty — no bbox / area / interior point until geometry is stored."
                        >
                          No geometry
                        </span>
                      )}
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
        {!loading && !geofencesLoadError && geofences.length === 0 && (
          <p className="mt-2 text-sm text-zinc-500 dark:text-zinc-400">
            No geofences in tbl_geofences.
          </p>
        )}
      </section>
    </div>
  );
}
