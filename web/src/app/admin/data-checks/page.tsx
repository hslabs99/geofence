'use client';

import { useState } from 'react';

type SummaryRow = {
  report_date: string;
  device_name: string;
  max_gap_minutes: string;
  gap_count: number;
  day_count: number;
  gap_from: string | null;
  gap_to: string | null;
  gap_from_nz: string;
  gap_to_nz: string;
  gap_start_lat?: number | null;
  gap_start_lon?: number | null;
  gap_end_lat?: number | null;
  gap_end_lon?: number | null;
};

/** True if device moved between outage start and end (lat/lon at From !== at To). */
function deviceMovedDuringOutage(row: SummaryRow): boolean {
  const slat = row.gap_start_lat;
  const slon = row.gap_start_lon;
  const elat = row.gap_end_lat;
  const elon = row.gap_end_lon;
  if (slat == null || slon == null || elat == null || elon == null) return false;
  return slat !== elat || slon !== elon;
}

type TrackingWindowRow = {
  device_name: string;
  position_time: string | null;
  position_time_nz: string | null;
  fence_name: string | null;
  geofence_type: string | null;
  lat: number | null;
  lon: number | null;
};

type DetailRow = {
  device_name: string;
  gap_start: string;
  gap_end: string;
  gap_minutes: string;
};

type GapsResult = {
  summary: SummaryRow[];
  detail: DetailRow[];
  minGapMinutes: number;
  dateFrom: string | null;
  dateTo: string | null;
};

function formatDateInput(value: string): string {
  if (!value) return '';
  const d = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  return value;
}

/** Format YYYY-MM-DD as dd/mm/yy */
function formatDateDDMMYY(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  return `${m[3]}/${m[2]}/${yy}`;
}

/** Format YYYY-MM-DD HH:MI:SS as dd/mm hh:mm */
function formatGapDateTime(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  const h = m[4].padStart(2, '0');
  return `${m[3]}/${m[2]}/${yy} ${h}:${m[5]}`;
}

/** Haversine distance in meters between two lat/lon points. */
function distanceMeters(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth radius in meters
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/** Format meters as "X m" or "X.X km". */
function formatDistance(m: number): string {
  if (m >= 1000) return `${(m / 1000).toFixed(1)} km`;
  return `${Math.round(m)} m`;
}

const SORT_KEYS = [
  { value: 'report_date', label: 'Date' },
  { value: 'device_name', label: 'Device' },
  { value: 'max_gap_minutes', label: 'Max gap (min)' },
  { value: 'gap_count', label: '# gaps' },
  { value: 'day_count', label: 'DayCount' },
  { value: 'gap_from', label: 'From' },
  { value: 'gap_to', label: 'To' },
  { value: 'gap_from_nz', label: 'From_NZ' },
  { value: 'gap_to_nz', label: 'To_NZ' },
] as const;

type SortKey = (typeof SORT_KEYS)[number]['value'];

function getSortValue(row: SummaryRow, key: SortKey): string | number {
  const v = row[key];
  if (key === 'max_gap_minutes') return parseFloat(String(v)) ?? 0;
  if (key === 'gap_count' || key === 'day_count') return Number(v) ?? 0;
  return (v ?? '') as string;
}

function sortSummary(rows: SummaryRow[], sortBy1: SortKey, sortBy2: SortKey, dir: 'asc' | 'desc'): SummaryRow[] {
  const mult = dir === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {
    const v1a = getSortValue(a, sortBy1);
    const v1b = getSortValue(b, sortBy1);
    let cmp = v1a < v1b ? -1 : v1a > v1b ? 1 : 0;
    if (cmp !== 0) return mult * cmp;
    const v2a = getSortValue(a, sortBy2);
    const v2b = getSortValue(b, sortBy2);
    cmp = v2a < v2b ? -1 : v2a > v2b ? 1 : 0;
    return mult * cmp;
  });
}

export default function DataChecksPage() {
  const [minGapMinutes, setMinGapMinutes] = useState<string>('60');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GapsResult | null>(null);
  const [showDetail, setShowDetail] = useState(false);
  const [popupRow, setPopupRow] = useState<SummaryRow | null>(null);
  const [popupLoading, setPopupLoading] = useState(false);
  const [popupRows, setPopupRows] = useState<TrackingWindowRow[]>([]);
  const [popupError, setPopupError] = useState<string | null>(null);
  const [deviceFilter, setDeviceFilter] = useState('');
  const [sortBy1, setSortBy1] = useState<SortKey>('device_name');
  const [sortBy2, setSortBy2] = useState<SortKey>('gap_from');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const runGapsScan = () => {
    const minutes = parseFloat(minGapMinutes);
    if (Number.isNaN(minutes) || minutes < 0) {
      setError('Enter a valid gap threshold (minutes).');
      return;
    }
    setError(null);
    setLoading(true);
    setResult(null);
    const params = new URLSearchParams({ minGapMinutes: String(minutes) });
    if (dateFrom.trim()) params.set('dateFrom', formatDateInput(dateFrom.trim()));
    if (dateTo.trim()) params.set('dateTo', formatDateInput(dateTo.trim()));
    fetch(`/api/admin/data-checks/gps-gaps?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: GapsResult) => {
        setResult(data);
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setLoading(false));
  };

  const openTrackingPopup = (row: SummaryRow) => {
    if (row.gap_from == null || row.gap_to == null) return;
    setPopupRow(row);
    setPopupError(null);
    setPopupRows([]);
    setPopupLoading(true);
    const params = new URLSearchParams({
      device: row.device_name,
      from: row.gap_from,
      to: row.gap_to,
    });
    fetch(`/api/admin/data-checks/tracking-window?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: TrackingWindowRow[] }) => {
        setPopupRows(data.rows ?? []);
      })
      .catch((e) => {
        setPopupError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setPopupLoading(false));
  };

  return (
    <div className="min-h-screen bg-zinc-50 dark:bg-zinc-950">
      <div className="mx-auto max-w-6xl px-4 py-8">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Data Checks</h1>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
          Scan and report on data quality (tbl_tracking).
        </p>

        {/* GPS tracking gaps */}
        <section className="mt-8 rounded-lg border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
            GPS tracking gaps
          </h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Find breaks in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> per
            device (ordered by device_name, position_time_nz). Report by date and device with largest gap in minutes.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">
                Gap threshold (minutes)
              </span>
              <input
                type="number"
                min={0}
                step={1}
                value={minGapMinutes}
                onChange={(e) => setMinGapMinutes(e.target.value)}
                className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                placeholder="60"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date from (optional)</span>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date to (optional)</span>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={runGapsScan}
              disabled={loading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {loading ? 'Scanning…' : 'Run scan'}
            </button>
          </div>

          {error && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {error}
            </div>
          )}

          {result && (
            <div className="mt-6">
              <div className="flex flex-wrap items-end justify-between gap-4">
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Gaps &gt; {result.minGapMinutes} min
                  {result.dateFrom || result.dateTo
                    ? ` (${result.dateFrom ?? '…'} to ${result.dateTo ?? '…'})`
                    : ''}
                  . Summary: {result.summary.length} date/device row(s).
                </p>
                <div className="flex flex-wrap items-center gap-4">
                  <label className="flex flex-col gap-1">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Filter by device</span>
                    <input
                      type="text"
                      value={deviceFilter}
                      onChange={(e) => setDeviceFilter(e.target.value)}
                      placeholder="Device name..."
                      className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    />
                  </label>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 1</span>
                    <select
                      value={sortBy1}
                      onChange={(e) => setSortBy1(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 2</span>
                    <select
                      value={sortBy2}
                      onChange={(e) => setSortBy2(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <select
                    value={sortDir}
                    onChange={(e) => setSortDir(e.target.value as 'asc' | 'desc')}
                    className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="asc">Ascending</option>
                    <option value="desc">Descending</option>
                  </select>
                </div>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400" title="List every gap above the threshold (up to 500 rows)">
                  <input
                    type="checkbox"
                    checked={showDetail}
                    onChange={(e) => setShowDetail(e.target.checked)}
                    className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
                  />
                  Show all gaps &gt; {result.minGapMinutes} min (up to 500)
                </label>
              </div>

              {(() => {
                const filtered = deviceFilter.trim()
                  ? result.summary.filter((r) =>
                      r.device_name.toLowerCase().includes(deviceFilter.trim().toLowerCase())
                    )
                  : result.summary;
                const sorted = sortSummary(filtered, sortBy1, sortBy2, sortDir);
                const displaySummary = sorted;
                return (
                  <>
              <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
                Click a row to load tbl_tracking data in a popup (5 min before From to 90 min after To). Red rows in popup = outage boundaries.
              </p>
              <div className="mt-4 overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[900px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300" title="Date from position_time (query and grouping use position_time)">Date</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        Max gap (min)
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        # gaps &gt; threshold
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right" title="Total tbl_tracking rows for this device on this date (reconcile with GPS import)">
                        DayCount
                      </th>
                      <th className="min-w-[5rem] px-3 py-2 text-center font-medium text-zinc-700 dark:text-zinc-300" title="!! = device moved between outage start and end (lat/lon at From ≠ at To); distance shown when moved">
                        !!
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From_NZ</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To_NZ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {displaySummary.length === 0 ? (
                      <tr>
                        <td colSpan={10} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          {result.summary.length === 0
                            ? 'No gaps above threshold in the selected range.'
                            : 'No rows match the device filter.'}
                        </td>
                      </tr>
                    ) : (
                      displaySummary.map((row, i) => (
                        <tr
                          key={`${row.report_date}-${row.device_name}-${i}`}
                          onClick={() => openTrackingPopup(row)}
                          className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-100 dark:hover:bg-zinc-800/70 ${row.gap_from != null && row.gap_to != null ? '' : 'opacity-70'}`}
                          title={row.gap_from != null && row.gap_to != null ? 'Click to view tbl_tracking data around this gap' : 'No position_time for this gap'}
                        >
                          <td className="px-3 py-2 font-mono text-zinc-900 dark:text-zinc-100">
                            {formatDateDDMMYY(row.report_date)}
                          </td>
                          <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.max_gap_minutes}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.gap_count}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200" title="Total rows for this device on this date (reconcile with GPS import)">
                            {row.day_count}
                          </td>
                          <td className="px-3 py-2 text-center font-bold text-amber-600 dark:text-amber-400" title={deviceMovedDuringOutage(row) ? 'Device moved between outage start and end' : 'Same position at start and end of gap'}>
                            {deviceMovedDuringOutage(row) && row.gap_start_lat != null && row.gap_start_lon != null && row.gap_end_lat != null && row.gap_end_lon != null
                              ? `!! ${formatDistance(distanceMeters(row.gap_start_lat, row.gap_start_lon, row.gap_end_lat, row.gap_end_lon))}`
                              : deviceMovedDuringOutage(row)
                                ? '!!'
                                : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time last read before gap">
                            {row.gap_from != null ? formatGapDateTime(row.gap_from) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time first read after gap">
                            {row.gap_to != null ? formatGapDateTime(row.gap_to) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz last read before gap">
                            {formatGapDateTime(row.gap_from_nz)}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz first read after gap">
                            {formatGapDateTime(row.gap_to_nz)}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
                  </>
                );
              })()}

              {popupRow && (
                <div
                  className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
                  role="dialog"
                  aria-modal="true"
                  aria-labelledby="popup-title"
                  onClick={(e) => e.target === e.currentTarget && setPopupRow(null)}
                >
                  <div
                    className="max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                      <h3 id="popup-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                        tbl_tracking: {popupRow.device_name} (5 min before From → 90 min after To)
                      </h3>
                      <button
                        type="button"
                        onClick={() => setPopupRow(null)}
                        className="rounded p-1 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700 dark:hover:bg-zinc-800 dark:hover:text-zinc-300"
                        aria-label="Close"
                      >
                        ✕
                      </button>
                    </div>
                    <div className="max-h-[70vh] overflow-auto p-4">
                      {popupLoading && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
                      )}
                      {popupError && (
                        <p className="text-sm text-red-600 dark:text-red-400">{popupError}</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length === 0 && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">No rows in this window.</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length > 0 && (
                        <table className="w-full min-w-[640px] border-collapse text-left text-sm">
                          <thead>
                            <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time_nz</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">fence_name</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">geofence_type</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lat</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lon</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-center">Map</th>
                              <th className="min-w-[4rem] px-2 py-1.5 text-center font-medium text-zinc-700 dark:text-zinc-300" title="Distance from previous row (→ moved, ● same position)">Move</th>
                            </tr>
                          </thead>
                          <tbody>
                            {popupRows.map((r, i) => {
                              const pt = (r.position_time ?? '').trim();
                              const isFrom = pt && popupRow.gap_from != null && pt === popupRow.gap_from.trim();
                              const isTo = pt && popupRow.gap_to != null && pt === popupRow.gap_to.trim();
                              const isOutageBoundary = isFrom || isTo;
                              const prev = i > 0 ? popupRows[i - 1] : null;
                              const sameAsAbove = prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null && prev.lat === r.lat && prev.lon === r.lon;
                              const moveSymbol = prev == null ? '—' : sameAsAbove ? '●' : '→';
                              const distM =
                                prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null
                                  ? distanceMeters(prev.lat, prev.lon, r.lat, r.lon)
                                  : null;
                              const distStr = distM != null ? formatDistance(distM) : '';
                              const moveTitle =
                                prev == null
                                  ? 'First row'
                                  : sameAsAbove
                                    ? `Stationary (0 m from row above)`
                                    : `Moved ${distStr} from row above`;
                              return (
                              <tr
                                key={i}
                                className={`border-b border-zinc-100 dark:border-zinc-800 ${isOutageBoundary ? 'bg-red-200 dark:bg-red-950/80' : ''}`}
                                title={isFrom ? 'Last read before gap (outage start)' : isTo ? 'First read after gap (outage end)' : undefined}
                              >
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time ?? '—'}
                                </td>
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time_nz ?? '—'}
                                </td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.fence_name ?? '—'}</td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.geofence_type ?? '—'}</td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lat != null ? r.lat : '—'}
                                </td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lon != null ? r.lon : '—'}
                                </td>
                                <td className="px-2 py-1 text-center">
                                  {r.lat != null && r.lon != null ? (
                                    <a
                                      href={`https://www.google.com/maps?q=${r.lat},${r.lon}`}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-blue-600 dark:text-blue-400 hover:underline"
                                      title={`Open ${r.lat}, ${r.lon} in Google Maps`}
                                    >
                                      Map
                                    </a>
                                  ) : (
                                    '—'
                                  )}
                                </td>
                                <td className="px-2 py-1 text-center text-sm font-mono text-xs" title={moveTitle}>
                                  {prev == null ? (
                                    '—'
                                  ) : (
                                    <>
                                      <span className={sameAsAbove ? 'text-zinc-400 dark:text-zinc-500' : 'text-emerald-600 dark:text-emerald-400'}>{moveSymbol}</span>
                                      {' '}
                                      <span className="text-zinc-600 dark:text-zinc-400">{distStr}</span>
                                    </>
                                  )}
                                </td>
                              </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {showDetail && result.detail.length > 0 && (
                <div className="mt-6">
                  <h3 className="text-sm font-medium text-zinc-700 dark:text-zinc-300">All gaps &gt; {result.minGapMinutes} min ({result.detail.length} shown)</h3>
                  <div className="mt-2 max-h-[400px] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                    <table className="w-full min-w-[520px] border-collapse text-left text-sm">
                      <thead className="sticky top-0 bg-zinc-100/95 dark:bg-zinc-800/95">
                        <tr className="border-b border-zinc-200 dark:border-zinc-700">
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap start</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap end</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                            Gap (min)
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.detail.map((row, i) => (
                          <tr
                            key={`${row.device_name}-${row.gap_start}-${i}`}
                            className="border-b border-zinc-100 dark:border-zinc-800"
                          >
                            <td className="px-3 py-1.5 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_start)}
                            </td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_end)}
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-zinc-800 dark:text-zinc-200">
                              {row.gap_minutes}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
