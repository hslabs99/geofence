'use client';

import { useCallback, useEffect, useState } from 'react';
import { haversineMeters } from '@/lib/haversine-meters';
import { formatColumnLabel, formatDateNZ } from '@/lib/utils';

function isIsoDateString(v: unknown): v is string {
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v);
}

/** Same column set as Inspect → GPS table (raw/entry_exit data rows). */
const TRACKING_DISPLAY_COLUMNS = ['device_name', 'fence_name', 'geofence_type', 'position_time_nz', 'position_time', 'lat', 'lon'] as const;

type GpsMapping = {
  id: number;
  type: string;
  vwname: string;
  gpsname: string;
};

type TrackingRow = {
  device_name: string;
  position_time: string | null;
  position_time_nz: string | null;
  fence_name: string | null;
  geofence_type: string | null;
  lat: number | null;
  lon: number | null;
};

type GeofenceRow = { fence_id: number; fence_name: string };

type Options = {
  fenceNames: string[];
  /** tbl_geofences rows (fence_id, fence_name); no duplicates */
  trackingFences?: GeofenceRow[];
  /** Count of tbl_tracking rows per fence_id */
  trackingCountByFenceId?: Record<number, number>;
  deliveryWineries: string[];
  vineyardNames: string[];
  /** Count of tbl_vworkjobs rows per vineyard_name (same key as vineyardNames entries) */
  vineyardJobCounts?: Record<string, number>;
};

const TYPE_OPTIONS = ['Winery', 'Vineyard'] as const;

export default function GpsMappingsPage() {
  const [rows, setRows] = useState<GpsMapping[]>([]);
  const [options, setOptions] = useState<Options | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [newRow, setNewRow] = useState({ type: '', vwname: '', gpsname: '' });
  const [saving, setSaving] = useState(false);
  /** Full-row edit: when set, this row shows inline dropdowns + Save/Cancel */
  const [editingRowId, setEditingRowId] = useState<number | null>(null);
  const [editRowValues, setEditRowValues] = useState<{ type: string; vwname: string; gpsname: string }>({ type: '', vwname: '', gpsname: '' });
  /** When true, GPS name dropdown shows only fences that are not linked and have data > 0 */
  const [gpsNameUnlinkedOnly, setGpsNameUnlinkedOnly] = useState(false);
  /** When true, tbl_geofences table shows only rows that are not Mapped/Direct and have count > 0 */
  const [geofencesFilterUnlinkedOnly, setGeofencesFilterUnlinkedOnly] = useState(false);
  /** Mappings table sort */
  const [mappingsSortKey, setMappingsSortKey] = useState<'id' | 'type' | 'vwname' | 'gpsname' | 'count' | null>(null);
  const [mappingsSortDir, setMappingsSortDir] = useState<'asc' | 'desc'>('asc');
  /** In-page tbl_tracking: when user clicks a data count, load rows here (no navigation) */
  const [trackingFence, setTrackingFence] = useState<{ fence_id: number; fence_name: string } | null>(null);
  const [trackingRows, setTrackingRows] = useState<TrackingRow[]>([]);
  const [trackingLoading, setTrackingLoading] = useState(false);
  const [trackingError, setTrackingError] = useState<string | null>(null);

  const formatGpsCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(String(v));
    if (typeof v === 'number' && !Number.isNaN(v)) return String(v);
    return String(v);
  }, []);

  const vwnameOptions = newRow.type === 'Winery'
    ? options?.deliveryWineries ?? []
    : newRow.type === 'Vineyard'
      ? options?.vineyardNames ?? []
      : [];

  // Resolvable names: tbl_geofences.fence_name (tbl_tracking) or tbl_gpsmappings.vwname/gpsname
  const resolvableSet = (() => {
    const set = new Set<string>();
    (options?.trackingFences ?? []).forEach((f) => {
      if (f.fence_name) set.add(f.fence_name.trim());
    });
    rows.forEach((r) => {
      if ((r.vwname ?? '').trim()) set.add(String(r.vwname).trim());
      if ((r.gpsname ?? '').trim()) set.add(String(r.gpsname).trim());
    });
    return set;
  })();
  const unmappedWineries = (options?.deliveryWineries ?? []).filter(
    (w) => w.trim() && !resolvableSet.has(w.trim())
  );
  const unmappedVineyards = (options?.vineyardNames ?? []).filter(
    (v) => v.trim() && !resolvableSet.has(v.trim())
  );

  // For tbl_geofences list: Mapped = in tbl_gpsmappings.gpsname; Direct = in vworkjobs delivery_winery/vineyard_name
  const gpsnameSet = new Set(
    rows.map((r) => (r.gpsname ?? '').trim()).filter(Boolean)
  );
  const vworkNameSet = new Set([
    ...(options?.deliveryWineries ?? []).map((s) => s.trim()).filter(Boolean),
    ...(options?.vineyardNames ?? []).map((s) => s.trim()).filter(Boolean),
  ]);
  const fenceTag = (fenceName: string) => {
    const t = fenceName.trim();
    if (gpsnameSet.has(t)) return 'Mapped' as const;
    if (vworkNameSet.has(t)) return 'Direct' as const;
    return null;
  };

  // GPS_Tracking: fence_names that are not Mapped/Direct but have tbl_tracking data (count > 0) — have data but no link
  const unlinkedTrackingFences = (() => {
    const list = (options?.trackingFences ?? [])
      .filter(
        (f) =>
          f.fence_name &&
          !fenceTag(f.fence_name) &&
          (options?.trackingCountByFenceId?.[f.fence_id] ?? 0) > 0
      )
      .map((f) => ({
        fence_id: f.fence_id,
        fence_name: f.fence_name!,
        count: options?.trackingCountByFenceId?.[f.fence_id] ?? 0,
      }));
    list.sort((a, b) => a.fence_name.localeCompare(b.fence_name));
    return list;
  })();

  const openTrackingForFence = (fence_id: number, fence_name: string) => {
    setTrackingFence({ fence_id, fence_name });
    setTrackingError(null);
    setTrackingRows([]);
    setTrackingLoading(true);
    fetch(`/api/gpsmappings/tracking-by-fence?fenceId=${encodeURIComponent(fence_id)}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: TrackingRow[] }) => {
        setTrackingRows(data.rows ?? []);
      })
      .catch((e) => {
        setTrackingError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setTrackingLoading(false));
  };

  // For "Add mapping" form: all GPS fence names, or only unlinked with data > 0
  const allGpsFenceNames = (() => {
    const set = new Set<string>();
    (options?.fenceNames ?? []).forEach((n) => set.add(n));
    (options?.trackingFences ?? []).forEach((f) => f.fence_name && set.add(f.fence_name));
    return [...set].sort((a, b) => a.localeCompare(b));
  })();
  const gpsnameOptionsForAdd = gpsNameUnlinkedOnly
    ? unlinkedTrackingFences.map((f) => f.fence_name)
    : allGpsFenceNames;

  /** tbl_tracking count for the GPS fence name (resolve gpsname → fence_id via tbl_geofences). */
  const getTrackingCountForRow = (r: GpsMapping): number => {
    const name = (r.gpsname ?? '').trim();
    if (!name || !options?.trackingFences?.length) return 0;
    const fence = options.trackingFences.find((f) => (f.fence_name ?? '').trim() === name);
    if (!fence) return 0;
    return options.trackingCountByFenceId?.[fence.fence_id] ?? 0;
  };

  const sortedRows = (() => {
    if (!mappingsSortKey) return rows;
    const dir = mappingsSortDir === 'asc' ? 1 : -1;
    return [...rows].sort((a, b) => {
      let cmp = 0;
      switch (mappingsSortKey) {
        case 'id':
          cmp = a.id - b.id;
          break;
        case 'type':
          cmp = (a.type ?? '').localeCompare(b.type ?? '');
          break;
        case 'vwname':
          cmp = (a.vwname ?? '').localeCompare(b.vwname ?? '');
          break;
        case 'gpsname':
          cmp = (a.gpsname ?? '').localeCompare(b.gpsname ?? '');
          break;
        case 'count':
          cmp = getTrackingCountForRow(a) - getTrackingCountForRow(b);
          break;
        default:
          return 0;
      }
      return cmp * dir;
    });
  })();

  const toggleSort = (key: typeof mappingsSortKey) => {
    if (key == null) return;
    if (mappingsSortKey === key) {
      setMappingsSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setMappingsSortKey(key);
      setMappingsSortDir('asc');
    }
  };

  // tbl_geofences table: optionally filter to only not Mapped/Direct and count > 0
  const displayedGeofences =
    geofencesFilterUnlinkedOnly
      ? (options?.trackingFences ?? []).filter(
          (f) =>
            f.fence_name &&
            !fenceTag(f.fence_name) &&
            (options?.trackingCountByFenceId?.[f.fence_id] ?? 0) > 0
        )
      : (options?.trackingFences ?? []);

  const load = () => {
    setLoading(true);
    Promise.all([fetch('/api/gpsmappings'), fetch('/api/gpsmappings/options')])
      .then(async ([r1, r2]) => {
        const data1 = await r1.json();
        const data2 = await r2.json();
        if (data1.error) throw new Error(data1.error);
        if (data2.error) throw new Error(data2.error);
        setRows(data1.rows ?? []);
        setOptions(data2);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => load(), []);

  const getVwnameOptionsForRow = (row: GpsMapping) =>
    row.type === 'Winery'
      ? options?.deliveryWineries ?? []
      : row.type === 'Vineyard'
        ? options?.vineyardNames ?? []
        : [];

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newRow.type.trim() || !newRow.vwname.trim() || !newRow.gpsname.trim()) {
      setError('All fields required');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const res = await fetch('/api/gpsmappings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newRow),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setNewRow({ type: '', vwname: '', gpsname: '' });
      setShowAdd(false);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Create failed');
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (r: GpsMapping) => {
    if (!confirm(`Delete mapping ${r.vwname} → ${r.gpsname}?`)) return;
    setSaving(true);
    try {
      const res = await fetch(`/api/gpsmappings/${r.id}`, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setRows((prev) => prev.filter((x) => x.id !== r.id));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Delete failed');
    } finally {
      setSaving(false);
    }
  };

  const startRowEdit = (r: GpsMapping) => {
    setEditingRowId(r.id);
    setEditRowValues({ type: r.type, vwname: r.vwname ?? '', gpsname: r.gpsname ?? '' });
  };

  const cancelRowEdit = () => {
    setEditingRowId(null);
    setEditRowValues({ type: '', vwname: '', gpsname: '' });
  };

  const saveRowEdit = async () => {
    if (editingRowId == null) return;
    setSaving(true);
    try {
      const res = await fetch(`/api/gpsmappings/${editingRowId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(editRowValues),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setRows((prev) =>
        prev.map((r) =>
          r.id === editingRowId
            ? { ...r, type: editRowValues.type, vwname: editRowValues.vwname, gpsname: editRowValues.gpsname }
            : r
        )
      );
      cancelRowEdit();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Update failed');
    } finally {
      setSaving(false);
    }
  };

  const handleClone = (r: GpsMapping) => {
    setNewRow({ type: r.type, vwname: r.vwname ?? '', gpsname: r.gpsname ?? '' });
    setShowAdd(true);
    cancelRowEdit();
  };

  /** Start Add mapping with Type=Winery and this delivery winery selected. */
  const startAddWithWinery = (name: string) => {
    setNewRow({ type: 'Winery', vwname: name.trim(), gpsname: '' });
    setShowAdd(true);
    cancelRowEdit();
  };

  /** Start Add mapping with Type=Vineyard and this vineyard selected. */
  const startAddWithVineyard = (name: string) => {
    setNewRow({ type: 'Vineyard', vwname: name.trim(), gpsname: '' });
    setShowAdd(true);
    cancelRowEdit();
  };

  const editRowVwnameOptions =
    editRowValues.type === 'Winery'
      ? options?.deliveryWineries ?? []
      : editRowValues.type === 'Vineyard'
        ? options?.vineyardNames ?? []
        : [];

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">GPS Mappings</h1>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-zinc-600">Loading…</p>
      ) : (
        <div className="flex flex-col gap-4 lg:flex-row lg:flex-wrap lg:items-start">
          {/* Left: Mappings table + Add row above it */}
          <div className="min-w-0 flex-1 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:max-w-xl">
            <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-700">
              <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Mappings</h2>
              <button
                type="button"
                onClick={() => {
                  setShowAdd(!showAdd);
                  cancelRowEdit();
                }}
                className="rounded bg-blue-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-blue-700"
              >
                {showAdd ? 'Cancel' : 'Add row'}
              </button>
            </div>
            {showAdd && (
              <form
                onSubmit={handleAdd}
                className="flex flex-wrap items-end gap-3 border-b border-zinc-200 p-3 dark:border-zinc-700 dark:bg-zinc-800/30"
              >
                <div>
                  <label className="mb-0.5 block text-xs font-medium text-zinc-500">Type</label>
                  <select
                    value={newRow.type}
                    onChange={(e) => setNewRow((n) => ({ ...n, type: e.target.value, vwname: '' }))}
                    required
                    className="w-24 rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">Select…</option>
                    {TYPE_OPTIONS.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="min-w-[160px]">
                  <label className="mb-0.5 block text-xs font-medium text-zinc-500">
                    VW name ({newRow.type || 'pick type first'})
                  </label>
                  <select
                    value={newRow.vwname}
                    onChange={(e) => setNewRow((n) => ({ ...n, vwname: e.target.value }))}
                    required
                    disabled={!newRow.type}
                    className="w-full rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 disabled:opacity-60"
                  >
                    <option value="">Select…</option>
                    {vwnameOptions.map((v) => (
                      <option key={v} value={v}>
                        {v}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="min-w-[180px]">
                  <label className="mb-0.5 block text-xs font-medium text-zinc-500">GPS fence name</label>
                  <label className="mb-0.5 flex items-center gap-1.5 text-xs text-zinc-500 dark:text-zinc-400">
                    <input
                      type="checkbox"
                      checked={gpsNameUnlinkedOnly}
                      onChange={(e) => setGpsNameUnlinkedOnly(e.target.checked)}
                      className="rounded border-zinc-300 dark:border-zinc-600"
                    />
                    Only: not linked, data &gt; 0
                  </label>
                  <select
                    value={newRow.gpsname}
                    onChange={(e) => setNewRow((n) => ({ ...n, gpsname: e.target.value }))}
                    required
                    className="w-full rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">Select…</option>
                    {gpsnameOptionsForAdd.map((v) => (
                      <option key={v} value={v}>
                        {v}
                      </option>
                    ))}
                    {newRow.gpsname && !gpsnameOptionsForAdd.includes(newRow.gpsname) && (
                      <option value={newRow.gpsname}>{newRow.gpsname}</option>
                    )}
                  </select>
                </div>
                <button
                  type="submit"
                  disabled={saving}
                  className="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                >
                  Save
                </button>
              </form>
            )}
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-xs">
                <colgroup>
                  <col className="w-12" />
                  <col className="w-20" />
                  <col className="min-w-[9.5rem]" />
                  <col className="min-w-[10rem]" />
                  <col className="w-14" />
                  <col className="min-w-[6rem]" />
                </colgroup>
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                    <th
                      className="cursor-pointer select-none px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 hover:bg-zinc-200 dark:hover:bg-zinc-700"
                      onClick={() => toggleSort('id')}
                      title="Sort by ID"
                    >
                      ID{mappingsSortKey === 'id' ? (mappingsSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                    <th
                      className="cursor-pointer select-none px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 hover:bg-zinc-200 dark:hover:bg-zinc-700"
                      onClick={() => toggleSort('type')}
                      title="Sort by Type"
                    >
                      Type{mappingsSortKey === 'type' ? (mappingsSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                    <th
                      className="cursor-pointer select-none px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 hover:bg-zinc-200 dark:hover:bg-zinc-700"
                      onClick={() => toggleSort('vwname')}
                      title="Sort by VW name"
                    >
                      VW name{mappingsSortKey === 'vwname' ? (mappingsSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                    <th
                      className="cursor-pointer select-none min-w-[10rem] px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 hover:bg-zinc-200 dark:hover:bg-zinc-700"
                      onClick={() => toggleSort('gpsname')}
                      title="Sort by GPS fence"
                    >
                      GPS fence{mappingsSortKey === 'gpsname' ? (mappingsSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                    <th
                      className="cursor-pointer select-none px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 hover:bg-zinc-200 dark:hover:bg-zinc-700"
                      onClick={() => toggleSort('count')}
                      title="Sort by tbl_tracking count"
                    >
                      Count{mappingsSortKey === 'count' ? (mappingsSortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.map((r) => (
                    <tr
                      key={r.id}
                      className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                    >
                      <td className="whitespace-nowrap px-2 py-1.5 text-zinc-500 dark:text-zinc-400">{r.id}</td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-zinc-700 dark:text-zinc-300">{r.type}</td>
                      <td className="max-w-[9.5rem] truncate px-2 py-1.5 text-zinc-700 dark:text-zinc-300" title={r.vwname}>
                        {r.vwname}
                      </td>
                      <td className="max-w-[10rem] truncate px-2 py-1.5 text-zinc-700 dark:text-zinc-300" title={r.gpsname}>
                        {r.gpsname}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-zinc-500 dark:text-zinc-400 tabular-nums" title="Click to load tbl_tracking rows below">
                        {(() => {
                          const count = getTrackingCountForRow(r);
                          const fence = (r.gpsname ?? '').trim() && options?.trackingFences
                            ? options.trackingFences.find((f) => (f.fence_name ?? '').trim() === (r.gpsname ?? '').trim())
                            : null;
                          if (count > 0 && fence) {
                            return (
                              <button
                                type="button"
                                onClick={() => openTrackingForFence(fence.fence_id, fence.fence_name ?? r.gpsname ?? '')}
                                className="text-blue-600 underline hover:no-underline dark:text-blue-400"
                              >
                                {count}
                              </button>
                            );
                          }
                          return count;
                        })()}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5">
                        <button
                          type="button"
                          onClick={() => startRowEdit(r)}
                          disabled={saving}
                          className="text-blue-600 hover:underline disabled:opacity-50 dark:text-blue-400"
                        >
                          Edit
                        </button>
                        <span className="mx-1 text-zinc-300 dark:text-zinc-600">|</span>
                        <button
                          type="button"
                          onClick={() => handleClone(r)}
                          disabled={saving}
                          className="text-zinc-600 hover:underline disabled:opacity-50 dark:text-zinc-400"
                        >
                          Clone
                        </button>
                        <span className="mx-1 text-zinc-300 dark:text-zinc-600">|</span>
                        <button
                          type="button"
                          onClick={() => handleDelete(r)}
                          disabled={saving}
                          className="text-red-600 hover:underline disabled:opacity-50 dark:text-red-400"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {rows.length === 0 && <p className="p-3 text-zinc-500 text-xs">No rows yet. Add one above.</p>}

            {editingRowId != null && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
                onClick={(e) => e.target === e.currentTarget && cancelRowEdit()}
                role="dialog"
                aria-modal="true"
                aria-labelledby="edit-mapping-title"
              >
                <div
                  className="w-full max-w-lg rounded-lg border border-zinc-200 bg-white p-6 shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h2 id="edit-mapping-title" className="mb-4 text-lg font-semibold text-zinc-900 dark:text-zinc-100">
                    Edit mapping
                  </h2>
                  <div className="flex flex-col gap-4">
                    <div>
                      <label className="mb-1 block text-sm font-medium text-zinc-700 dark:text-zinc-300">Type</label>
                      <select
                        value={editRowValues.type}
                        onChange={(e) => setEditRowValues((v) => ({ ...v, type: e.target.value, vwname: '' }))}
                        className="w-full min-w-0 rounded border border-zinc-300 px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                      >
                        {TYPE_OPTIONS.map((t) => (
                          <option key={t} value={t}>
                            {t}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="mb-1 block text-sm font-medium text-zinc-700 dark:text-zinc-300">VW name</label>
                      <select
                        value={editRowValues.vwname}
                        onChange={(e) => setEditRowValues((v) => ({ ...v, vwname: e.target.value }))}
                        disabled={!editRowValues.type}
                        className="w-full min-w-0 rounded border border-zinc-300 px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 disabled:opacity-60"
                      >
                        <option value="">Select…</option>
                        {editRowVwnameOptions.map((v) => (
                          <option key={v} value={v}>
                            {v}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="mb-1 block text-sm font-medium text-zinc-700 dark:text-zinc-300">GPS fence name</label>
                      <select
                        value={editRowValues.gpsname}
                        onChange={(e) => setEditRowValues((v) => ({ ...v, gpsname: e.target.value }))}
                        className="w-full min-w-0 rounded border border-zinc-300 px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                      >
                        <option value="">Select…</option>
                        {(options?.fenceNames ?? []).map((v) => (
                          <option key={v} value={v}>
                            {v}
                          </option>
                        ))}
                        {editRowValues.gpsname && !(options?.fenceNames ?? []).includes(editRowValues.gpsname) && (
                          <option value={editRowValues.gpsname}>{editRowValues.gpsname}</option>
                        )}
                      </select>
                    </div>
                    <div className="flex justify-end gap-2 pt-2">
                      <button
                        type="button"
                        onClick={cancelRowEdit}
                        className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                      >
                        Cancel
                      </button>
                      <button
                        type="button"
                        onClick={saveRowEdit}
                        disabled={saving}
                        className="rounded bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50 dark:bg-emerald-700 dark:hover:bg-emerald-600"
                      >
                        {saving ? 'Saving…' : 'Save'}
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Right: Mapping Issues (temp – no DB, computed from vworkjobs vs tracking/mappings) */}
          <div className="min-w-0 flex-shrink-0 rounded-lg border border-amber-200 bg-amber-50/80 dark:border-amber-800 dark:bg-amber-900/20 lg:w-80">
            <h2 className="border-b border-amber-200 px-3 py-2 text-sm font-semibold text-amber-900 dark:border-amber-800 dark:text-amber-200">
              Mapping Issues
            </h2>
            <p className="px-3 py-1.5 text-xs text-zinc-600 dark:text-zinc-400">
              Names in <code className="rounded bg-zinc-200 px-0.5 dark:bg-zinc-700">tbl_vworkjobs</code> with no match in{' '}
              <code className="rounded bg-zinc-200 px-0.5 dark:bg-zinc-700">tbl_geofences.fence_name</code> or{' '}
              <code className="rounded bg-zinc-200 px-0.5 dark:bg-zinc-700">tbl_gpsmappings</code> (vwname/gpsname).
            </p>
            <div className="space-y-3 p-3">
              <div>
                <h3 className="mb-1 text-xs font-medium text-amber-800 dark:text-amber-300">
                  Wineries (delivery_winery)
                </h3>
                {unmappedWineries.length === 0 ? (
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">None</p>
                ) : (
                  <ul className="max-h-40 overflow-y-auto text-xs text-zinc-700 dark:text-zinc-300">
                    {unmappedWineries.map((name) => (
                      <li key={name} className="truncate py-0.5" title={name}>
                        {name}
                      </li>
                    ))}
                  </ul>
                )}
                {unmappedWineries.length > 0 && (
                  <p className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">
                    {unmappedWineries.length} unmapped
                  </p>
                )}
              </div>
              <div>
                <h3 className="mb-1 text-xs font-medium text-amber-800 dark:text-amber-300">
                  Vineyards (vineyard_name)
                </h3>
                {unmappedVineyards.length === 0 ? (
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">None</p>
                ) : (
                  <ul className="max-h-40 overflow-y-auto text-xs text-zinc-700 dark:text-zinc-300">
                    {unmappedVineyards.map((name) => (
                      <li key={name} className="truncate py-0.5" title={name}>
                        {name}
                      </li>
                    ))}
                  </ul>
                )}
                {unmappedVineyards.length > 0 && (
                  <p className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">
                    {unmappedVineyards.length} unmapped
                  </p>
                )}
              </div>
              <div>
                <h3 className="mb-1 text-xs font-medium text-amber-800 dark:text-amber-300">
                  GPS_Tracking
                </h3>
                <p className="mb-1 text-xs text-zinc-500 dark:text-zinc-400">
                  fence_names with data (count &gt; 0) but not Mapped or Direct — not linked
                </p>
                {unlinkedTrackingFences.length === 0 ? (
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">None</p>
                ) : (
                  <ul className="max-h-40 overflow-y-auto text-xs text-zinc-700 dark:text-zinc-300">
                    {unlinkedTrackingFences.map(({ fence_id, fence_name, count }) => (
                      <li key={fence_name} className="flex items-baseline justify-between gap-2 py-0.5" title={fence_name}>
                        <span className="truncate min-w-0">{fence_name}</span>
                        <button
                          type="button"
                          onClick={() => openTrackingForFence(fence_id, fence_name)}
                          className="flex-shrink-0 tabular-nums text-blue-600 underline hover:no-underline dark:text-blue-400"
                        >
                          {count}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
                {unlinkedTrackingFences.length > 0 && (
                  <p className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">
                    {unlinkedTrackingFences.length} with data, not linked
                  </p>
                )}
              </div>
            </div>
          </div>

          {/* Distinct delivery_winery from tbl_vworkjobs */}
          <div className="min-w-0 flex-shrink-0 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:w-56">
            <h2 className="border-b border-zinc-200 px-3 py-2 text-sm font-semibold text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">
              Distinct delivery_winery
            </h2>
            <p className="px-3 py-1 text-xs text-zinc-500 dark:text-zinc-400">
              <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">tbl_vworkjobs</code>
            </p>
            <ul className="max-h-[32rem] overflow-y-auto px-3 pb-3 text-xs text-zinc-700 dark:text-zinc-300">
              {(options?.deliveryWineries ?? []).map((name) => (
                <li key={name} className="flex items-baseline gap-1 py-0.5 min-w-0" title={`Click to add mapping: ${name}`}>
                  <button
                    type="button"
                    onClick={() => startAddWithWinery(name)}
                    className="truncate min-w-0 text-left underline hover:no-underline cursor-pointer text-inherit"
                  >
                    {name}
                  </button>
                  {resolvableSet.has(name.trim()) && (
                    <span className="flex-shrink-0 text-emerald-600 dark:text-emerald-400">(Mapped)</span>
                  )}
                </li>
              ))}
            </ul>
            {(options?.deliveryWineries ?? []).length === 0 && (
              <p className="px-3 pb-3 text-xs text-zinc-500 dark:text-zinc-400">None</p>
            )}
          </div>

          {/* Distinct vineyard_name from tbl_vworkjobs (+50% width vs delivery_winery column) */}
          <div className="min-w-0 flex-shrink-0 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:w-[21rem]">
            <h2 className="border-b border-zinc-200 px-3 py-2 text-sm font-semibold text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">
              Distinct vineyard_name
            </h2>
            <p className="px-3 py-1 text-xs text-zinc-500 dark:text-zinc-400">
              <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">tbl_vworkjobs</code>
              {' '}
              <span className="text-zinc-400 dark:text-zinc-500">(n) = job count</span>
            </p>
            <ul className="max-h-[64rem] overflow-y-auto px-3 pb-3 text-xs text-zinc-700 dark:text-zinc-300">
              {(options?.vineyardNames ?? []).map((name) => (
                <li key={name} className="flex flex-wrap items-baseline gap-x-1 gap-y-0 py-0.5 min-w-0" title={`Click to add mapping: ${name}`}>
                  <button
                    type="button"
                    onClick={() => startAddWithVineyard(name)}
                    className="min-w-0 text-left underline hover:no-underline cursor-pointer text-inherit break-words"
                  >
                    {name}
                    <span className="whitespace-nowrap font-normal text-zinc-500 dark:text-zinc-400">
                      {' '}
                      ({options?.vineyardJobCounts?.[name] ?? '—'})
                    </span>
                  </button>
                  {resolvableSet.has(name.trim()) && (
                    <span className="flex-shrink-0 text-emerald-600 dark:text-emerald-400">(Mapped)</span>
                  )}
                </li>
              ))}
            </ul>
            {(options?.vineyardNames ?? []).length === 0 && (
              <p className="px-3 pb-3 text-xs text-zinc-500 dark:text-zinc-400">None</p>
            )}
          </div>

          {/* tbl_geofences: fence_id, fence_name; Mapped/Direct/DataCount; filter to unlinked + data > 0 */}
          <div className="min-w-0 flex-shrink-0 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:w-[28rem]">
            <h2 className="border-b border-zinc-200 px-3 py-2 text-sm font-semibold text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">
              tbl_geofences
            </h2>
            <p className="px-3 py-1 text-xs text-zinc-500 dark:text-zinc-400">
              <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">fence_id</code>, <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">fence_name</code>. Mapped = in gpsname; Direct = in vworkjobs; DataCount = tbl_tracking rows.
            </p>
            <label className="mx-3 mb-1 flex items-center gap-2 border-b border-zinc-200 pb-2 text-xs dark:border-zinc-700">
              <input
                type="checkbox"
                checked={geofencesFilterUnlinkedOnly}
                onChange={(e) => setGeofencesFilterUnlinkedOnly(e.target.checked)}
                className="rounded border-zinc-300 dark:border-zinc-600"
              />
              Only: not Mapped/Direct, count &gt; 0
            </label>
            <div className="max-h-[48rem] overflow-y-auto">
              <table className="min-w-full text-left text-xs">
                <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">fence_id</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">fence_name</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">Mapped</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100 text-right">DataCount</th>
                  </tr>
                </thead>
                <tbody className="text-zinc-700 dark:text-zinc-300">
                  {displayedGeofences.map((f) => {
                    const tag = f.fence_name ? fenceTag(f.fence_name) : null;
                    const dataCount = options?.trackingCountByFenceId?.[f.fence_id] ?? 0;
                    return (
                      <tr key={f.fence_id} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                        <td className="whitespace-nowrap px-2 py-1.5 tabular-nums text-zinc-500 dark:text-zinc-400">{f.fence_id}</td>
                        <td className="max-w-[12rem] truncate px-2 py-1.5" title={f.fence_name}>{f.fence_name || '—'}</td>
                        <td className="whitespace-nowrap px-2 py-1.5">
                          {tag === 'Mapped' && <span className="text-emerald-600 dark:text-emerald-400">Mapped</span>}
                          {tag === 'Direct' && <span className="text-blue-600 dark:text-blue-400">Direct</span>}
                          {!tag && <span className="text-zinc-400 dark:text-zinc-500">—</span>}
                        </td>
                        <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums">
                          <button
                            type="button"
                            onClick={() => openTrackingForFence(f.fence_id, f.fence_name ?? '')}
                            className="text-blue-600 underline hover:no-underline dark:text-blue-400"
                            title="Load tbl_tracking rows in table below"
                          >
                            {dataCount}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {displayedGeofences.length === 0 && (
              <p className="p-3 text-xs text-zinc-500 dark:text-zinc-400">
                {geofencesFilterUnlinkedOnly ? 'No fences with data and no mapping.' : 'None'}
              </p>
            )}
          </div>

          {/* In-page tbl_tracking table (when a data count was clicked) */}
          {trackingFence && (
            <div className="mt-6 w-full rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-700">
                <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                  tbl_tracking: {trackingFence.fence_name} (fence_id {trackingFence.fence_id})
                </h2>
                <button
                  type="button"
                  onClick={() => {
                    setTrackingFence(null);
                    setTrackingRows([]);
                    setTrackingError(null);
                  }}
                  className="rounded border border-zinc-300 px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:text-zinc-300 dark:hover:bg-zinc-800"
                >
                  Close
                </button>
              </div>
              {trackingLoading && (
                <p className="p-3 text-xs text-zinc-500 dark:text-zinc-400">Loading…</p>
              )}
              {trackingError && (
                <p className="p-3 text-sm text-red-600 dark:text-red-400">{trackingError}</p>
              )}
              {!trackingLoading && !trackingError && trackingRows.length === 0 && (
                <p className="p-3 text-xs text-zinc-500 dark:text-zinc-400">No rows</p>
              )}
              {!trackingLoading && !trackingError && trackingRows.length > 0 && (
                <div className="overflow-x-auto max-h-[28rem] overflow-y-auto">
                  <table className="min-w-full border-collapse text-left text-xs">
                    <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
                      <tr className="border-b border-zinc-200 dark:border-zinc-700">
                        {TRACKING_DISPLAY_COLUMNS.map((col) => (
                          <th key={col} className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">
                            {formatColumnLabel(col)}
                          </th>
                        ))}
                        <th className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">Distance (m)</th>
                        <th className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">Map</th>
                      </tr>
                    </thead>
                    <tbody className="text-zinc-700 dark:text-zinc-300">
                      {trackingRows.map((r, i) => {
                        const rec = r as Record<string, unknown>;
                        const lat = r.lat != null ? Number(r.lat) : NaN;
                        const lon = r.lon != null ? Number(r.lon) : NaN;
                        const hasCoords = !Number.isNaN(lat) && !Number.isNaN(lon);
                        const prev = i > 0 ? trackingRows[i - 1] : null;
                        const prevLat = prev && prev.lat != null ? Number(prev.lat) : NaN;
                        const prevLon = prev && prev.lon != null ? Number(prev.lon) : NaN;
                        const distanceM =
                          prev && Number.isFinite(prevLat) && Number.isFinite(prevLon) && hasCoords
                            ? haversineMeters(prevLat, prevLon, lat, lon)
                            : null;
                        return (
                          <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                            {TRACKING_DISPLAY_COLUMNS.map((col) => (
                              <td key={col} className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400">
                                {formatGpsCell(rec[col])}
                              </td>
                            ))}
                            <td className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400 tabular-nums">
                              {distanceM != null ? `${Math.round(distanceM)}` : '—'}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1.5">
                              {hasCoords ? (
                                <a
                                  href={`https://www.google.com/maps?q=${lat},${lon}`}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                                >
                                  View
                                </a>
                              ) : (
                                <span className="text-zinc-400">—</span>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
