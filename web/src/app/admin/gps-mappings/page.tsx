'use client';

import { useEffect, useState } from 'react';

type GpsMapping = {
  id: number;
  type: string;
  vwname: string;
  gpsname: string;
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
};

const TYPE_OPTIONS = ['Winery', 'Vineyard'] as const;

export default function GpsMappingsPage() {
  const [rows, setRows] = useState<GpsMapping[]>([]);
  const [options, setOptions] = useState<Options | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ id: number; field: 'type' | 'vwname' | 'gpsname' } | null>(null);
  const [editValue, setEditValue] = useState('');
  const [showAdd, setShowAdd] = useState(false);
  const [newRow, setNewRow] = useState({ type: '', vwname: '', gpsname: '' });
  const [saving, setSaving] = useState(false);
  /** When true, GPS name dropdown shows only fences that are not linked and have data > 0 */
  const [gpsNameUnlinkedOnly, setGpsNameUnlinkedOnly] = useState(false);
  /** When true, tbl_geofences table shows only rows that are not Mapped/Direct and have count > 0 */
  const [geofencesFilterUnlinkedOnly, setGeofencesFilterUnlinkedOnly] = useState(false);

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
      .map((f) => ({ fence_name: f.fence_name!, count: options?.trackingCountByFenceId?.[f.fence_id] ?? 0 }));
    list.sort((a, b) => a.fence_name.localeCompare(b.fence_name));
    return list;
  })();

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

  const startEdit = (r: GpsMapping, field: 'type' | 'vwname' | 'gpsname') => {
    setEditing({ id: r.id, field });
    setEditValue(r[field] ?? '');
  };

  const cancelEdit = () => {
    setEditing(null);
    setEditValue('');
  };

  const saveEdit = async () => {
    if (!editing) return;
    setSaving(true);
    try {
      const res = await fetch(`/api/gpsmappings/${editing.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [editing.field]: editValue }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setRows((prev) =>
        prev.map((r) =>
          r.id === editing.id ? { ...r, [editing.field]: editValue.trim() } : r
        )
      );
      cancelEdit();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Update failed');
    } finally {
      setSaving(false);
    }
  };

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

  const renderEditCell = (
    r: GpsMapping,
    field: 'type' | 'vwname' | 'gpsname',
    opts: { options: string[]; placeholder?: string }
  ) => {
    if (editing?.id !== r.id || editing.field !== field) return null;
    return (
      <select
        value={editValue}
        onChange={(e) => setEditValue(e.target.value)}
        onBlur={saveEdit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') saveEdit();
          if (e.key === 'Escape') cancelEdit();
        }}
        autoFocus
        className="min-w-[140px] rounded border border-blue-500 px-1.5 py-0.5 text-sm dark:bg-zinc-800 dark:text-zinc-100"
      >
        <option value="">{opts.placeholder ?? 'Select…'}</option>
        {opts.options.map((v) => (
          <option key={v} value={v}>
            {v}
          </option>
        ))}
        {!opts.options.includes(editValue) && editValue && (
          <option value={editValue}>{editValue}</option>
        )}
      </select>
    );
  };

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">GPS Mappings</h1>
        <button
          type="button"
          onClick={() => setShowAdd(!showAdd)}
          className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
        >
          {showAdd ? 'Cancel' : 'Add row'}
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          {error}
        </div>
      )}

      {showAdd && (
        <form
          onSubmit={handleAdd}
          className="mb-4 flex flex-wrap items-end gap-4 rounded-lg border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-700 dark:bg-zinc-800/50"
        >
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-500">Type</label>
            <select
              value={newRow.type}
              onChange={(e) => setNewRow((n) => ({ ...n, type: e.target.value, vwname: '' }))}
              required
              className="w-28 rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            >
              <option value="">Select…</option>
              {TYPE_OPTIONS.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div className="min-w-[200px]">
            <label className="mb-1 block text-xs font-medium text-zinc-500">
              VW name ({newRow.type || 'pick type first'})
            </label>
            <select
              value={newRow.vwname}
              onChange={(e) => setNewRow((n) => ({ ...n, vwname: e.target.value }))}
              required
              disabled={!newRow.type}
              className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 disabled:opacity-60"
            >
              <option value="">Select…</option>
              {vwnameOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </div>
          <div className="min-w-[220px]">
            <label className="mb-1 block text-xs font-medium text-zinc-500">GPS fence name</label>
            <label className="mb-1.5 flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
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
              className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
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
            className="rounded bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            Save
          </button>
        </form>
      )}

      {loading ? (
        <p className="text-zinc-600">Loading…</p>
      ) : (
        <div className="flex flex-col gap-4 lg:flex-row lg:flex-wrap lg:items-start">
          {/* Left: compact tbl_gpsmappings */}
          <div className="min-w-0 flex-1 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:max-w-xl">
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">ID</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">VW name</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">GPS fence</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr
                      key={r.id}
                      className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                    >
                      <td className="whitespace-nowrap px-2 py-1.5 text-zinc-500 dark:text-zinc-400">{r.id}</td>
                      <td
                        className="cursor-pointer px-2 py-1.5 text-zinc-700 dark:text-zinc-300"
                        onClick={() => startEdit(r, 'type')}
                      >
                        {renderEditCell(r, 'type', { options: [...TYPE_OPTIONS] }) ?? r.type}
                      </td>
                      <td
                        className="max-w-[120px] truncate cursor-pointer px-2 py-1.5 text-zinc-700 dark:text-zinc-300"
                        title={r.vwname}
                        onClick={() => startEdit(r, 'vwname')}
                      >
                        {renderEditCell(r, 'vwname', {
                          options: getVwnameOptionsForRow(r),
                          placeholder: r.type ? 'Select…' : 'Set type first',
                        }) ?? r.vwname}
                      </td>
                      <td
                        className="max-w-[120px] truncate cursor-pointer px-2 py-1.5 text-zinc-700 dark:text-zinc-300"
                        title={r.gpsname}
                        onClick={() => startEdit(r, 'gpsname')}
                      >
                        {renderEditCell(r, 'gpsname', {
                          options: options?.fenceNames ?? [],
                        }) ?? r.gpsname}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5">
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
                    {unlinkedTrackingFences.map(({ fence_name, count }) => (
                      <li key={fence_name} className="flex items-baseline justify-between gap-2 py-0.5" title={fence_name}>
                        <span className="truncate min-w-0">{fence_name}</span>
                        <span className="flex-shrink-0 tabular-nums text-zinc-500 dark:text-zinc-400">{count}</span>
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
            <ul className="max-h-64 overflow-y-auto px-3 pb-3 text-xs text-zinc-700 dark:text-zinc-300">
              {(options?.deliveryWineries ?? []).map((name) => (
                <li key={name} className="flex items-baseline gap-1 py-0.5 min-w-0" title={name}>
                  <span className="truncate min-w-0">{name}</span>
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

          {/* Distinct vineyard_name from tbl_vworkjobs */}
          <div className="min-w-0 flex-shrink-0 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900 lg:w-56">
            <h2 className="border-b border-zinc-200 px-3 py-2 text-sm font-semibold text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">
              Distinct vineyard_name
            </h2>
            <p className="px-3 py-1 text-xs text-zinc-500 dark:text-zinc-400">
              <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">tbl_vworkjobs</code>
            </p>
            <ul className="max-h-64 overflow-y-auto px-3 pb-3 text-xs text-zinc-700 dark:text-zinc-300">
              {(options?.vineyardNames ?? []).map((name) => (
                <li key={name} className="flex items-baseline gap-1 py-0.5 min-w-0" title={name}>
                  <span className="truncate min-w-0">{name}</span>
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
                        <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums">{dataCount}</td>
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
        </div>
      )}
    </div>
  );
}
