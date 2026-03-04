'use client';

import { useEffect, useState } from 'react';

type GpsMapping = {
  id: number;
  type: string;
  vwname: string;
  gpsname: string;
};

type Options = {
  fenceNames: string[];
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

  const vwnameOptions = newRow.type === 'Winery'
    ? options?.deliveryWineries ?? []
    : newRow.type === 'Vineyard'
      ? options?.vineyardNames ?? []
      : [];

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
          className="mb-4 flex flex-wrap items-end gap-3 rounded-lg border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-700 dark:bg-zinc-800/50"
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
          <div className="min-w-[200px]">
            <label className="mb-1 block text-xs font-medium text-zinc-500">GPS fence name</label>
            <select
              value={newRow.gpsname}
              onChange={(e) => setNewRow((n) => ({ ...n, gpsname: e.target.value }))}
              required
              className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            >
              <option value="">Select…</option>
              {(options?.fenceNames ?? []).map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
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
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">ID</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">VW name</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">GPS fence name</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr
                  key={r.id}
                  className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                >
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-500 dark:text-zinc-400">{r.id}</td>
                  <td
                    className="cursor-pointer px-3 py-2 text-zinc-700 dark:text-zinc-300"
                    onClick={() => startEdit(r, 'type')}
                  >
                    {renderEditCell(r, 'type', { options: [...TYPE_OPTIONS] }) ?? r.type}
                  </td>
                  <td
                    className="cursor-pointer px-3 py-2 text-zinc-700 dark:text-zinc-300"
                    onClick={() => startEdit(r, 'vwname')}
                  >
                    {renderEditCell(r, 'vwname', {
                      options: getVwnameOptionsForRow(r),
                      placeholder: r.type ? 'Select…' : 'Set type first',
                    }) ?? r.vwname}
                  </td>
                  <td
                    className="cursor-pointer px-3 py-2 text-zinc-700 dark:text-zinc-300"
                    onClick={() => startEdit(r, 'gpsname')}
                  >
                    {renderEditCell(r, 'gpsname', {
                      options: options?.fenceNames ?? [],
                    }) ?? r.gpsname}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2">
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
          {rows.length === 0 && <p className="p-4 text-zinc-500">No rows yet. Add one above.</p>}
        </div>
      )}
    </div>
  );
}
