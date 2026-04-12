'use client';

import { useCallback, useEffect, useState } from 'react';

type StepRuleNoteRow = {
  id: number;
  ruled: string;
  ruledesc: string | null;
  level: string;
  rulenotes: string | null;
  created_at: string;
  updated_at: string;
};

const LEVEL_OPTIONS = ['Winery', 'Vineyard'] as const;

export default function RulesNotesPage() {
  const [rows, setRows] = useState<StepRuleNoteRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [newRow, setNewRow] = useState({ ruled: '', ruledesc: '', level: 'Vineyard' as string, rulenotes: '' });
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editValues, setEditValues] = useState({ ruled: '', ruledesc: '', level: 'Vineyard', rulenotes: '' });

  const load = useCallback(() => {
    setError(null);
    fetch('/api/admin/step-rule-notes')
      .then((r) => r.json())
      .then((data: { rows?: StepRuleNoteRow[]; error?: string }) => {
        if (data.error) throw new Error(data.error);
        setRows(data.rows ?? []);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const addRow = () => {
    const ruled = newRow.ruled.trim();
    if (!ruled) {
      setError('Rule id (ruled) is required');
      return;
    }
    setSaving(true);
    setError(null);
    fetch('/api/admin/step-rule-notes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ruled,
        ruledesc: newRow.ruledesc.trim() || null,
        level: newRow.level,
        rulenotes: newRow.rulenotes.trim() || null,
      }),
    })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        setNewRow({ ruled: '', ruledesc: '', level: 'Vineyard', rulenotes: '' });
        load();
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSaving(false));
  };

  const startEdit = (row: StepRuleNoteRow) => {
    setEditingId(row.id);
    setEditValues({
      ruled: row.ruled,
      ruledesc: row.ruledesc ?? '',
      level: row.level,
      rulenotes: row.rulenotes ?? '',
    });
  };

  const saveEdit = (id: number) => {
    const ruled = editValues.ruled.trim();
    if (!ruled) {
      setError('Rule id (ruled) is required');
      return;
    }
    setSaving(true);
    setError(null);
    fetch(`/api/admin/step-rule-notes/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ruled,
        ruledesc: editValues.ruledesc.trim() || null,
        level: editValues.level,
        rulenotes: editValues.rulenotes.trim() || null,
      }),
    })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        setEditingId(null);
        load();
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSaving(false));
  };

  const deleteRow = (id: number) => {
    if (!confirm('Delete this rules note?')) return;
    setSaving(true);
    setError(null);
    fetch(`/api/admin/step-rule-notes/${id}`, { method: 'DELETE' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        if (editingId === id) setEditingId(null);
        load();
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSaving(false));
  };

  return (
    <div className="mx-auto max-w-5xl p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Rules notes</h1>
      <p className="mb-6 text-sm text-zinc-600 dark:text-zinc-400">
        Document special GPS step rules (e.g. VineSR1). Stored in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_step_rule_notes</code>.
      </p>

      {error && (
        <div className="mb-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
          {error}
        </div>
      )}

      <div className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-medium text-zinc-800 dark:text-zinc-200">Add rule note</h2>
        <div className="grid gap-3 sm:grid-cols-2">
          <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
            Rule id (ruled)
            <input
              className="mt-1 w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              value={newRow.ruled}
              onChange={(e) => setNewRow((r) => ({ ...r, ruled: e.target.value }))}
              placeholder="e.g. VineSR1"
            />
          </label>
          <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
            Level
            <select
              className="mt-1 w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              value={newRow.level}
              onChange={(e) => setNewRow((r) => ({ ...r, level: e.target.value }))}
            >
              {LEVEL_OPTIONS.map((l) => (
                <option key={l} value={l}>
                  {l}
                </option>
              ))}
            </select>
          </label>
          <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:col-span-2">
            Description (ruledesc)
            <input
              className="mt-1 w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              value={newRow.ruledesc}
              onChange={(e) => setNewRow((r) => ({ ...r, ruledesc: e.target.value }))}
            />
          </label>
          <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:col-span-2">
            Notes (rulenotes)
            <textarea
              className="mt-1 min-h-[88px] w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              value={newRow.rulenotes}
              onChange={(e) => setNewRow((r) => ({ ...r, rulenotes: e.target.value }))}
            />
          </label>
        </div>
        <button
          type="button"
          disabled={saving}
          onClick={addRow}
          className="mt-3 rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-200"
        >
          Add
        </button>
      </div>

      <div className="overflow-x-auto rounded-lg border border-zinc-200 dark:border-zinc-700">
        <table className="w-full min-w-[640px] border-collapse text-sm">
          <thead>
            <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/80">
              <th className="px-3 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">ruled</th>
              <th className="px-3 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">level</th>
              <th className="px-3 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">ruledesc</th>
              <th className="px-3 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">rulenotes</th>
              <th className="px-3 py-2 text-right font-semibold text-zinc-700 dark:text-zinc-200">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={5} className="px-3 py-8 text-center text-zinc-500">
                  Loading…
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-3 py-8 text-center text-zinc-500">
                  No rows yet. Add one above, or run <code className="text-xs">create_tbl_step_rule_notes.sql</code>.
                </td>
              </tr>
            ) : (
              rows.map((row) =>
                editingId === row.id ? (
                  <tr key={row.id} className="border-b border-zinc-100 align-top dark:border-zinc-800">
                    <td className="px-3 py-2">
                      <input
                        className="w-full rounded border border-zinc-300 px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        value={editValues.ruled}
                        onChange={(e) => setEditValues((v) => ({ ...v, ruled: e.target.value }))}
                      />
                    </td>
                    <td className="px-3 py-2">
                      <select
                        className="w-full rounded border border-zinc-300 px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        value={editValues.level}
                        onChange={(e) => setEditValues((v) => ({ ...v, level: e.target.value }))}
                      >
                        {LEVEL_OPTIONS.map((l) => (
                          <option key={l} value={l}>
                            {l}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-2">
                      <input
                        className="w-full rounded border border-zinc-300 px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        value={editValues.ruledesc}
                        onChange={(e) => setEditValues((v) => ({ ...v, ruledesc: e.target.value }))}
                      />
                    </td>
                    <td className="px-3 py-2">
                      <textarea
                        className="min-h-[60px] w-full rounded border border-zinc-300 px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        value={editValues.rulenotes}
                        onChange={(e) => setEditValues((v) => ({ ...v, rulenotes: e.target.value }))}
                      />
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-right">
                      <button
                        type="button"
                        disabled={saving}
                        onClick={() => saveEdit(row.id)}
                        className="mr-2 rounded bg-emerald-700 px-2 py-1 text-xs text-white hover:bg-emerald-800 disabled:opacity-50"
                      >
                        Save
                      </button>
                      <button
                        type="button"
                        disabled={saving}
                        onClick={() => setEditingId(null)}
                        className="rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600"
                      >
                        Cancel
                      </button>
                    </td>
                  </tr>
                ) : (
                  <tr key={row.id} className="border-b border-zinc-100 align-top dark:border-zinc-800">
                    <td className="px-3 py-2 font-mono text-xs text-zinc-900 dark:text-zinc-100">{row.ruled}</td>
                    <td className="px-3 py-2 text-zinc-700 dark:text-zinc-300">{row.level}</td>
                    <td className="max-w-[200px] px-3 py-2 text-zinc-600 dark:text-zinc-400">{row.ruledesc ?? '—'}</td>
                    <td className="max-w-md px-3 py-2 whitespace-pre-wrap text-zinc-600 dark:text-zinc-400">
                      {row.rulenotes ?? '—'}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => startEdit(row)}
                        className="mr-2 rounded border border-zinc-300 px-2 py-1 text-xs hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        disabled={saving}
                        onClick={() => deleteRow(row.id)}
                        className="rounded border border-red-200 px-2 py-1 text-xs text-red-800 hover:bg-red-50 dark:border-red-900 dark:text-red-300 dark:hover:bg-red-950/40"
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                )
              )
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
