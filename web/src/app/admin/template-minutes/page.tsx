'use client';

import { useCallback, useEffect, useState } from 'react';

type TemplateMinutesRow = {
  id: number;
  Customer: string | null;
  Template: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

type ModalState = { mode: 'add' } | { mode: 'edit'; row: TemplateMinutesRow };

const emptyForm = () => ({
  Customer: '',
  Template: '',
  ToVineMins: '',
  InVineMins: '',
  ToWineMins: '',
  InWineMins: '',
  TotalMins: '',
});

function numOrEmpty(v: number | null | undefined): string {
  if (v == null) return '';
  return String(v);
}

export default function TemplateMinutesPage() {
  const [rows, setRows] = useState<TemplateMinutesRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modal, setModal] = useState<ModalState | null>(null);
  const [form, setForm] = useState(emptyForm());
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [customers, setCustomers] = useState<string[]>([]);
  const [templates, setTemplates] = useState<string[]>([]);
  const [customersLoading, setCustomersLoading] = useState(false);
  const [templatesLoading, setTemplatesLoading] = useState(false);

  const isAdd = modal?.mode === 'add';
  const editing = modal?.mode === 'edit' ? modal.row : null;

  const loadCustomers = useCallback(() => {
    setCustomersLoading(true);
    fetch('/api/vworkjobs/customers')
      .then((r) => r.json())
      .then((data) => setCustomers(data?.customers ?? []))
      .catch(() => setCustomers([]))
      .finally(() => setCustomersLoading(false));
  }, []);

  const loadTemplates = useCallback((customer: string) => {
    setTemplatesLoading(true);
    const url = customer
      ? `/api/vworkjobs/templates?customer=${encodeURIComponent(customer)}`
      : '/api/vworkjobs/templates';
    fetch(url)
      .then((r) => r.json())
      .then((data) => setTemplates(data?.templates ?? []))
      .catch(() => setTemplates([]))
      .finally(() => setTemplatesLoading(false));
  }, []);

  const loadRows = useCallback(() => {
    setLoading(true);
    fetch('/api/admin/templateminutes')
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setRows(data.rows ?? []);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => loadRows(), [loadRows]);
  useEffect(() => loadCustomers(), [loadCustomers]);

  useEffect(() => {
    if (modal == null) return;
    loadTemplates(form.Customer);
  }, [modal, form.Customer, loadTemplates]);

  const openAdd = useCallback(() => {
    setSubmitError(null);
    setForm(emptyForm());
    setModal({ mode: 'add' });
  }, []);

  const openEdit = useCallback((row: TemplateMinutesRow) => {
    setSubmitError(null);
    setForm({
      Customer: row.Customer ?? '',
      Template: row.Template ?? '',
      ToVineMins: numOrEmpty(row.ToVineMins),
      InVineMins: numOrEmpty(row.InVineMins),
      ToWineMins: numOrEmpty(row.ToWineMins),
      InWineMins: numOrEmpty(row.InWineMins),
      TotalMins: numOrEmpty(row.TotalMins),
    });
    setModal({ mode: 'edit', row });
  }, []);

  const closeForm = useCallback(() => {
    setModal(null);
    setSubmitError(null);
    setForm(emptyForm());
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitError(null);
    const payload = {
      Customer: form.Customer.trim() || null,
      Template: form.Template.trim() || null,
      ToVineMins: form.ToVineMins === '' ? null : parseInt(form.ToVineMins, 10),
      InVineMins: form.InVineMins === '' ? null : parseInt(form.InVineMins, 10),
      ToWineMins: form.ToWineMins === '' ? null : parseInt(form.ToWineMins, 10),
      InWineMins: form.InWineMins === '' ? null : parseInt(form.InWineMins, 10),
      TotalMins: form.TotalMins === '' ? null : parseInt(form.TotalMins, 10),
    };
    if (editing) {
      fetch(`/api/admin/templateminutes/${editing.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
        .then(async (r) => {
          const data = await r.json();
          if (!r.ok) throw new Error(data.error ?? r.statusText);
          closeForm();
          loadRows();
        })
        .catch((e) => setSubmitError(e.message));
    } else {
      fetch('/api/admin/templateminutes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
        .then(async (r) => {
          const data = await r.json();
          if (!r.ok) throw new Error(data.error ?? r.statusText);
          closeForm();
          loadRows();
        })
        .catch((e) => setSubmitError(e.message));
    }
  };

  const handleDelete = (row: TemplateMinutesRow) => {
    if (!confirm(`Delete template minutes for ${row.Customer ?? ''} / ${row.Template ?? ''}?`)) return;
    fetch(`/api/admin/templateminutes/${row.id}`, { method: 'DELETE' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error ?? r.statusText);
        loadRows();
      })
      .catch((e) => setError(e.message));
  };

  const numInput = (
    key: keyof typeof form,
    label: string
  ) => (
    <div key={key}>
      <label className="mb-1 block text-xs font-medium text-zinc-500">{label}</label>
      <input
        type="number"
        min={0}
        value={form[key]}
        onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
        className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
      />
    </div>
  );

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Template Minutes</h1>
        <button
          type="button"
          onClick={openAdd}
          className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
        >
          Add record
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-zinc-600">Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Customer</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Template</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">To Vine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">In Vine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">To Wine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">In Wine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Total</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.Customer ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.Template ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 tabular-nums text-zinc-700 dark:text-zinc-300">{r.ToVineMins ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 tabular-nums text-zinc-700 dark:text-zinc-300">{r.InVineMins ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 tabular-nums text-zinc-700 dark:text-zinc-300">{r.ToWineMins ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 tabular-nums text-zinc-700 dark:text-zinc-300">{r.InWineMins ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 tabular-nums font-medium text-zinc-900 dark:text-zinc-100">{r.TotalMins ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2">
                    <button
                      type="button"
                      onClick={() => openEdit(r)}
                      className="mr-2 text-blue-600 hover:underline dark:text-blue-400"
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDelete(r)}
                      className="text-red-600 hover:underline dark:text-red-400"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && <p className="p-4 text-zinc-500">No template minutes yet. Add a record to get started.</p>}
        </div>
      )}

      {modal !== null && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
          onClick={(e) => { if (e.target === e.currentTarget) closeForm(); }}
        >
          <div
            className="w-full max-w-md rounded-lg bg-white p-6 shadow-lg dark:bg-zinc-900"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="mb-4 text-lg font-semibold text-zinc-900 dark:text-zinc-100">
              {editing ? 'Edit template minutes' : 'Add template minutes'}
            </h2>
            <form onSubmit={handleSubmit} className="flex flex-col gap-3">
              {submitError && (
                <p className="text-sm text-red-600 dark:text-red-400">{submitError}</p>
              )}
              <div>
                <div className="mb-1 flex items-center justify-between">
                  <label className="text-xs font-medium text-zinc-500">Customer</label>
                  <button
                    type="button"
                    onClick={loadCustomers}
                    disabled={customersLoading}
                    title="Refresh from vwork"
                    className="rounded p-1 text-zinc-500 hover:bg-zinc-200 hover:text-zinc-700 disabled:opacity-50 dark:hover:bg-zinc-700 dark:hover:text-zinc-300"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={customersLoading ? 'animate-spin' : ''}>
                      <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" /><path d="M3 3v5h5" />
                      <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16" /><path d="M16 21h5v-5" />
                    </svg>
                  </button>
                </div>
                <select
                  value={form.Customer}
                  onChange={(e) => setForm((f) => ({ ...f, Customer: e.target.value, Template: '' }))}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                >
                  <option value="">— Select —</option>
                  {customers.map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Template</label>
                <select
                  value={form.Template}
                  onChange={(e) => setForm((f) => ({ ...f, Template: e.target.value }))}
                  disabled={!form.Customer}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                >
                  <option value="">— Select —</option>
                  {templates.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </div>
              {numInput('ToVineMins', 'To Vine (mins)')}
              {numInput('InVineMins', 'In Vine (mins)')}
              {numInput('ToWineMins', 'To Wine (mins)')}
              {numInput('InWineMins', 'In Wine (mins)')}
              {numInput('TotalMins', 'Total (mins)')}
              <div className="flex justify-end gap-2 pt-2">
                <button type="button" onClick={closeForm} className="rounded bg-zinc-200 px-4 py-2 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">
                  Cancel
                </button>
                <button type="submit" className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700">
                  {editing ? 'Update' : 'Create'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
