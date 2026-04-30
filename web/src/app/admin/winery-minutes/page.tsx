'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

const TT_OPTIONS = ['T', 'TT', 'TTT'] as const;

type WineryMinutesRow = {
  id: number;
  Customer: string | null;
  Template: string | null;
  vineyardgroup: string | null;
  Winery: string | null;
  TT: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

type ModalState = { mode: 'add' } | { mode: 'edit'; row: WineryMinutesRow };

const emptyForm = () => ({
  Customer: '',
  Template: '',
  vineyardgroup: '',
  Winery: '',
  TT: '' as '' | 'T' | 'TT' | 'TTT',
  ToVineMins: '',
  InVineMins: '',
  ToWineMins: '',
  InWineMins: '',
  TotalMins: '',
});

function numOrEmpty(v: number | null | undefined): string {
  if (v == null) return '';
  const n = Number(v);
  if (Number.isNaN(n)) return '';
  return n % 1 === 0 ? String(n) : n.toFixed(2).replace(/\.?0+$/, '');
}

function normCell(s: string | null | undefined): string {
  return (s ?? '').trim();
}

function ctwKey(r: WineryMinutesRow): string {
  return `${normCell(r.Customer)}\u0000${normCell(r.Template)}\u0000${normCell(r.Winery)}`;
}

/** Alternating band index (boolean) whenever Customer+Template+Winery changes from the row above. */
function ctwGroupBands(rows: WineryMinutesRow[]): boolean[] {
  const out: boolean[] = [];
  let band = false;
  let prev: string | null = null;
  for (const r of rows) {
    const k = ctwKey(r);
    if (prev !== null && k !== prev) band = !band;
    out.push(band);
    prev = k;
  }
  return out;
}

export default function WineryMinutesPage() {
  const [rows, setRows] = useState<WineryMinutesRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modal, setModal] = useState<ModalState | null>(null);
  const [form, setForm] = useState(emptyForm());
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [customers, setCustomers] = useState<string[]>([]);
  const [templates, setTemplates] = useState<string[]>([]);
  const [vineyardGroups, setVineyardGroups] = useState<string[]>([]);
  const [wineries, setWineries] = useState<string[]>([]);
  const [customersLoading, setCustomersLoading] = useState(false);
  const [templatesLoading, setTemplatesLoading] = useState(false);
  const [wineriesLoading, setWineriesLoading] = useState(false);
  const [totalMismatchInfo, setTotalMismatchInfo] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<WineryMinutesRow | null>(null);
  const [subTab, setSubTab] = useState<'records' | 'exceptions'>('records');
  const [limitExceptions, setLimitExceptions] = useState<
    { customer: string; template: string; delivery_winery: string; vineyard_group: string; job_tt: string; job_count: number }[]
  >([]);
  const [exceptionsLoading, setExceptionsLoading] = useState(false);
  const [exceptionsError, setExceptionsError] = useState<string | null>(null);
  const [exceptionStats, setExceptionStats] = useState<{ comboCount: number; exceptionCount: number } | null>(null);
  const importFileRef = useRef<HTMLInputElement>(null);
  const [importBusy, setImportBusy] = useState(false);
  const [importSummary, setImportSummary] = useState<string | null>(null);

  const editing = modal?.mode === 'edit' ? modal.row : null;

  const ctwBands = useMemo(() => ctwGroupBands(rows), [rows]);

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

  const loadWineries = useCallback((customer: string, template: string) => {
    setWineriesLoading(true);
    const params = new URLSearchParams();
    if (customer) params.set('customer', customer);
    if (template) params.set('template', template);
    const url = `/api/admin/data-checks/wine-mapp/delivery-wineries${params.toString() ? `?${params}` : ''}`;
    fetch(url)
      .then((r) => r.json())
      .then((data) => setWineries(data?.rows ?? []))
      .catch(() => setWineries([]))
      .finally(() => setWineriesLoading(false));
  }, []);

  const loadRows = useCallback(() => {
    setLoading(true);
    fetch('/api/admin/wineryminutes')
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

  const loadLimitExceptions = useCallback(() => {
    setExceptionsLoading(true);
    setExceptionsError(null);
    fetch('/api/admin/wineryminutes/limit-exceptions')
      .then((r) => r.json())
      .then((data) => {
        if (data?.error) throw new Error(data.error);
        setLimitExceptions(data.exceptions ?? []);
        setExceptionStats(
          data.comboCount != null && data.exceptionCount != null
            ? { comboCount: data.comboCount, exceptionCount: data.exceptionCount }
            : null,
        );
      })
      .catch((e) => {
        setExceptionsError(e instanceof Error ? e.message : String(e));
        setLimitExceptions([]);
        setExceptionStats(null);
      })
      .finally(() => setExceptionsLoading(false));
  }, []);

  useEffect(() => {
    if (subTab === 'exceptions') loadLimitExceptions();
  }, [subTab, loadLimitExceptions]);
  useEffect(() => loadCustomers(), [loadCustomers]);
  useEffect(() => {
    fetch('/api/admin/wineryminutes/vineyard-groups')
      .then((r) => r.json())
      .then((data) => setVineyardGroups(data?.rows ?? []))
      .catch(() => setVineyardGroups([]));
  }, []);

  useEffect(() => {
    if (modal == null) return;
    loadTemplates(form.Customer);
  }, [modal, form.Customer, loadTemplates]);

  useEffect(() => {
    if (modal == null) return;
    loadWineries(form.Customer, form.Template);
  }, [modal, form.Customer, form.Template, loadWineries]);

  const openAdd = useCallback(() => {
    setSubmitError(null);
    setForm(emptyForm());
    setModal({ mode: 'add' });
  }, []);

  const openEdit = useCallback((row: WineryMinutesRow) => {
    setSubmitError(null);
    setForm({
      Customer: row.Customer ?? '',
      Template: row.Template ?? '',
      vineyardgroup: row.vineyardgroup ?? '',
      Winery: row.Winery ?? '',
      TT: (row.TT === 'T' || row.TT === 'TT' || row.TT === 'TTT' ? row.TT : '') as '' | 'T' | 'TT' | 'TTT',
      ToVineMins: numOrEmpty(row.ToVineMins),
      InVineMins: numOrEmpty(row.InVineMins),
      ToWineMins: numOrEmpty(row.ToWineMins),
      InWineMins: numOrEmpty(row.InWineMins),
      TotalMins: numOrEmpty(row.TotalMins),
    });
    setModal({ mode: 'edit', row });
  }, []);

  const openClone = useCallback((row: WineryMinutesRow) => {
    setSubmitError(null);
    setForm({
      Customer: row.Customer ?? '',
      Template: row.Template ?? '',
      vineyardgroup: row.vineyardgroup ?? '',
      Winery: row.Winery ?? '',
      TT: (row.TT === 'T' || row.TT === 'TT' || row.TT === 'TTT' ? row.TT : '') as '' | 'T' | 'TT' | 'TTT',
      ToVineMins: numOrEmpty(row.ToVineMins),
      InVineMins: numOrEmpty(row.InVineMins),
      ToWineMins: numOrEmpty(row.ToWineMins),
      InWineMins: numOrEmpty(row.InWineMins),
      TotalMins: numOrEmpty(row.TotalMins),
    });
    setModal({ mode: 'add' });
  }, []);

  const closeForm = useCallback(() => {
    setModal(null);
    setSubmitError(null);
    setTotalMismatchInfo(null);
    setForm(emptyForm());
  }, []);

  const sumOfFour = (): number => {
    const a = parseFloat(form.ToVineMins) || 0;
    const b = parseFloat(form.InVineMins) || 0;
    const c = parseFloat(form.ToWineMins) || 0;
    const d = parseFloat(form.InWineMins) || 0;
    return Math.round((a + b + c + d) * 100) / 100;
  };

  useEffect(() => {
    if (form.TotalMins === '') {
      setTotalMismatchInfo(null);
      return;
    }
    const total = parseFloat(form.TotalMins);
    if (Number.isNaN(total)) return;
    const a = parseFloat(form.ToVineMins) || 0;
    const b = parseFloat(form.InVineMins) || 0;
    const c = parseFloat(form.ToWineMins) || 0;
    const d = parseFloat(form.InWineMins) || 0;
    const sum = Math.round((a + b + c + d) * 100) / 100;
    if (Math.abs(total - sum) > 0.005) {
      setTotalMismatchInfo(`Total (${form.TotalMins}) does not equal sum of the four (${sum.toFixed(2)}). You can keep your value.`);
    } else {
      setTotalMismatchInfo(null);
    }
  }, [form.ToVineMins, form.InVineMins, form.ToWineMins, form.InWineMins, form.TotalMins]);

  const fillTotalFromSum = () => {
    if (form.TotalMins !== '') return;
    const sum = sumOfFour();
    setForm((f) => ({ ...f, TotalMins: sum % 1 === 0 ? String(sum) : sum.toFixed(2).replace(/\.?0+$/, '') }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitError(null);
    const toNum = (s: string) => (s === '' ? null : Math.round(parseFloat(s) * 100) / 100);
    const payload = {
      Customer: form.Customer.trim() || null,
      Template: form.Template.trim() || null,
      vineyardgroup: form.vineyardgroup.trim() || null,
      Winery: form.Winery.trim() || null,
      TT: (form.TT === 'T' || form.TT === 'TT' || form.TT === 'TTT' ? form.TT : null) as string | null,
      ToVineMins: toNum(form.ToVineMins),
      InVineMins: toNum(form.InVineMins),
      ToWineMins: toNum(form.ToWineMins),
      InWineMins: toNum(form.InWineMins),
      TotalMins: toNum(form.TotalMins),
    };
    if (editing) {
      fetch(`/api/admin/wineryminutes/${editing.id}`, {
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
      fetch('/api/admin/wineryminutes', {
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

  const confirmDelete = () => {
    const row = pendingDelete;
    if (!row) return;
    setPendingDelete(null);
    fetch(`/api/admin/wineryminutes/${row.id}`, { method: 'DELETE' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error ?? r.statusText);
        loadRows();
      })
      .catch((e) => setError(e.message));
  };

  const handleExportXlsx = useCallback(async () => {
    setError(null);
    try {
      const r = await fetch('/api/admin/wineryminutes/export');
      if (!r.ok) {
        const j = (await r.json().catch(() => ({}))) as { error?: string };
        throw new Error(j.error ?? r.statusText);
      }
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `winery-minutes-${new Date().toISOString().slice(0, 10)}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const handleImportFile = useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      e.target.value = '';
      if (!file) return;
      setImportBusy(true);
      setError(null);
      setImportSummary(null);
      try {
        const fd = new FormData();
        fd.append('file', file);
        const r = await fetch('/api/admin/wineryminutes/import', { method: 'POST', body: fd });
        const data = (await r.json()) as {
          error?: string;
          ok?: boolean;
          updated?: number;
          inserted?: number;
          skipped?: number;
          errors?: { row: number; message: string }[];
        };
        if (!r.ok) throw new Error(data.error ?? r.statusText);
        const parts = [
          `Updated ${data.updated ?? 0}, inserted ${data.inserted ?? 0}, skipped blank ${data.skipped ?? 0}.`,
        ];
        if (data.errors?.length) {
          parts.push(
            ` Row issues: ${data.errors.map((x) => `${x.row}: ${x.message}`).join(' · ')}`,
          );
        }
        setImportSummary(parts.join(''));
        loadRows();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setImportBusy(false);
      }
    },
    [loadRows],
  );

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Winery Minutes</h1>
        {subTab === 'records' ? (
          <div className="flex flex-wrap items-center gap-2">
            <input
              ref={importFileRef}
              type="file"
              accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              className="hidden"
              onChange={handleImportFile}
            />
            <button
              type="button"
              onClick={handleExportXlsx}
              className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-800 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
            >
              Export XLSX
            </button>
            <button
              type="button"
              disabled={importBusy}
              onClick={() => importFileRef.current?.click()}
              className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-60 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
            >
              {importBusy ? 'Importing…' : 'Import XLSX'}
            </button>
            <button
              type="button"
              onClick={openAdd}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
            >
              Add record
            </button>
          </div>
        ) : (
          <button
            type="button"
            onClick={loadLimitExceptions}
            disabled={exceptionsLoading}
            className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-60 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
          >
            {exceptionsLoading ? 'Refreshing…' : 'Refresh'}
          </button>
        )}
      </div>

      <div className="mb-4 flex gap-1 border-b border-zinc-200 dark:border-zinc-700">
        <button
          type="button"
          onClick={() => setSubTab('records')}
          className={`rounded-t px-4 py-2 text-sm font-medium ${
            subTab === 'records'
              ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100'
              : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Records
        </button>
        <button
          type="button"
          onClick={() => setSubTab('exceptions')}
          className={`rounded-t px-4 py-2 text-sm font-medium ${
            subTab === 'exceptions'
              ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100'
              : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Limit exceptions
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          {error}
        </div>
      )}

      {subTab === 'records' && importSummary && (
        <div className="mb-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-200">
          {importSummary}
        </div>
      )}

      {subTab === 'records' && (
        <p className="mb-3 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          Export downloads all rows. Edit minute columns in Excel, then import: rows are matched on{' '}
          <strong>Template</strong>, <strong>Winery</strong>, <strong>Vineyard group</strong> (blank matches blank), and{' '}
          <strong>TT</strong>. If <strong>Customer</strong> is set, it is included in the match; if it is empty, the file
          must uniquely identify one DB row. Matching rows only update minute fields; otherwise a new row is inserted when
          Customer is present.
        </p>
      )}

      {subTab === 'exceptions' ? (
        <>
          <p className="mb-3 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
            Jobs in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs</code> grouped by customer, template, delivery winery, vineyard group, and TT. Listed rows have{' '}
            <strong>no</strong> matching row in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_wineryminutes</code> under the same rules as Summary: exact vineyard group (blank only matches blank), plus TT match or TTT fallback for T/TT.
          </p>
          {exceptionStats && (
            <p className="mb-3 text-xs text-zinc-500 dark:text-zinc-400">
              {exceptionStats.exceptionCount} missing of {exceptionStats.comboCount} distinct job keys (with delivery winery set).
            </p>
          )}
          {exceptionsError && (
            <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">{exceptionsError}</div>
          )}
          {exceptionsLoading ? (
            <p className="text-zinc-600">Loading…</p>
          ) : (
            <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <table className="min-w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                    <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Customer</th>
                    <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Template</th>
                    <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Winery</th>
                    <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Vineyard group</th>
                    <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">TT</th>
                    <th className="px-3 py-2 text-right font-medium text-zinc-900 dark:text-zinc-100">Jobs</th>
                  </tr>
                </thead>
                <tbody>
                  {limitExceptions.map((ex, i) => (
                    <tr key={`${ex.customer}|${ex.template}|${ex.delivery_winery}|${ex.vineyard_group}|${ex.job_tt}|${i}`} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-800 dark:text-zinc-200">{ex.customer || '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-800 dark:text-zinc-200">{ex.template || '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-800 dark:text-zinc-200">{ex.delivery_winery || '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-800 dark:text-zinc-200">{ex.vineyard_group || '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-800 dark:text-zinc-200">{ex.job_tt || '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-800 dark:text-zinc-200">{ex.job_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {limitExceptions.length === 0 && !exceptionsError && (
                <p className="p-4 text-zinc-500">No gaps — every vwork job key has a matching winery minutes row.</p>
              )}
            </div>
          )}
        </>
      ) : loading ? (
        <p className="text-zinc-600">Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Customer</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Template</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Winery</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Vineyard group</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">TT</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">To Vine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">In Vine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">To Wine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">In Wine</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Total</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, rowIdx) => {
                const newGroup =
                  rowIdx > 0 && ctwKey(r) !== ctwKey(rows[rowIdx - 1]!);
                const shaded = ctwBands[rowIdx];
                return (
                <tr
                  key={r.id}
                  className={`border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-100/80 dark:hover:bg-zinc-800/70 ${
                    shaded ? 'bg-sky-50/90 dark:bg-sky-950/30' : ''
                  } ${newGroup ? 'border-t-2 border-t-zinc-300 dark:border-t-zinc-600' : ''}`}
                >
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.Customer ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.Template ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.Winery ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.vineyardgroup ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{r.TT ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-700 dark:text-zinc-300">{r.ToVineMins != null ? Number(r.ToVineMins).toFixed(2) : '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-700 dark:text-zinc-300">{r.InVineMins != null ? Number(r.InVineMins).toFixed(2) : '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-700 dark:text-zinc-300">{r.ToWineMins != null ? Number(r.ToWineMins).toFixed(2) : '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums text-zinc-700 dark:text-zinc-300">{r.InWineMins != null ? Number(r.InWineMins).toFixed(2) : '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums font-medium text-zinc-900 dark:text-zinc-100">{r.TotalMins != null ? Number(r.TotalMins).toFixed(2) : '—'}</td>
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
                      onClick={() => openClone(r)}
                      className="mr-2 text-zinc-600 hover:underline dark:text-zinc-400"
                      title="Clone as new record"
                    >
                      Clone
                    </button>
                    <button
                      type="button"
                      onClick={() => setPendingDelete(r)}
                      className="text-red-600 hover:underline dark:text-red-400"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              );
              })}
            </tbody>
          </table>
          {rows.length === 0 && <p className="p-4 text-zinc-500">No winery minutes yet. Add a record to get started.</p>}
        </div>
      )}


      {pendingDelete && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="winery-minutes-delete-title"
          onClick={(e) => { if (e.target === e.currentTarget) setPendingDelete(null); }}
        >
          <div
            className="w-full max-w-md rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-600 dark:bg-zinc-900"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
              <h3 id="winery-minutes-delete-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                Delete winery minutes?
              </h3>
            </div>
            <div className="px-4 py-4 text-sm text-zinc-700 dark:text-zinc-300">
              <p>
                This will remove limits for{' '}
                <span className="font-medium text-zinc-900 dark:text-zinc-100">
                  {pendingDelete.Customer ?? '—'} / {pendingDelete.Template ?? '—'} / {pendingDelete.vineyardgroup ?? '—'} / {pendingDelete.Winery ?? '—'}
                </span>
                {pendingDelete.TT ? (
                  <>
                    {' '}
                    (TT: {pendingDelete.TT})
                  </>
                ) : null}
                .
              </p>
            </div>
            <div className="flex justify-end gap-2 border-t border-zinc-200 px-4 py-3 dark:border-zinc-700">
              <button
                type="button"
                onClick={() => setPendingDelete(null)}
                className="rounded bg-zinc-200 px-4 py-2 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={confirmDelete}
                className="rounded bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
              >
                Delete
              </button>
            </div>
          </div>
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
              {editing ? 'Edit winery minutes' : 'Add winery minutes'}
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
                  onChange={(e) => setForm((f) => ({ ...f, Customer: e.target.value, Template: '', vineyardgroup: '', Winery: '' }))}
                  className={`w-full rounded border px-2 py-1.5 text-sm dark:bg-zinc-800 ${
                    form.Customer && !customers.includes(form.Customer)
                      ? 'border-red-400 text-red-600 dark:border-red-500 dark:text-red-400'
                      : 'border-zinc-300 dark:border-zinc-600 dark:text-zinc-100'
                  }`}
                >
                  <option value="">— Select —</option>
                  {form.Customer && !customers.includes(form.Customer) && (
                    <option value={form.Customer}>{form.Customer} (not in list)</option>
                  )}
                  {customers.map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Template</label>
                <select
                  value={form.Template}
                  onChange={(e) => setForm((f) => ({ ...f, Template: e.target.value, vineyardgroup: '', Winery: '' }))}
                  disabled={!form.Customer}
                  className={`w-full rounded border px-2 py-1.5 text-sm dark:bg-zinc-800 disabled:opacity-70 ${
                    form.Template && !templates.includes(form.Template)
                      ? 'border-red-400 text-red-600 dark:border-red-500 dark:text-red-400'
                      : 'border-zinc-300 dark:border-zinc-600 dark:text-zinc-100'
                  }`}
                >
                  <option value="">— Select —</option>
                  {form.Template && !templates.includes(form.Template) && (
                    <option value={form.Template}>{form.Template} (not in list)</option>
                  )}
                  {templates.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </div>
              <div>
                <div className="mb-1 flex items-center justify-between">
                  <label className="text-xs font-medium text-zinc-500">Winery</label>
                  <button
                    type="button"
                    onClick={() => loadWineries(form.Customer, form.Template)}
                    disabled={wineriesLoading || !form.Customer}
                    title="Refresh delivery wineries (by customer only if template empty)"
                    className="rounded p-1 text-zinc-500 hover:bg-zinc-200 hover:text-zinc-700 disabled:opacity-50 dark:hover:bg-zinc-700 dark:hover:text-zinc-300"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={wineriesLoading ? 'animate-spin' : ''}>
                      <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" /><path d="M3 3v5h5" />
                      <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16" /><path d="M16 21h5v-5" />
                    </svg>
                  </button>
                </div>
                <select
                  value={form.Winery}
                  onChange={(e) => setForm((f) => ({ ...f, Winery: e.target.value }))}
                  disabled={!form.Customer}
                  className={`w-full rounded border px-2 py-1.5 text-sm dark:bg-zinc-800 disabled:opacity-70 ${
                    form.Winery && !wineries.includes(form.Winery)
                      ? 'border-red-400 text-red-600 dark:border-red-500 dark:text-red-400'
                      : 'border-zinc-300 dark:border-zinc-600 dark:text-zinc-100'
                  }`}
                >
                  <option value="">— Select —</option>
                  {form.Winery && !wineries.includes(form.Winery) && (
                    <option value={form.Winery}>{form.Winery} (not in list)</option>
                  )}
                  {wineries.map((w) => (
                    <option key={w} value={w}>{w}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Vineyard group</label>
                <input
                  type="text"
                  list="vineyardgroup-datalist"
                  value={form.vineyardgroup}
                  onChange={(e) => setForm((f) => ({ ...f, vineyardgroup: e.target.value }))}
                  disabled={!form.Customer}
                  placeholder="Type or pick from list"
                  className={`w-full rounded border px-2 py-1.5 text-sm dark:bg-zinc-800 disabled:opacity-70 ${
                    form.vineyardgroup && !vineyardGroups.includes(form.vineyardgroup)
                      ? 'border-amber-400 text-zinc-700 dark:border-amber-500 dark:text-zinc-200'
                      : 'border-zinc-300 dark:border-zinc-600 dark:text-zinc-100'
                  }`}
                />
                <datalist id="vineyardgroup-datalist">
                  {vineyardGroups.map((g) => (
                    <option key={g} value={g} />
                  ))}
                </datalist>
                <p className="mt-0.5 text-[11px] text-zinc-500 dark:text-zinc-500">Free text or pick from tbl_vworkjobs.vineyard_group</p>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">TT</label>
                <select
                  value={form.TT}
                  onChange={(e) => setForm((f) => ({ ...f, TT: e.target.value as '' | 'T' | 'TT' | 'TTT' }))}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                >
                  <option value="">— Select —</option>
                  {TT_OPTIONS.map((v) => (
                    <option key={v} value={v}>{v}</option>
                  ))}
                </select>
              </div>
              {/* Graphical flow: Winery → Vineyard (load) → Winery (unload) */}
              <div className="rounded-lg border border-zinc-200 bg-zinc-50/80 p-4 dark:border-zinc-600 dark:bg-zinc-800/50">
                <p className="mb-3 text-xs font-medium uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                  Minute limits (trip flow)
                </p>
                <div className="space-y-3">
                  <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-amber-100 text-sm font-bold text-amber-800 dark:bg-amber-900/50 dark:text-amber-200" title="Drive to vineyard">→</div>
                    <div className="min-w-0 flex-1">
                      <label className="mb-0.5 block text-xs font-medium text-zinc-600 dark:text-zinc-400">To Vine (mins)</label>
                      <p className="text-[11px] text-zinc-500 dark:text-zinc-500">Time driving from Winery to Vineyard</p>
                    </div>
                    <input
                      type="number"
                      min={0}
                      step={0.01}
                      value={form.ToVineMins}
                      onChange={(e) => setForm((f) => ({ ...f, ToVineMins: e.target.value }))}
                      className="w-24 shrink-0 rounded border border-zinc-300 bg-white px-2 py-1 text-right text-sm tabular-nums text-zinc-900 dark:border-zinc-500 dark:bg-white dark:text-zinc-900"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-emerald-100 text-xs font-semibold text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-200" title="Vineyard">V</div>
                    <div className="min-w-0 flex-1">
                      <label className="mb-0.5 block text-xs font-medium text-zinc-600 dark:text-zinc-400">In Vine (mins)</label>
                      <p className="text-[11px] text-zinc-500 dark:text-zinc-500">Time spent inside Vineyard (loading)</p>
                    </div>
                    <input
                      type="number"
                      min={0}
                      step={0.01}
                      value={form.InVineMins}
                      onChange={(e) => setForm((f) => ({ ...f, InVineMins: e.target.value }))}
                      className="w-24 shrink-0 rounded border border-zinc-300 bg-white px-2 py-1 text-right text-sm tabular-nums text-zinc-900 dark:border-zinc-500 dark:bg-white dark:text-zinc-900"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-sky-100 text-sm font-bold text-sky-800 dark:bg-sky-900/50 dark:text-sky-200" title="Drive back to winery">←</div>
                    <div className="min-w-0 flex-1">
                      <label className="mb-0.5 block text-xs font-medium text-zinc-600 dark:text-zinc-400">To Wine (mins)</label>
                      <p className="text-[11px] text-zinc-500 dark:text-zinc-500">Time travelling back to Winery</p>
                    </div>
                    <input
                      type="number"
                      min={0}
                      step={0.01}
                      value={form.ToWineMins}
                      onChange={(e) => setForm((f) => ({ ...f, ToWineMins: e.target.value }))}
                      className="w-24 shrink-0 rounded border border-zinc-300 bg-white px-2 py-1 text-right text-sm tabular-nums text-zinc-900 dark:border-zinc-500 dark:bg-white dark:text-zinc-900"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-amber-100 text-xs font-semibold text-amber-800 dark:bg-amber-900/50 dark:text-amber-200" title="Back at winery">W</div>
                    <div className="min-w-0 flex-1">
                      <label className="mb-0.5 block text-xs font-medium text-zinc-600 dark:text-zinc-400">In Wine (mins)</label>
                      <p className="text-[11px] text-zinc-500 dark:text-zinc-500">Time spent at Winery (unloading)</p>
                    </div>
                    <input
                      type="number"
                      min={0}
                      step={0.01}
                      value={form.InWineMins}
                      onChange={(e) => setForm((f) => ({ ...f, InWineMins: e.target.value }))}
                      className="w-24 shrink-0 rounded border border-zinc-300 bg-white px-2 py-1 text-right text-sm tabular-nums text-zinc-900 dark:border-zinc-500 dark:bg-white dark:text-zinc-900"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="h-9 w-9 shrink-0" />
                    <div className="min-w-0 flex-1">
                      <label className="mb-0.5 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Total (mins)</label>
                      <p className="text-[11px] text-zinc-500 dark:text-zinc-500">Leave empty and click field to fill from sum of the four</p>
                    </div>
                    <input
                      type="number"
                      min={0}
                      max={999.99}
                      step={0.01}
                      value={form.TotalMins}
                      onFocus={fillTotalFromSum}
                      onChange={(e) => setForm((f) => ({ ...f, TotalMins: e.target.value }))}
                      className="w-24 shrink-0 rounded border border-zinc-300 bg-white px-2 py-1 text-right text-sm tabular-nums text-zinc-900 dark:border-zinc-500 dark:bg-white dark:text-zinc-900"
                    />
                  </div>
                </div>
                {totalMismatchInfo && (
                  <div className="mt-3 rounded bg-blue-50 py-2 px-3 text-xs text-blue-800 dark:bg-blue-950/50 dark:text-blue-200" role="alert">
                    {totalMismatchInfo}
                  </div>
                )}
              </div>
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
