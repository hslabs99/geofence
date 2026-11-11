'use client';

import { useEffect, useMemo, useState } from 'react';
import DriverDiversionSetup from '@/components/admin/DriverDiversionSetup';
import StaffOnlyGate from '@/components/StaffOnlyGate';
import DriverAppPreview from '@/components/driver/DriverAppPreview';
import { DriverLookup } from '@/components/driver/DriverLookup';
import { JobQueueIssues } from '@/components/driver/JobQueueIssues';
import RealTimeJobs from '@/components/driver/RealTimeJobs';
import { completedGateCount } from '@/lib/driver-jobs';

type DriverJobRow = {
  id: number;
  job_id: string;
  company_name: string | null;
  group_name: string | null;
  truck_id: string | null;
  imei: string | null;
  driver: string | null;
  planned_start_time: string | null;
  winery_name: string | null;
  vineyard_name: string | null;
  winery_geo_fence: string | null;
  vineyard_geo_fence: string | null;
  step_1_click: string | null;
  step_2_click: string | null;
  step_3_click: string | null;
  step_4_click: string | null;
  step_5_click: string | null;
};

const STAGE_NAME = ['Not started', 'Start job', 'Arrive vineyard', 'Depart vineyard', 'Arrive winery', 'End job'] as const;

function jobStage(row: DriverJobRow): number {
  return completedGateCount(row);
}

type DeviceOption = { deviceName: string; imei: string };
type DriverUserOption = { value: string; label: string };

function cell(value: string | null | undefined): string {
  const s = value == null ? '' : String(value).trim();
  return s || '—';
}

function TrashIcon() {
  return (
    <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" className="h-4 w-4" aria-hidden="true">
      <path strokeLinecap="round" d="M4.5 6.5h11" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M8 6.5V5.2A1.2 1.2 0 0 1 9.2 4h1.6A1.2 1.2 0 0 1 12 5.2v1.3" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M6.2 6.5l.6 9.1a1.2 1.2 0 0 0 1.2 1.1h4a1.2 1.2 0 0 0 1.2-1.1l.6-9.1" />
      <path strokeLinecap="round" d="M8.5 9.2v4.6M11.5 9.2v4.6" />
    </svg>
  );
}

function FenceMapMark({ mapped }: { mapped: boolean }) {
  const label = mapped ? 'Geofence mapped' : 'Geofence not mapped';
  return (
    <span
      title={label}
      aria-label={label}
      className={`inline-flex shrink-0 ${mapped ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}
    >
      {mapped ? (
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2.2" className="h-3.5 w-3.5" aria-hidden="true">
          <path strokeLinecap="round" strokeLinejoin="round" d="M3 8.2 6.2 11.5 13 4.5" />
        </svg>
      ) : (
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2.2" className="h-3.5 w-3.5" aria-hidden="true">
          <path strokeLinecap="round" d="M4 4l8 8M12 4 4 12" />
        </svg>
      )}
    </span>
  );
}

function placeCell(name: string | null | undefined, fence: string | null | undefined) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {cell(name)}
      <FenceMapMark mapped={Boolean((fence ?? '').trim())} />
    </span>
  );
}

const emptyJobForm = {
  job_id: '',
  imei: '',
  driver: '',
  planned_start_time: '',
  winery_name: '',
  vineyard_name: '',
  winery_geo_fence: '',
  vineyard_geo_fence: '',
};

function DriverVisibilityPageContent() {
  const [filterDriver, setFilterDriver] = useState('');
  const [filterWinery, setFilterWinery] = useState('');
  const [filterVineyard, setFilterVineyard] = useState('');
  const [drivers, setDrivers] = useState<string[]>([]);
  const [driverUsers, setDriverUsers] = useState<DriverUserOption[]>([]);
  const [wineries, setWineries] = useState<string[]>([]);
  const [vineyards, setVineyards] = useState<string[]>([]);
  const [fenceNames, setFenceNames] = useState<string[]>([]);
  const [devices, setDevices] = useState<DeviceOption[]>([]);
  const [rows, setRows] = useState<DriverJobRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [listVersion, setListVersion] = useState(0);
  const [jobForm, setJobForm] = useState(emptyJobForm);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [addError, setAddError] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<DriverJobRow | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [tab, setTab] = useState<'queue' | 'issues' | 'drivers' | 'live' | 'app' | 'app-settings'>('queue');
  const [issueCount, setIssueCount] = useState(0);
  const [driversMissing, setDriversMissing] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const loadCount = () => {
      fetch('/api/driver-jobs/queue-issues', { cache: 'no-store' })
        .then(async (response) => {
          const data = await response.json();
          if (!response.ok) return;
          if (!cancelled) setIssueCount(Number(data?.open ?? 0));
        })
        .catch(() => undefined);
    };
    loadCount();
    const timer = setInterval(loadCount, 15000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [listVersion]);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/driver-jobs/drivers', { cache: 'no-store' })
      .then(async (response) => {
        const data = await response.json();
        if (!response.ok || cancelled) return;
        setDriversMissing(Number(data?.missing ?? 0));
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const params = new URLSearchParams();
    if (filterDriver.trim()) params.set('driver', filterDriver.trim());
    if (filterWinery.trim()) params.set('winery', filterWinery.trim());
    if (filterVineyard.trim()) params.set('vineyard', filterVineyard.trim());
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`/api/driver-jobs?${params}`, { cache: 'no-store' })
      .then(async (r) => {
        const d = await r.json();
        if (!r.ok) throw new Error(d?.error ?? r.statusText);
        return d;
      })
      .then((d) => {
        if (cancelled) return;
        setRows(Array.isArray(d?.rows) ? (d.rows as DriverJobRow[]) : []);
        setDrivers(Array.isArray(d?.drivers) ? d.drivers.map((x: unknown) => String(x)) : []);
        setDriverUsers(
          Array.isArray(d?.driverUsers)
            ? d.driverUsers.flatMap((user: { value?: unknown; label?: unknown }) => {
                const value = String(user?.value ?? '').trim();
                if (!value) return [];
                const label = String(user?.label ?? '').trim() || value;
                return [{ value, label }];
              })
            : [],
        );
        setWineries(Array.isArray(d?.wineries) ? d.wineries.map((x: unknown) => String(x)) : []);
        setVineyards(Array.isArray(d?.vineyards) ? d.vineyards.map((x: unknown) => String(x)) : []);
        setFenceNames(Array.isArray(d?.fenceNames) ? d.fenceNames.map((x: unknown) => String(x)) : []);
        const loaded = Array.isArray(d?.rows) ? (d.rows as DriverJobRow[]) : [];
        setJobForm((current) => {
          if (editingId == null) return current;
          const row = loaded.find((item) => item.id === editingId);
          if (!row) return current;
          const winery = current.winery_geo_fence.trim() ? current.winery_geo_fence : (row.winery_geo_fence ?? '');
          const vineyard = current.vineyard_geo_fence.trim() ? current.vineyard_geo_fence : (row.vineyard_geo_fence ?? '');
          if (winery === current.winery_geo_fence && vineyard === current.vineyard_geo_fence) return current;
          return { ...current, winery_geo_fence: winery, vineyard_geo_fence: vineyard };
        });
      })
      .catch((e) => {
        if (cancelled) return;
        setRows([]);
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [filterDriver, filterWinery, filterVineyard, listVersion]);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/devices', { cache: 'no-store' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        return data;
      })
      .then((data) => {
        if (cancelled) return;
        const list = Array.isArray(data?.devices) ? data.devices : [];
        const byImei = new Map<string, string[]>();
        for (const device of list as { deviceName?: unknown; imei?: unknown }[]) {
          const imei = String(device.imei ?? '').trim();
          const deviceName = String(device.deviceName ?? '').trim();
          if (!imei) continue;
          const names = byImei.get(imei) ?? [];
          if (deviceName && deviceName !== imei && !names.includes(deviceName)) names.push(deviceName);
          byImei.set(imei, names);
        }
        const options: DeviceOption[] = [...byImei.entries()]
          .map(([imei, names]) => ({
            imei,
            deviceName: names.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' })).join(', '),
          }))
          .sort((a, b) => (a.deviceName || a.imei).localeCompare(b.deviceName || b.imei, undefined, { sensitivity: 'base' }));
        setDevices(options);
      })
      .catch(() => {
        if (!cancelled) setDevices([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const deviceOptions = useMemo(() => {
    if (!jobForm.imei || devices.some((device) => device.imei === jobForm.imei)) return devices;
    return [...devices, { deviceName: jobForm.imei, imei: jobForm.imei }];
  }, [devices, jobForm.imei]);

  const driverOptions = useMemo(() => {
    const current = jobForm.driver.trim();
    if (!current || driverUsers.some((driver) => driver.value === current)) return driverUsers;
    return [{ value: current, label: current }, ...driverUsers];
  }, [driverUsers, jobForm.driver]);

  function setName(key: 'winery_name' | 'vineyard_name', value: string) {
    const fenceKey = key === 'winery_name' ? 'winery_geo_fence' : 'vineyard_geo_fence';
    const exact = fenceNames.find((name) => name.toLowerCase() === value.trim().toLowerCase()) ?? '';
    setJobForm((current) => {
      if (!current[fenceKey].trim() && exact) return { ...current, [key]: value, [fenceKey]: exact };
      return { ...current, [key]: value };
    });
  }

  function askDelete(row: DriverJobRow) {
    setDeleteError(null);
    setPendingDelete(row);
  }

  function closeDelete() {
    if (deleting) return;
    setPendingDelete(null);
    setDeleteError(null);
  }

  useEffect(() => {
    if (!pendingDelete || deleting) return;
    function onKey(event: KeyboardEvent) {
      if (event.key !== 'Escape') return;
      setPendingDelete(null);
      setDeleteError(null);
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [pendingDelete, deleting]);

  async function confirmDelete() {
    if (!pendingDelete || deleting) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      const deletedId = pendingDelete.id;
      const res = await fetch(`/api/driver-jobs/${deletedId}`, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok || data?.ok !== true) throw new Error(data?.error ?? res.statusText);
      if (editingId === deletedId) {
        setEditingId(null);
        setJobForm(emptyJobForm);
        setAddError(null);
      }
      setRows((current) => current.filter((row) => row.id !== deletedId));
      setPendingDelete(null);
      setListVersion((n) => n + 1);
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleting(false);
    }
  }

  function startEdit(row: DriverJobRow) {
    setAddError(null);
    setEditingId(row.id);
    setJobForm({
      job_id: row.job_id ?? '',
      imei: row.imei ?? '',
      driver: row.driver ?? '',
      planned_start_time: row.planned_start_time ?? '',
      winery_name: row.winery_name ?? '',
      vineyard_name: row.vineyard_name ?? '',
      winery_geo_fence: row.winery_geo_fence ?? '',
      vineyard_geo_fence: row.vineyard_geo_fence ?? '',
    });
  }

  async function handleAddJob(e: React.FormEvent) {
    e.preventDefault();
    setAddError(null);
    if (!jobForm.job_id.trim()) {
      setAddError('Job ID is required');
      return;
    }
    setSaving(true);
    try {
      const res = await fetch(editingId == null ? '/api/driver-jobs' : `/api/driver-jobs/${editingId}`, {
        method: editingId == null ? 'POST' : 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(jobForm),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setJobForm(emptyJobForm);
      setEditingId(null);
      if (editingId == null) {
        setFilterDriver('');
        setFilterWinery('');
        setFilterVineyard('');
      }
      setListVersion((n) => n + 1);
    } catch (err) {
      setAddError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  const selectCls =
    'w-full max-w-[16rem] rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800';

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Driver Visibility</h1>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
          Job Queue lists and edits jobs. Job Queue Issues is where a VWork winery, vineyard, or driver name still needs one match. Drivers lists the driver users and the TrackSolid IMEI for each driver ID. Real Time Jobs shows every job on the same step track the driver sees, and keeps a finished job on the list for the debrief. Driver app logins opens the same phone app used at /driver, as the driver you select. Driver app settings holds the diversion reasons.
        </p>
      </div>

      <div className="mb-4 flex gap-2 border-b border-zinc-200 dark:border-zinc-700">
        {(
          [
            ['queue', 'Job Queue'],
            ['issues', issueCount > 0 ? `Job Queue Issues (${issueCount})` : 'Job Queue Issues'],
            ['drivers', driversMissing > 0 ? `Drivers (${driversMissing})` : 'Drivers'],
            ['live', 'Real Time Jobs'],
            ['app', 'Driver app logins'],
            ['app-settings', 'Driver app settings'],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => {
              setTab(id);
              if (id === 'queue') setListVersion((version) => version + 1);
            }}
            className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium ${
              tab === id
                ? 'border-zinc-900 text-zinc-900 dark:border-zinc-100 dark:text-zinc-100'
                : (id === 'issues' && issueCount > 0) || (id === 'drivers' && driversMissing > 0)
                  ? 'border-transparent text-amber-700 dark:text-amber-300'
                  : 'border-transparent text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'issues' && (
        <JobQueueIssues
          showMailConfig
          onChange={setIssueCount}
          onResolved={() => setListVersion((version) => version + 1)}
        />
      )}

      {tab === 'drivers' && <DriverLookup onChange={setDriversMissing} />}

      {tab === 'app' && <DriverAppPreview />}

      {tab === 'app-settings' && (
        <section>
          <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Driver app settings</h2>
          <DriverDiversionSetup />
        </section>
      )}

      {tab === 'live' && <RealTimeJobs />}

      {tab === 'queue' && (
      <>
      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}

      <form
        onSubmit={handleAddJob}
        className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900"
      >
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
          {editingId == null ? 'Add job' : 'Edit job'}
        </h2>
        {addError && <p className="mb-2 text-sm text-red-600 dark:text-red-400">{addError}</p>}
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Job ID</label>
            <input
              type="text"
              value={jobForm.job_id}
              onChange={(e) => setJobForm((f) => ({ ...f, job_id: e.target.value }))}
              className={selectCls}
            />
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Driver</label>
            <select
              value={jobForm.driver}
              onChange={(e) => setJobForm((f) => ({ ...f, driver: e.target.value }))}
              className={selectCls}
            >
              <option value="">Select driver</option>
              {driverOptions.map((driver) => (
                <option key={driver.value} value={driver.value}>
                  {driver.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Planned start</label>
            <input
              type="text"
              value={jobForm.planned_start_time}
              onChange={(e) => setJobForm((f) => ({ ...f, planned_start_time: e.target.value }))}
              placeholder="2026-10-15 08:00"
              className={selectCls}
            />
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Device</label>
            <select
              value={jobForm.imei}
              onChange={(e) => setJobForm((current) => ({ ...current, imei: e.target.value }))}
              className={`${selectCls} max-w-[24rem] font-mono`}
            >
              <option value="">Select device</option>
              {deviceOptions.map((device) => (
                <option key={device.imei} value={device.imei}>
                  {device.deviceName ? `${device.deviceName} · ${device.imei}` : device.imei}
                </option>
              ))}
            </select>
          </div>
          {(
            [
              ['winery_name', 'Winery'],
              ['vineyard_name', 'Vineyard'],
            ] as const
          ).map(([key, label]) => (
            <div key={key}>
              <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">{label}</label>
              <input
                type="text"
                value={jobForm[key]}
                onChange={(e) => setName(key, e.target.value)}
                className={`${selectCls} max-w-[22rem]`}
              />
            </div>
          ))}
          {(
            [
              ['winery_geo_fence', 'Winery geofence'],
              ['vineyard_geo_fence', 'Vineyard geofence'],
            ] as const
          ).map(([key, label]) => (
            <div key={key}>
              <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">{label}</label>
              <select
                value={jobForm[key]}
                onChange={(e) => setJobForm((current) => ({ ...current, [key]: e.target.value }))}
                className={`${selectCls} max-w-[22rem]`}
              >
                <option value="">Select fence</option>
                {fenceNames.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </div>
          ))}
          <button
            type="submit"
            disabled={saving}
            className="rounded bg-blue-600 px-3 py-1 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {saving ? 'Saving…' : editingId == null ? 'Add job' : 'Save job'}
          </button>
          {editingId != null && (
            <button
              type="button"
              onClick={() => {
                setEditingId(null);
                setJobForm(emptyJobForm);
                setAddError(null);
              }}
              className="rounded border border-zinc-300 px-3 py-1 text-xs font-medium text-zinc-700 dark:border-zinc-600 dark:text-zinc-200"
            >
              Cancel
            </button>
          )}
        </div>
      </form>

      <div className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <div className="mb-2 flex items-center justify-between gap-3">
          <h2 className="text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Filters</h2>
          <button
            type="button"
            onClick={() => setListVersion((n) => n + 1)}
            disabled={loading}
            className="rounded border border-zinc-300 px-3 py-1 text-xs font-medium text-zinc-700 disabled:opacity-50 dark:border-zinc-600 dark:text-zinc-200"
          >
            {loading ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Driver</label>
            <select value={filterDriver} onChange={(e) => setFilterDriver(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {drivers.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Winery</label>
            <select value={filterWinery} onChange={(e) => setFilterWinery(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {wineries.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Vineyard</label>
            <select value={filterVineyard} onChange={(e) => setFilterVineyard(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {vineyards.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {loading ? (
        <p className="text-zinc-600 dark:text-zinc-400">Loading…</p>
      ) : (
        <div className="max-h-[72vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="w-full min-w-[52rem] text-left text-xs">
            <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
              <tr className="border-b border-zinc-200 dark:border-zinc-700">
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Job ID
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Status
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Truck ID
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  IMEI
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Driver
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Planned start
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Winery
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Vineyard
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Winery geofence
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Vineyard geofence
                </th>
                <th className="px-2 py-2 text-xs font-medium text-zinc-500 dark:text-zinc-400" />
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={11} className="px-3 py-6 text-sm text-zinc-500 dark:text-zinc-400">
                    No jobs match these filters.
                  </td>
                </tr>
              ) : (
                rows.map((row) => (
                  <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-900 dark:border-zinc-800 dark:text-zinc-100">
                      <div className="text-[11px] font-medium text-zinc-700 dark:text-zinc-200">{cell(row.company_name)}</div>
                      <div className="text-[11px] text-zinc-500 dark:text-zinc-400">{cell(row.group_name)}</div>
                      <div className="font-mono">{cell(row.job_id)}</div>
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-900 dark:border-zinc-800 dark:text-zinc-100">
                      <div className="font-medium">Stage {jobStage(row)}</div>
                      <div className="text-[11px] text-zinc-500 dark:text-zinc-400">{STAGE_NAME[jobStage(row)]}</div>
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.truck_id)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 font-mono text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.imei)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.driver)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 font-mono text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.planned_start_time)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {placeCell(row.winery_name, row.winery_geo_fence)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {placeCell(row.vineyard_name, row.vineyard_geo_fence)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.winery_geo_fence)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.vineyard_geo_fence)}
                    </td>
                    <td className="px-2 py-1.5">
                      <div className="flex items-center gap-2">
                        <button
                          type="button"
                          onClick={() => startEdit(row)}
                          className="font-medium text-blue-700 hover:underline dark:text-blue-300"
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={() => askDelete(row)}
                          aria-label={`Delete job ${row.job_id || row.id}`}
                          title="Delete job"
                          className="rounded p-0.5 text-red-600 hover:bg-red-50 hover:text-red-800 dark:text-red-400 dark:hover:bg-red-950/40 dark:hover:text-red-300"
                        >
                          <TrashIcon />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      <p className="mt-3 text-xs text-zinc-500 dark:text-zinc-400">
        {loading ? '…' : `${rows.length} job${rows.length === 1 ? '' : 's'}`}
      </p>

      {pendingDelete && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="delete-job-title"
        >
          <div className="w-full max-w-lg rounded-lg bg-white p-5 shadow-xl dark:bg-zinc-900">
            <h2 id="delete-job-title" className="text-lg font-semibold text-zinc-900 dark:text-zinc-100">
              Delete this job?
            </h2>
            <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-300">
              This removes the job below from the queue, including its step log and diversions.
            </p>
            <dl className="mt-4 grid grid-cols-[9rem_1fr] gap-x-3 gap-y-1 rounded border border-zinc-200 bg-zinc-50 p-3 text-sm dark:border-zinc-700 dark:bg-zinc-800">
              {(
                [
                  ['Company', pendingDelete.company_name],
                  ['Group', pendingDelete.group_name],
                  ['Job ID', pendingDelete.job_id],
                  ['Truck ID', pendingDelete.truck_id],
                  ['IMEI', pendingDelete.imei],
                  ['Driver', pendingDelete.driver],
                  ['Planned start', pendingDelete.planned_start_time],
                  ['Winery', pendingDelete.winery_name],
                  ['Vineyard', pendingDelete.vineyard_name],
                  ['Winery geofence', pendingDelete.winery_geo_fence],
                  ['Vineyard geofence', pendingDelete.vineyard_geo_fence],
                ] as const
              ).map(([label, value]) => (
                <div key={label} className="contents">
                  <dt className="text-zinc-500 dark:text-zinc-400">{label}</dt>
                  <dd className="font-medium text-zinc-900 dark:text-zinc-100">{cell(value)}</dd>
                </div>
              ))}
            </dl>
            {deleteError && <p className="mt-2 text-sm text-red-600 dark:text-red-400">{deleteError}</p>}
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                disabled={deleting}
                onClick={closeDelete}
                className="rounded border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-600"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={deleting}
                autoFocus
                onClick={() => void confirmDelete()}
                className="rounded bg-red-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-40"
              >
                {deleting ? 'Deleting…' : 'Delete job'}
              </button>
            </div>
          </div>
        </div>
      )}
      </>
      )}
    </div>
  );
}

export default function DriverVisibilityPage() {
  return (
    <StaffOnlyGate>
      <DriverVisibilityPageContent />
    </StaffOnlyGate>
  );
}
