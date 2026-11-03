'use client';

import { useEffect, useMemo, useState } from 'react';
import StaffOnlyGate from '@/components/StaffOnlyGate';
import DriverAppPreview from '@/components/driver/DriverAppPreview';
import RealTimeJobs from '@/components/driver/RealTimeJobs';

type DriverJobRow = {
  id: number;
  job_id: string;
  truck_id: string | null;
  imei: string | null;
  driver: string | null;
  planned_start_time: string | null;
  winery_name: string | null;
  vineyard_name: string | null;
  winery_geo_fence: string | null;
  vineyard_geo_fence: string | null;
};

type DeviceOption = { deviceName: string; imei: string };

function cell(value: string | null | undefined): string {
  const s = value == null ? '' : String(value).trim();
  return s || '—';
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
  const [tab, setTab] = useState<'queue' | 'live' | 'app'>('queue');

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
        setWineries(Array.isArray(d?.wineries) ? d.wineries.map((x: unknown) => String(x)) : []);
        setVineyards(Array.isArray(d?.vineyards) ? d.vineyards.map((x: unknown) => String(x)) : []);
        setFenceNames(Array.isArray(d?.fenceNames) ? d.fenceNames.map((x: unknown) => String(x)) : []);
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

  function setFence(key: 'winery_name' | 'vineyard_name' | 'winery_geo_fence' | 'vineyard_geo_fence', value: string) {
    setJobForm((current) => {
      if (key === 'winery_name' && !current.winery_geo_fence.trim()) {
        return { ...current, winery_name: value, winery_geo_fence: value };
      }
      if (key === 'vineyard_name' && !current.vineyard_geo_fence.trim()) {
        return { ...current, vineyard_name: value, vineyard_geo_fence: value };
      }
      return { ...current, [key]: value };
    });
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
          Job Queue lists and edits jobs. Real Time Jobs shows each open job on the same step track the driver sees. The driver app tab opens the same phone app used at /driver, as the driver you select.
        </p>
      </div>

      <div className="mb-4 flex gap-2 border-b border-zinc-200 dark:border-zinc-700">
        {(
          [
            ['queue', 'Job Queue'],
            ['live', 'Real Time Jobs'],
            ['app', 'Driver app'],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => setTab(id)}
            className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium ${
              tab === id
                ? 'border-zinc-900 text-zinc-900 dark:border-zinc-100 dark:text-zinc-100'
                : 'border-transparent text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'app' && <DriverAppPreview />}

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
          {(
            [
              ['job_id', 'Job ID'],
              ['driver', 'Driver'],
              ['planned_start_time', 'Planned start'],
            ] as const
          ).map(([key, label]) => (
            <div key={key}>
              <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">{label}</label>
              <input
                type="text"
                value={jobForm[key]}
                onChange={(e) => setJobForm((f) => ({ ...f, [key]: e.target.value }))}
                placeholder={key === 'planned_start_time' ? '2026-10-15 08:00' : undefined}
                className={selectCls}
              />
            </div>
          ))}
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
              ['winery_geo_fence', 'Winery geofence'],
              ['vineyard_geo_fence', 'Vineyard geofence'],
            ] as const
          ).map(([key, label]) => (
            <div key={key}>
              <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">{label}</label>
              <select
                value={jobForm[key]}
                onChange={(e) => setFence(key, e.target.value)}
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
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Filters</h2>
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
                  <td colSpan={10} className="px-3 py-6 text-sm text-zinc-500 dark:text-zinc-400">
                    No jobs match these filters.
                  </td>
                </tr>
              ) : (
                rows.map((row) => (
                  <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="border-r border-zinc-100 px-2 py-1.5 font-mono text-zinc-900 dark:border-zinc-800 dark:text-zinc-100">
                      {cell(row.job_id)}
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
                      {cell(row.winery_name)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.vineyard_name)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.winery_geo_fence)}
                    </td>
                    <td className="border-r border-zinc-100 px-2 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200">
                      {cell(row.vineyard_geo_fence)}
                    </td>
                    <td className="px-2 py-1.5">
                      <button
                        type="button"
                        onClick={() => startEdit(row)}
                        className="font-medium text-blue-700 hover:underline dark:text-blue-300"
                      >
                        Edit
                      </button>
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
