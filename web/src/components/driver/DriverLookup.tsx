'use client';

import { useCallback, useEffect, useState } from 'react';

type DriverRow = {
  userid: string;
  email: string;
  firstname: string | null;
  surname: string | null;
  phone: string | null;
  userType: string;
  customer: string | null;
  truckRego: string | null;
  imei: string | null;
};

type DriversResponse = {
  rows?: DriverRow[];
  missing?: number;
  filled?: { driverId: string; imei: string }[];
  stillMissing?: string[];
  tracksolidDevices?: number;
  jobsUpdated?: number;
  error?: string;
};

function cell(value: string | null | undefined): string {
  const text = value == null ? '' : String(value).trim();
  return text || '—';
}

export function DriverLookup({ onChange }: { onChange?: (missing: number) => void }) {
  const [rows, setRows] = useState<DriverRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [filling, setFilling] = useState(false);

  const load = useCallback(() => {
    fetch('/api/driver-jobs/drivers', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as DriversResponse;
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        onChange?.(data.missing ?? 0);
        setError(null);
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false));
  }, [onChange]);

  useEffect(() => {
    load();
  }, [load]);

  async function fillMissing() {
    setFilling(true);
    setError(null);
    setNote(null);
    try {
      const response = await fetch('/api/driver-jobs/drivers', { method: 'POST' });
      const data = (await response.json()) as DriversResponse;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setRows(data.rows ?? []);
      onChange?.(data.missing ?? 0);
      const filled = data.filled?.length ?? 0;
      const missing = data.stillMissing ?? [];
      const jobs = data.jobsUpdated ?? 0;
      setNote(
        `TrackSolid listed ${data.tracksolidDevices ?? 0} devices. Filled ${filled} driver IMEI${filled === 1 ? '' : 's'}` +
          `${jobs > 0 ? ` and ${jobs} queued job${jobs === 1 ? '' : 's'}` : ''}.` +
          (missing.length > 0 ? ` Still missing: ${missing.join(', ')}.` : ' Every driver ID now has an IMEI.'),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setFilling(false);
    }
  }

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          Drivers from the user list. The IMEI is the TrackSolid device whose name is that driver ID, so a VWork job for the driver can use that device.
        </p>
        <button
          type="button"
          onClick={() => void fillMissing()}
          disabled={filling}
          className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          {filling ? 'Looking up IMEIs…' : 'Fill missing IMEIs'}
        </button>
      </div>
      {error && <div className="mb-4 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>}
      {note && <div className="mb-4 rounded bg-zinc-100 p-3 text-sm text-zinc-800 dark:bg-zinc-800 dark:text-zinc-100">{note}</div>}
      {loading ? (
        <p className="text-sm text-zinc-600">Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Email / driver ID</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Customer</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Truck rego</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">First name</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Surname</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Phone</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">IMEI</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.userid} className="border-b border-zinc-100 dark:border-zinc-800">
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.email)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.userType)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.customer)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.truckRego)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.firstname)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.surname)}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{cell(row.phone)}</td>
                  <td className="whitespace-nowrap px-3 py-2 font-mono text-zinc-700 dark:text-zinc-300">{cell(row.imei)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && <p className="p-4 text-zinc-500">No drivers yet.</p>}
        </div>
      )}
    </div>
  );
}
