'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useEffect, useState } from 'react';
import { useViewMode } from '@/contexts/ViewModeContext';

export default function Sidebar() {
  const pathname = usePathname();
  const { viewMode, setViewMode, clientCustomer, setClientCustomer } = useViewMode();
  const [customers, setCustomers] = useState<string[]>([]);

  useEffect(() => {
    fetch('/api/vworkjobs/customers')
      .then((res) => res.json())
      .then((data) => setCustomers(data?.customers ?? []))
      .catch(() => setCustomers([]));
  }, []);

  return (
    <aside className="w-56 shrink-0 border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
      <nav className="flex flex-col gap-1 p-3">
        <Link
          href="/"
          className={`rounded px-3 py-2 text-sm font-medium ${
            pathname === '/' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Home
        </Link>
        <div className="mt-2 px-1">
          <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">View</div>
          <div className="flex flex-col gap-1">
            {(['super', 'admin', 'client'] as const).map((mode) => (
              <div key={mode}>
                <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800">
                  <input
                    type="radio"
                    name="viewMode"
                    checked={viewMode === mode}
                    onChange={() => setViewMode(mode)}
                    className="border-zinc-300 text-blue-600 dark:border-zinc-600"
                  />
                  <span className={viewMode === mode ? 'font-medium text-zinc-900 dark:text-zinc-100' : 'text-zinc-600 dark:text-zinc-400'}>
                    {mode === 'super' ? 'Super' : mode === 'admin' ? 'Admin' : 'Client'}
                  </span>
                </label>
                {mode === 'client' && (
                  <div className="ml-5 mt-1 mb-1">
                    <label className="mb-0.5 block text-xs font-medium text-zinc-500 dark:text-zinc-400">Customer</label>
                    <select
                      value={clientCustomer}
                      onChange={(e) => setClientCustomer(e.target.value)}
                      className="w-full rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                    >
                      <option value="">— Select —</option>
                      {customers.map((c) => (
                        <option key={c} value={c}>{c}</option>
                      ))}
                    </select>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
        <div className="my-1 border-t border-zinc-200 dark:border-zinc-700" />
        <div className="mt-2 rounded-md border-l-4 border-blue-500 bg-blue-50 px-3 py-1.5 text-xs font-bold uppercase tracking-wider text-blue-800 dark:border-blue-400 dark:bg-blue-950/50 dark:text-blue-200">
          Query
        </div>
        <Link
          href="/query/vwork"
          className={`rounded px-3 py-2 text-sm ${
            pathname === '/query/vwork' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Vwork
        </Link>
        <Link
          href="/query/gpsdata"
          className={`rounded px-3 py-2 text-sm ${
            pathname === '/query/gpsdata' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          GPS Tracking
        </Link>
        <Link
          href="/query/inspect"
          className={`rounded px-3 py-2 text-sm ${
            pathname === '/query/inspect' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Inspect
        </Link>
        <Link
          href="/query/summary"
          className={`rounded px-3 py-2 text-sm ${
            pathname === '/query/summary' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Summary
        </Link>
        <div className="my-1 border-t border-zinc-200 dark:border-zinc-700" />
        <div className="mt-2 rounded-md border-l-4 border-amber-500 bg-amber-50 px-3 py-1.5 text-xs font-bold uppercase tracking-wider text-amber-800 dark:border-amber-400 dark:bg-amber-950/50 dark:text-amber-200">
          Admin
        </div>
          <Link
            href="/admin/users"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/users' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            Users
          </Link>
          <Link
            href="/admin/autoruns"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/autoruns' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            AutoRuns
          </Link>
          <Link
            href="/admin/gps-mappings"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/gps-mappings' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            GPS Mappings
          </Link>
          <Link
            href="/admin/import-geofences"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/import-geofences' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            Import Geofences
          </Link>
          <Link
            href="/admin/tagging"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/tagging' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            Tagging
          </Link>
          <Link
            href="/admin/settings"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/settings' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            Settings
          </Link>
          <Link
            href="/admin/api-test"
            className={`rounded px-3 py-2 text-sm ${
              pathname === '/admin/api-test' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
            }`}
          >
            API GPS Import
          </Link>
      </nav>
    </aside>
  );
}
