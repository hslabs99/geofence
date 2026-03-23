'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { createPortal } from 'react-dom';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { GEODATA_USER_STORAGE_KEY, useViewMode } from '@/contexts/ViewModeContext';
import { useSummaryHistory } from '@/contexts/SummaryHistoryContext';
import { listSummaryHistory } from '@/lib/summary-history-storage';

type InspectHistoryEntry = {
  job_id: string;
  delivery_winery: string | null;
  vineyard_name: string | null;
  worker: string | null;
  actual_start_time: string | null;
  truck_id: string | null;
};

function buildInspectUrl(entry: InspectHistoryEntry): string {
  const params = new URLSearchParams();
  params.set('locateJobId', entry.job_id);
  if (entry.truck_id) params.set('truckId', entry.truck_id);
  const actualStart = entry.actual_start_time?.trim();
  if (actualStart) {
    const d = new Date(actualStart.includes('T') ? actualStart : actualStart.replace(' ', 'T'));
    if (!Number.isNaN(d.getTime())) {
      const pad = (n: number) => String(n).padStart(2, '0');
      const from = new Date(d);
      from.setDate(from.getDate() - 1);
      params.set('actualFrom', `${from.getFullYear()}-${pad(from.getMonth() + 1)}-${pad(from.getDate())}`);
      const to = new Date(d);
      to.setDate(to.getDate() + 1);
      params.set('actualTo', `${to.getFullYear()}-${pad(to.getMonth() + 1)}-${pad(to.getDate())}`);
    }
  }
  return `/query/inspect?${params.toString()}`;
}

/** Renders in document.body so it stacks above <main> and is not clipped by sidebar overflow. */
function SidebarFlyoutPortal({
  open,
  anchorRef,
  contentRef,
  children,
}: {
  open: boolean;
  anchorRef: React.RefObject<HTMLElement | null>;
  contentRef: React.RefObject<HTMLDivElement | null>;
  children: React.ReactNode;
}) {
  const [pos, setPos] = useState({ top: 0, left: 0 });

  useLayoutEffect(() => {
    if (!open || !anchorRef.current) return;
    const update = () => {
      const el = anchorRef.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      const gap = 6;
      let left = r.right + gap;
      const minW = 420;
      if (left + minW > window.innerWidth - 8) {
        left = Math.max(8, r.left - minW - gap);
      }
      setPos({ top: Math.max(8, r.top), left });
    };
    update();
    window.addEventListener('scroll', update, true);
    window.addEventListener('resize', update);
    return () => {
      window.removeEventListener('scroll', update, true);
      window.removeEventListener('resize', update);
    };
  }, [open, anchorRef]);

  if (!open || typeof document === 'undefined') return null;

  return createPortal(
    <div
      ref={contentRef}
      className="fixed z-[10000] max-h-[min(80vh,480px)] overflow-y-auto rounded-lg border border-zinc-200 bg-white py-1 shadow-xl dark:border-zinc-600 dark:bg-zinc-800"
      style={{
        top: pos.top,
        left: pos.left,
        minWidth: 420,
        maxWidth: 'min(90vw, 560px)',
      }}
      role="listbox"
    >
      {children}
    </div>,
    document.body
  );
}

function RecentViewsDropdown({
  containerRef,
  flyoutRef,
  open,
  onToggle,
  onClose,
}: {
  containerRef: React.RefObject<HTMLDivElement | null>;
  flyoutRef: React.RefObject<HTMLDivElement | null>;
  open: boolean;
  onToggle: () => void;
  onClose: () => void;
}) {
  const router = useRouter();
  const { historyVersion } = useSummaryHistory();
  const entries = useMemo(() => listSummaryHistory(), [historyVersion]);
  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onToggle();
        }}
        className="w-full rounded px-3 py-1.5 text-left text-xs text-zinc-500 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
      >
        Recent views {entries.length > 0 ? `(${entries.length})` : ''}
      </button>
      <SidebarFlyoutPortal open={open} anchorRef={containerRef} contentRef={flyoutRef}>
        {entries.length === 0 ? (
          <div className="px-3 py-2 text-xs text-zinc-500 dark:text-zinc-400">
            Leave Summary with filters set, then open another page — your view is saved here.
          </div>
        ) : (
          entries.map((e) => (
            <button
              key={e.id}
              type="button"
              role="option"
              title={e.label}
              onClick={() => {
                onClose();
                router.push(`/query/summary?sh=${encodeURIComponent(e.id)}`);
              }}
              className="w-full px-3 py-2 text-left text-xs hover:bg-zinc-100 dark:hover:bg-zinc-700"
            >
              <div className="text-zinc-900 dark:text-zinc-100">{e.label}</div>
            </button>
          ))
        )}
      </SidebarFlyoutPortal>
    </div>
  );
}

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const { viewMode, setViewMode, clientCustomer, setClientCustomer, clientCustomerLocked, allowedViewModes, userType, refreshUser } = useViewMode();
  const [customers, setCustomers] = useState<string[]>([]);
  const [inspectHistory, setInspectHistory] = useState<InspectHistoryEntry[]>([]);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [recentViewsOpen, setRecentViewsOpen] = useState(false);
  const historyRef = useRef<HTMLDivElement>(null);
  const recentViewsRef = useRef<HTMLDivElement>(null);
  const recentViewsFlyoutRef = useRef<HTMLDivElement>(null);
  const inspectFlyoutRef = useRef<HTMLDivElement>(null);

  const fetchInspectHistory = useCallback(() => {
    fetch('/api/inspect-history')
      .then((r) => r.json())
      .then((data) => setInspectHistory(data?.entries ?? []))
      .catch(() => setInspectHistory([]));
  }, []);

  useEffect(() => {
    fetchInspectHistory();
  }, [fetchInspectHistory]);

  useEffect(() => {
    if (!historyOpen && !recentViewsOpen) return;
    const close = (e: MouseEvent) => {
      const t = e.target as Node;
      if (historyRef.current?.contains(t)) return;
      if (recentViewsRef.current?.contains(t)) return;
      if (inspectFlyoutRef.current?.contains(t)) return;
      if (recentViewsFlyoutRef.current?.contains(t)) return;
      setHistoryOpen(false);
      setRecentViewsOpen(false);
    };
    document.addEventListener('click', close);
    return () => document.removeEventListener('click', close);
  }, [historyOpen, recentViewsOpen]);

  function handleSignOut() {
    if (typeof localStorage !== 'undefined') localStorage.removeItem(GEODATA_USER_STORAGE_KEY);
    refreshUser();
    router.push('/login');
  }

  useEffect(() => {
    fetch('/api/vworkjobs/customers')
      .then((res) => res.json())
      .then((data) => setCustomers(data?.customers ?? []))
      .catch(() => setCustomers([]));
  }, []);

  return (
    <aside className="flex min-h-screen w-56 shrink-0 flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
      <nav className="flex min-h-0 flex-1 flex-col gap-1 overflow-y-auto overflow-x-hidden p-3">
        <Link
          href="/"
          className={`rounded px-3 py-2 text-sm font-medium ${
            pathname === '/' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
          }`}
        >
          Home
        </Link>
        {allowedViewModes.length > 0 && (
        <div className="mt-2 px-1">
          <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">View</div>
          <div className="flex flex-col gap-1">
            {(['super', 'admin', 'client'] as const)
              .filter((mode) => allowedViewModes.includes(mode))
              .map((mode) => (
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
              </div>
            ))}
          </div>
          {/* Customer filter: show in both Admin and Client view so both can filter by client. */}
          {(viewMode === 'admin' || viewMode === 'client') && (
            <div className="mt-2 ml-1">
              <label className="mb-0.5 block text-xs font-medium text-zinc-500 dark:text-zinc-400">
                Customer {clientCustomerLocked && '(locked)'}
              </label>
              {clientCustomerLocked ? (
                <select
                  value={clientCustomer}
                  disabled
                  className="w-full rounded border border-zinc-300 bg-zinc-100 px-2 py-1 text-sm text-zinc-700 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
                >
                  <option value={clientCustomer || ''}>{clientCustomer || '—'}</option>
                </select>
              ) : (
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
              )}
            </div>
          )}
        </div>
        )}
        <div className="my-1 border-t border-zinc-200 dark:border-zinc-700" />
        {viewMode === 'client' ? (
          <>
            <Link
              href="/query/summary"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/query/summary' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Summary
            </Link>
            {userType && (
              <RecentViewsDropdown
                containerRef={recentViewsRef}
                flyoutRef={recentViewsFlyoutRef}
                open={recentViewsOpen}
                onToggle={() => setRecentViewsOpen((o) => !o)}
                onClose={() => setRecentViewsOpen(false)}
              />
            )}
          </>
        ) : (
          <>
            <div className="mt-2 rounded-md border-l-4 border-blue-500 bg-blue-50 px-3 py-1.5 text-xs font-bold uppercase tracking-wider text-blue-800 dark:border-blue-400 dark:bg-blue-950/50 dark:text-blue-200">
              Query
            </div>
            <Link
              href="/query/summary"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/query/summary' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Summary
            </Link>
            {userType && (
              <RecentViewsDropdown
                containerRef={recentViewsRef}
                flyoutRef={recentViewsFlyoutRef}
                open={recentViewsOpen}
                onToggle={() => setRecentViewsOpen((o) => !o)}
                onClose={() => setRecentViewsOpen(false)}
              />
            )}
            <Link
              href="/query/inspect"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/query/inspect' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Inspect
            </Link>
            <div className="relative" ref={historyRef}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setHistoryOpen((o) => {
                    if (!o) fetchInspectHistory();
                    return !o;
                  });
                }}
                className="w-full rounded px-3 py-1.5 text-left text-xs text-zinc-500 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
              >
                Recent jobs {inspectHistory.length > 0 ? `(${inspectHistory.length})` : ''}
              </button>
              <SidebarFlyoutPortal open={historyOpen} anchorRef={historyRef} contentRef={inspectFlyoutRef}>
                {inspectHistory.length === 0 ? (
                  <div className="px-3 py-2 text-xs text-zinc-500 dark:text-zinc-400">No recent jobs</div>
                ) : (
                  inspectHistory.map((entry) => (
                    <button
                      key={entry.job_id}
                      type="button"
                      role="option"
                      onClick={() => {
                        setHistoryOpen(false);
                        setInspectHistory((prev) => {
                          const rest = prev.filter((e) => String(e.job_id).trim() !== String(entry.job_id).trim());
                          return [entry, ...rest];
                        });
                        router.push(buildInspectUrl(entry));
                      }}
                      className="w-full px-3 py-2 text-left text-xs hover:bg-zinc-100 dark:hover:bg-zinc-700"
                    >
                      <div className="font-medium text-zinc-900 dark:text-zinc-100">{entry.job_id}</div>
                      <div className="mt-0.5 truncate text-zinc-600 dark:text-zinc-400">
                        {[entry.delivery_winery, entry.vineyard_name].filter(Boolean).join(' · ') || '—'}
                      </div>
                      <div className="mt-0.5 flex flex-wrap gap-x-2 gap-y-0 text-zinc-500 dark:text-zinc-500">
                        {entry.worker && <span>{entry.worker}</span>}
                        {entry.actual_start_time && <span>{entry.actual_start_time}</span>}
                      </div>
                    </button>
                  ))
                )}
              </SidebarFlyoutPortal>
            </div>
            <Link
              href="/query/gpsdata"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/query/gpsdata' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              GPS Tracking
            </Link>
            <Link
              href="/query/vwork"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/query/vwork' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Vwork
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
              href="/admin/logs"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/admin/logs' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Logs
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
              href="/admin/data-checks"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/admin/data-checks' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Data Checks
            </Link>
            <Link
              href="/admin/import-geofences"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/admin/import-geofences' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              GeoFences
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
              href="/admin/winery-minutes"
              className={`rounded px-3 py-2 text-sm ${
                pathname === '/admin/winery-minutes' ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Winery Minutes
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
          </>
        )}
      </nav>
      {userType && (
        <div className="shrink-0 border-t border-zinc-200 p-3 dark:border-zinc-700">
          <button
            type="button"
            onClick={handleSignOut}
            className="w-full rounded px-3 py-2 text-left text-sm text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
          >
            Sign out
          </button>
        </div>
      )}
    </aside>
  );
}
