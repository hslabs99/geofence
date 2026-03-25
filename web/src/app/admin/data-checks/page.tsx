'use client';

import { Suspense, useState, useEffect, useCallback } from 'react';
import { useSearchParams } from 'next/navigation';

type SummaryRow = {
  report_date: string;
  device_name: string;
  max_gap_minutes: string;
  gap_count: number;
  day_count: number;
  gap_from: string | null;
  gap_to: string | null;
  gap_from_nz: string;
  gap_to_nz: string;
  gap_start_lat?: number | null;
  gap_start_lon?: number | null;
  gap_end_lat?: number | null;
  gap_end_lon?: number | null;
};

/** True if device moved between outage start and end (lat/lon at From !== at To). */
function deviceMovedDuringOutage(row: SummaryRow): boolean {
  const slat = row.gap_start_lat;
  const slon = row.gap_start_lon;
  const elat = row.gap_end_lat;
  const elon = row.gap_end_lon;
  if (slat == null || slon == null || elat == null || elon == null) return false;
  return slat !== elat || slon !== elon;
}

type TrackingWindowRow = {
  device_name: string;
  position_time: string | null;
  position_time_nz: string | null;
  fence_name: string | null;
  geofence_type: string | null;
  lat: number | null;
  lon: number | null;
};

type DetailRow = {
  device_name: string;
  gap_start: string;
  gap_end: string;
  gap_minutes: string;
};

type GapsResult = {
  summary: SummaryRow[];
  detail: DetailRow[];
  minGapMinutes: number;
  dateFrom: string | null;
  dateTo: string | null;
};

type WineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
  created_at: string;
};

type VineyardGroupRow = {
  id: number;
  winery_name: string | null;
  vineyard_name: string;
  vineyard_group: string;
  created_at: string;
};

function formatDateInput(value: string): string {
  if (!value) return '';
  const d = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  return value;
}

/** Format YYYY-MM-DD as dd/mm/yy */
function formatDateDDMMYY(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  return `${m[3]}/${m[2]}/${yy}`;
}

/** Format YYYY-MM-DD HH:MI:SS as dd/mm hh:mm */
function formatGapDateTime(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  const h = m[4].padStart(2, '0');
  return `${m[3]}/${m[2]}/${yy} ${h}:${m[5]}`;
}

/** Haversine distance in meters between two lat/lon points. */
function distanceMeters(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth radius in meters
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/** Format meters as "X m" or "X.X km". */
function formatDistance(m: number): string {
  if (m >= 1000) return `${(m / 1000).toFixed(1)} km`;
  return `${Math.round(m)} m`;
}

const SORT_KEYS = [
  { value: 'report_date', label: 'Date' },
  { value: 'device_name', label: 'Device' },
  { value: 'max_gap_minutes', label: 'Max gap (min)' },
  { value: 'gap_count', label: '# gaps' },
  { value: 'day_count', label: 'DayCount' },
  { value: 'gap_from', label: 'From' },
  { value: 'gap_to', label: 'To' },
  { value: 'gap_from_nz', label: 'From_NZ' },
  { value: 'gap_to_nz', label: 'To_NZ' },
] as const;

type SortKey = (typeof SORT_KEYS)[number]['value'];

function getSortValue(row: SummaryRow, key: SortKey): string | number {
  const v = row[key];
  if (key === 'max_gap_minutes') return parseFloat(String(v)) ?? 0;
  if (key === 'gap_count' || key === 'day_count') return Number(v) ?? 0;
  return (v ?? '') as string;
}

function sortSummary(rows: SummaryRow[], sortBy1: SortKey, sortBy2: SortKey, dir: 'asc' | 'desc'): SummaryRow[] {
  const mult = dir === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {
    const v1a = getSortValue(a, sortBy1);
    const v1b = getSortValue(b, sortBy1);
    let cmp = v1a < v1b ? -1 : v1a > v1b ? 1 : 0;
    if (cmp !== 0) return mult * cmp;
    const v2a = getSortValue(a, sortBy2);
    const v2b = getSortValue(b, sortBy2);
    cmp = v2a < v2b ? -1 : v2a > v2b ? 1 : 0;
    return mult * cmp;
  });
}

type GpsIntegrityLogEntry = {
  date: string;
  device: string;
  outcomes: ('true' | 'not found')[];
  sampleTimes: string[];
};

type GpsIntegrityCell =
  | { status: 'no_data' }
  | {
      status: 'data';
      firstOk: boolean;
      lastOk: boolean;
      countOk: boolean;
      firstTime: string;
      lastTime: string;
      apiCount: number;
      dbCount: number;
      dbFirstTime: string | null;
      dbLastTime: string | null;
    };

type GpsIntegrityGridResponse = {
  dates: string[];
  devices: string[];
  rows: Array<{ date: string; cells: GpsIntegrityCell[] }>;
};

type DataChecksTab = 'gps-gaps' | 'winery-fixes' | 'varied' | 'gps-integrity' | 'vineyard-mappings';

function DataChecksPageContent() {
  const searchParams = useSearchParams();
  const tabParam = searchParams.get('tab');
  const [activeTab, setActiveTab] = useState<DataChecksTab>(() => {
    if (tabParam === 'vineyard-mappings') return 'vineyard-mappings';
    if (tabParam === 'gps-integrity') return 'gps-integrity';
    if (tabParam === 'winery-fixes') return 'winery-fixes';
    if (tabParam === 'varied') return 'varied';
    if (tabParam === 'gps-gaps') return 'gps-gaps';
    return 'gps-gaps';
  });

  useEffect(() => {
    if (tabParam === 'vineyard-mappings') setActiveTab('vineyard-mappings');
    else if (tabParam === 'gps-integrity') setActiveTab('gps-integrity');
    else if (tabParam === 'winery-fixes') setActiveTab('winery-fixes');
    else if (tabParam === 'varied') setActiveTab('varied');
    else if (tabParam === 'gps-gaps') setActiveTab('gps-gaps');
  }, [tabParam]);
  const [minGapMinutes, setMinGapMinutes] = useState<string>('60');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GapsResult | null>(null);
  const [showDetail, setShowDetail] = useState(false);
  const [popupRow, setPopupRow] = useState<SummaryRow | null>(null);
  const [popupLoading, setPopupLoading] = useState(false);
  const [popupRows, setPopupRows] = useState<TrackingWindowRow[]>([]);
  const [popupError, setPopupError] = useState<string | null>(null);
  const [deviceFilter, setDeviceFilter] = useState('');
  const [sortBy1, setSortBy1] = useState<SortKey>('device_name');
  const [sortBy2, setSortBy2] = useState<SortKey>('gap_from');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  // Winery / vineyard name fixes (tab 2)
  const [wineMappRows, setWineMappRows] = useState<WineMappRow[]>([]);
  const [wineMappLoading, setWineMappLoading] = useState(false);
  const [wineMappError, setWineMappError] = useState<string | null>(null);
  const [wineMappNewOld, setWineMappNewOld] = useState('');
  const [wineMappNewNew, setWineMappNewNew] = useState('');
  const [wineMappSaving, setWineMappSaving] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editOld, setEditOld] = useState('');
  const [editNew, setEditNew] = useState('');
  const [deliveryWineries, setDeliveryWineries] = useState<string[]>([]);
  const [runFixesLoading, setRunFixesLoading] = useState(false);
  const [runFixesResult, setRunFixesResult] = useState<{ totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] } | null>(null);

  // Vineyard name fixes (same tab as winery)
  const [vineMappRows, setVineMappRows] = useState<WineMappRow[]>([]);
  const [vineMappLoading, setVineMappLoading] = useState(false);
  const [vineMappError, setVineMappError] = useState<string | null>(null);
  const [vineMappNewOld, setVineMappNewOld] = useState('');
  const [vineMappNewNew, setVineMappNewNew] = useState('');
  const [vineMappSaving, setVineMappSaving] = useState(false);
  const [vineEditingId, setVineEditingId] = useState<number | null>(null);
  const [vineEditOld, setVineEditOld] = useState('');
  const [vineEditNew, setVineEditNew] = useState('');
  const [vineyardNamesList, setVineyardNamesList] = useState<string[]>([]);
  const [runVineFixesLoading, setRunVineFixesLoading] = useState(false);
  const [runVineFixesResult, setRunVineFixesResult] = useState<{
    totalUpdated: number;
    perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[];
  } | null>(null);

  // Varied tab: Set Trailer Type
  const [setTrailerLoading, setSetTrailerLoading] = useState(false);
  const [setTrailerError, setSetTrailerError] = useState<string | null>(null);
  const [setTrailerResult, setSetTrailerResult] = useState<{ updatedTT: number; updatedT: number; totalUpdated: number } | null>(null);

  // Varied tab: Update Vineyard_Group (also used in vineyard-mappings tab)
  const [vineyardGroupLoading, setVineyardGroupLoading] = useState(false);
  const [vineyardGroupError, setVineyardGroupError] = useState<string | null>(null);
  const [vineyardGroupResult, setVineyardGroupResult] = useState<{ setToNa: number; matched: number; totalRows: number } | null>(null);

  // Vineyard Mappings tab (tab 4)
  const [vgRows, setVgRows] = useState<VineyardGroupRow[]>([]);
  const [vgLoading, setVgLoading] = useState(false);
  const [vgError, setVgError] = useState<string | null>(null);
  const [vgSaving, setVgSaving] = useState(false);
  const [vgDeliveryWineries, setVgDeliveryWineries] = useState<string[]>([]);
  const [vgVineyardNames, setVgVineyardNames] = useState<string[]>([]);
  const [vgNewWinery, setVgNewWinery] = useState('');
  const [vgNewVineyard, setVgNewVineyard] = useState('');
  const [vgNewGroup, setVgNewGroup] = useState('');
  const [vgEditingId, setVgEditingId] = useState<number | null>(null);
  const [vgEditWinery, setVgEditWinery] = useState('');
  const [vgEditVineyard, setVgEditVineyard] = useState('');
  const [vgEditGroup, setVgEditGroup] = useState('');

  // GPS Integrity Check tab (tab 4): grid = rows (dates) × columns (devices), 1st row check vs tbl_tracking.position_time
  const [gpsIntegrityFrom, setGpsIntegrityFrom] = useState('');
  const [gpsIntegrityTo, setGpsIntegrityTo] = useState('');
  const [gpsIntegrityLoading, setGpsIntegrityLoading] = useState(false);
  const [gpsIntegrityError, setGpsIntegrityError] = useState<string | null>(null);
  const [gpsIntegrityGrid, setGpsIntegrityGrid] = useState<GpsIntegrityGridResponse | null>(null);

  const fetchWineMapp = useCallback(() => {
    setWineMappLoading(true);
    setWineMappError(null);
    fetch('/api/admin/data-checks/wine-mapp')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: WineMappRow[] }) => setWineMappRows(data.rows ?? []))
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappLoading(false));
  }, []);

  const fetchVineMapp = useCallback(() => {
    setVineMappLoading(true);
    setVineMappError(null);
    fetch('/api/admin/data-checks/vine-mapp')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: WineMappRow[] }) => setVineMappRows(data.rows ?? []))
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappLoading(false));
  }, []);

  useEffect(() => {
    if (activeTab === 'winery-fixes') {
      fetchWineMapp();
      fetchVineMapp();
      fetch('/api/admin/data-checks/wine-mapp/delivery-wineries')
        .then((r) => {
          if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
          return r.json();
        })
        .then((data: { rows: string[] }) => setDeliveryWineries(data.rows ?? []))
        .catch(() => setDeliveryWineries([]));
      fetch('/api/admin/data-checks/vine-mapp/vineyard-names')
        .then((r) => {
          if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
          return r.json();
        })
        .then((data: { rows: string[] }) => setVineyardNamesList(data.rows ?? []))
        .catch(() => setVineyardNamesList([]));
    }
  }, [activeTab, fetchWineMapp, fetchVineMapp]);

  const runGapsScan = () => {
    const minutes = parseFloat(minGapMinutes);
    if (Number.isNaN(minutes) || minutes < 0) {
      setError('Enter a valid gap threshold (minutes).');
      return;
    }
    setError(null);
    setLoading(true);
    setResult(null);
    const params = new URLSearchParams({ minGapMinutes: String(minutes) });
    if (dateFrom.trim()) params.set('dateFrom', formatDateInput(dateFrom.trim()));
    if (dateTo.trim()) params.set('dateTo', formatDateInput(dateTo.trim()));
    fetch(`/api/admin/data-checks/gps-gaps?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: GapsResult) => {
        setResult(data);
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setLoading(false));
  };

  const openTrackingPopup = (row: SummaryRow) => {
    if (row.gap_from == null || row.gap_to == null) return;
    setPopupRow(row);
    setPopupError(null);
    setPopupRows([]);
    setPopupLoading(true);
    const params = new URLSearchParams({
      device: row.device_name,
      from: row.gap_from,
      to: row.gap_to,
    });
    fetch(`/api/admin/data-checks/tracking-window?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: TrackingWindowRow[] }) => {
        setPopupRows(data.rows ?? []);
      })
      .catch((e) => {
        setPopupError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setPopupLoading(false));
  };

  const createWineMapp = () => {
    const oldV = wineMappNewOld.trim();
    const newV = wineMappNewNew.trim();
    if (!oldV || !newV) {
      setWineMappError('Old name and new name are required.');
      return;
    }
    setWineMappError(null);
    setWineMappSaving(true);
    fetch('/api/admin/data-checks/wine-mapp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setWineMappNewOld('');
        setWineMappNewNew('');
        fetchWineMapp();
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const updateWineMapp = (id: number) => {
    const oldV = editOld.trim();
    const newV = editNew.trim();
    if (!oldV || !newV) {
      setWineMappError('Old name and new name are required.');
      return;
    }
    setWineMappError(null);
    setWineMappSaving(true);
    fetch(`/api/admin/data-checks/wine-mapp/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setEditingId(null);
        fetchWineMapp();
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const deleteWineMapp = (id: number) => {
    if (!confirm('Delete this winery name fix?')) return;
    setWineMappError(null);
    setWineMappSaving(true);
    fetch(`/api/admin/data-checks/wine-mapp/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchWineMapp())
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const startEdit = (row: WineMappRow) => {
    setEditingId(row.id);
    setEditOld(row.oldvworkname);
    setEditNew(row.newvworkname);
  };

  const runFixes = () => {
    setWineMappError(null);
    setRunFixesResult(null);
    setRunFixesLoading(true);
    fetch('/api/admin/data-checks/wine-mapp/run-fixes', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] }) => {
        setRunFixesResult({ totalUpdated: data.totalUpdated, perMapping: data.perMapping ?? [] });
        setDeliveryWineries([]);
        fetch('/api/admin/data-checks/wine-mapp/delivery-wineries')
          .then((res) => res.ok ? res.json() : { rows: [] })
          .then((d: { rows: string[] }) => setDeliveryWineries(d.rows ?? []));
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setRunFixesLoading(false));
  };

  const createVineMapp = () => {
    const oldV = vineMappNewOld.trim();
    const newV = vineMappNewNew.trim();
    if (!oldV || !newV) {
      setVineMappError('Old name and new name are required.');
      return;
    }
    setVineMappError(null);
    setVineMappSaving(true);
    fetch('/api/admin/data-checks/vine-mapp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVineMappNewOld('');
        setVineMappNewNew('');
        fetchVineMapp();
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const updateVineMapp = (id: number) => {
    const oldV = vineEditOld.trim();
    const newV = vineEditNew.trim();
    if (!oldV || !newV) {
      setVineMappError('Old name and new name are required.');
      return;
    }
    setVineMappError(null);
    setVineMappSaving(true);
    fetch(`/api/admin/data-checks/vine-mapp/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVineEditingId(null);
        fetchVineMapp();
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const deleteVineMapp = (id: number) => {
    if (!confirm('Delete this vineyard name fix?')) return;
    setVineMappError(null);
    setVineMappSaving(true);
    fetch(`/api/admin/data-checks/vine-mapp/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchVineMapp())
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const startEditVine = (row: WineMappRow) => {
    setVineEditingId(row.id);
    setVineEditOld(row.oldvworkname);
    setVineEditNew(row.newvworkname);
  };

  const runVineFixes = () => {
    setVineMappError(null);
    setRunVineFixesResult(null);
    setRunVineFixesLoading(true);
    fetch('/api/admin/data-checks/vine-mapp/run-fixes', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] }) => {
        setRunVineFixesResult({ totalUpdated: data.totalUpdated, perMapping: data.perMapping ?? [] });
        setVineyardNamesList([]);
        fetch('/api/admin/data-checks/vine-mapp/vineyard-names')
          .then((res) => (res.ok ? res.json() : { rows: [] }))
          .then((d: { rows: string[] }) => setVineyardNamesList(d.rows ?? []));
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setRunVineFixesLoading(false));
  };

  const runSetTrailerType = () => {
    setSetTrailerError(null);
    setSetTrailerResult(null);
    setSetTrailerLoading(true);
    fetch('/api/admin/data-checks/set-trailer-type', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; updatedTT: number; updatedT: number; totalUpdated: number }) => {
        setSetTrailerResult({
          updatedTT: data.updatedTT,
          updatedT: data.updatedT,
          totalUpdated: data.totalUpdated,
        });
      })
      .catch((e) => setSetTrailerError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSetTrailerLoading(false));
  };

  const runUpdateVineyardGroup = () => {
    setVineyardGroupError(null);
    setVineyardGroupResult(null);
    setVineyardGroupLoading(true);
    fetch('/api/admin/data-checks/update-vineyard-group', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; setToNa: number; matched: number; totalRows: number }) => {
        setVineyardGroupResult({
          setToNa: data.setToNa,
          matched: data.matched,
          totalRows: data.totalRows ?? 0,
        });
      })
      .catch((e) => setVineyardGroupError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineyardGroupLoading(false));
  };

  const fetchVgRows = useCallback(() => {
    setVgLoading(true);
    setVgError(null);
    fetch('/api/admin/vineyardgroups')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: VineyardGroupRow[] }) => setVgRows(data.rows ?? []))
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgLoading(false));
  }, []);

  const fetchVgOptions = useCallback(() => {
    fetch('/api/admin/vineyardgroups/options')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { deliveryWineries?: string[]; vineyardNames?: string[] }) => {
        setVgDeliveryWineries(data.deliveryWineries ?? []);
        setVgVineyardNames(data.vineyardNames ?? []);
      })
      .catch(() => {
        setVgDeliveryWineries([]);
        setVgVineyardNames([]);
      });
  }, []);

  useEffect(() => {
    if (activeTab === 'vineyard-mappings') {
      fetchVgRows();
      fetchVgOptions();
    }
  }, [activeTab, fetchVgRows, fetchVgOptions]);

  const createVgRow = () => {
    const vineyard = vgNewVineyard.trim();
    const group = vgNewGroup.trim();
    if (!vineyard || !group) {
      setVgError('Vineyard name and group are required.');
      return;
    }
    setVgError(null);
    setVgSaving(true);
    fetch('/api/admin/vineyardgroups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        winery_name: vgNewWinery.trim() || null,
        vineyard_name: vineyard,
        vineyard_group: group,
      }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVgNewWinery('');
        setVgNewVineyard('');
        setVgNewGroup('');
        fetchVgRows();
      })
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const updateVgRow = (id: number) => {
    const vineyard = vgEditVineyard.trim();
    const group = vgEditGroup.trim();
    if (!vineyard || !group) {
      setVgError('Vineyard name and group are required.');
      return;
    }
    setVgError(null);
    setVgSaving(true);
    fetch(`/api/admin/vineyardgroups/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        winery_name: vgEditWinery.trim() || null,
        vineyard_name: vineyard,
        vineyard_group: group,
      }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVgEditingId(null);
        fetchVgRows();
      })
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const deleteVgRow = (id: number) => {
    if (!confirm('Delete this vineyard mapping?')) return;
    setVgError(null);
    setVgSaving(true);
    fetch(`/api/admin/vineyardgroups/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchVgRows())
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const startEditVg = (row: VineyardGroupRow) => {
    setVgEditingId(row.id);
    setVgEditWinery(row.winery_name ?? '');
    setVgEditVineyard(row.vineyard_name);
    setVgEditGroup(row.vineyard_group);
  };

  const cloneVgRow = (row: VineyardGroupRow) => {
    setVgEditingId(null);
    setVgNewWinery(row.winery_name ?? '');
    setVgNewVineyard(row.vineyard_name);
    setVgNewGroup(row.vineyard_group);
    setVgError(null);
  };

  return (
    <div className="min-h-screen min-w-0 bg-zinc-50 dark:bg-zinc-950">
      <div className="w-full min-w-0 px-4 py-8 text-left">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Data Checks</h1>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
          Scan and report on data quality (tbl_tracking).
        </p>

        <div className="mt-6 flex gap-1 border-b border-zinc-200 dark:border-zinc-700">
          <button
            type="button"
            onClick={() => setActiveTab('gps-gaps')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'gps-gaps' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            1. GPS Tracking Gaps
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('winery-fixes')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'winery-fixes' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            2. Winery/Vineyard Name Fixes
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('varied')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'varied' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            3. Varied
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('gps-integrity')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'gps-integrity' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            4. GPS Integrity Check
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('vineyard-mappings')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'vineyard-mappings' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            5. Vineyard Mappings
          </button>
        </div>

        {activeTab === 'gps-gaps' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
            GPS tracking gaps
          </h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Find breaks in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> per
            device (ordered by device_name, position_time_nz). Report by date and device with largest gap in minutes.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">
                Gap threshold (minutes)
              </span>
              <input
                type="number"
                min={0}
                step={1}
                value={minGapMinutes}
                onChange={(e) => setMinGapMinutes(e.target.value)}
                className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                placeholder="60"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date from (optional)</span>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date to (optional)</span>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={runGapsScan}
              disabled={loading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {loading ? 'Scanning…' : 'Run scan'}
            </button>
          </div>

          {error && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {error}
            </div>
          )}

          {result && (
            <div className="mt-6">
              <div className="flex flex-wrap items-end justify-between gap-4">
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Gaps &gt; {result.minGapMinutes} min
                  {result.dateFrom || result.dateTo
                    ? ` (${result.dateFrom ?? '…'} to ${result.dateTo ?? '…'})`
                    : ''}
                  . Summary: {result.summary.length} date/device row(s).
                </p>
                <div className="flex flex-wrap items-center gap-4">
                  <label className="flex flex-col gap-1">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Filter by device</span>
                    <input
                      type="text"
                      value={deviceFilter}
                      onChange={(e) => setDeviceFilter(e.target.value)}
                      placeholder="Device name..."
                      className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    />
                  </label>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 1</span>
                    <select
                      value={sortBy1}
                      onChange={(e) => setSortBy1(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 2</span>
                    <select
                      value={sortBy2}
                      onChange={(e) => setSortBy2(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <select
                    value={sortDir}
                    onChange={(e) => setSortDir(e.target.value as 'asc' | 'desc')}
                    className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="asc">Ascending</option>
                    <option value="desc">Descending</option>
                  </select>
                </div>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400" title="List every gap above the threshold (up to 500 rows)">
                  <input
                    type="checkbox"
                    checked={showDetail}
                    onChange={(e) => setShowDetail(e.target.checked)}
                    className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
                  />
                  Show all gaps &gt; {result.minGapMinutes} min (up to 500)
                </label>
              </div>

              {(() => {
                const filtered = deviceFilter.trim()
                  ? result.summary.filter((r) =>
                      r.device_name.toLowerCase().includes(deviceFilter.trim().toLowerCase())
                    )
                  : result.summary;
                const sorted = sortSummary(filtered, sortBy1, sortBy2, sortDir);
                const displaySummary = sorted;
                return (
                  <>
              <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
                Click a row to load tbl_tracking data in a popup (5 min before From to 90 min after To). Red rows in popup = outage boundaries.
              </p>
              <div className="mt-4 overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[900px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300" title="Date from position_time (query and grouping use position_time)">Date</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        Max gap (min)
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        # gaps &gt; threshold
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right" title="Total tbl_tracking rows for this device on this date (reconcile with GPS import)">
                        DayCount
                      </th>
                      <th className="min-w-[5rem] px-3 py-2 text-center font-medium text-zinc-700 dark:text-zinc-300" title="!! = device moved between outage start and end (lat/lon at From ≠ at To); distance shown when moved">
                        !!
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From_NZ</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To_NZ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {displaySummary.length === 0 ? (
                      <tr>
                        <td colSpan={10} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          {result.summary.length === 0
                            ? 'No gaps above threshold in the selected range.'
                            : 'No rows match the device filter.'}
                        </td>
                      </tr>
                    ) : (
                      displaySummary.map((row, i) => (
                        <tr
                          key={`${row.report_date}-${row.device_name}-${i}`}
                          onClick={() => openTrackingPopup(row)}
                          className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-100 dark:hover:bg-zinc-800/70 ${row.gap_from != null && row.gap_to != null ? '' : 'opacity-70'}`}
                          title={row.gap_from != null && row.gap_to != null ? 'Click to view tbl_tracking data around this gap' : 'No position_time for this gap'}
                        >
                          <td className="px-3 py-2 font-mono text-zinc-900 dark:text-zinc-100">
                            {formatDateDDMMYY(row.report_date)}
                          </td>
                          <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.max_gap_minutes}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.gap_count}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200" title="Total rows for this device on this date (reconcile with GPS import)">
                            {row.day_count}
                          </td>
                          <td className="px-3 py-2 text-center font-bold text-amber-600 dark:text-amber-400" title={deviceMovedDuringOutage(row) ? 'Device moved between outage start and end' : 'Same position at start and end of gap'}>
                            {deviceMovedDuringOutage(row) && row.gap_start_lat != null && row.gap_start_lon != null && row.gap_end_lat != null && row.gap_end_lon != null
                              ? `!! ${formatDistance(distanceMeters(row.gap_start_lat, row.gap_start_lon, row.gap_end_lat, row.gap_end_lon))}`
                              : deviceMovedDuringOutage(row)
                                ? '!!'
                                : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time last read before gap">
                            {row.gap_from != null ? formatGapDateTime(row.gap_from) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time first read after gap">
                            {row.gap_to != null ? formatGapDateTime(row.gap_to) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz last read before gap">
                            {formatGapDateTime(row.gap_from_nz)}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz first read after gap">
                            {formatGapDateTime(row.gap_to_nz)}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
                  </>
                );
              })()}

              {popupRow && (
                <div
                  className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
                  role="dialog"
                  aria-modal="true"
                  aria-labelledby="popup-title"
                  onClick={(e) => e.target === e.currentTarget && setPopupRow(null)}
                >
                  <div
                    className="max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                      <h3 id="popup-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                        tbl_tracking: {popupRow.device_name} (5 min before From → 90 min after To)
                      </h3>
                      <button
                        type="button"
                        onClick={() => setPopupRow(null)}
                        className="rounded p-1 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700 dark:hover:bg-zinc-800 dark:hover:text-zinc-300"
                        aria-label="Close"
                      >
                        ✕
                      </button>
                    </div>
                    <div className="max-h-[70vh] overflow-auto p-4">
                      {popupLoading && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
                      )}
                      {popupError && (
                        <p className="text-sm text-red-600 dark:text-red-400">{popupError}</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length === 0 && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">No rows in this window.</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length > 0 && (
                        <table className="w-full min-w-[640px] border-collapse text-left text-sm">
                          <thead>
                            <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time_nz</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">fence_name</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">geofence_type</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lat</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lon</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-center">Map</th>
                              <th className="min-w-[4rem] px-2 py-1.5 text-center font-medium text-zinc-700 dark:text-zinc-300" title="Distance from previous row (→ moved, ● same position)">Move</th>
                            </tr>
                          </thead>
                          <tbody>
                            {popupRows.map((r, i) => {
                              const pt = (r.position_time ?? '').trim();
                              const isFrom = pt && popupRow.gap_from != null && pt === popupRow.gap_from.trim();
                              const isTo = pt && popupRow.gap_to != null && pt === popupRow.gap_to.trim();
                              const isOutageBoundary = isFrom || isTo;
                              const prev = i > 0 ? popupRows[i - 1] : null;
                              const sameAsAbove = prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null && prev.lat === r.lat && prev.lon === r.lon;
                              const moveSymbol = prev == null ? '—' : sameAsAbove ? '●' : '→';
                              const distM =
                                prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null
                                  ? distanceMeters(prev.lat, prev.lon, r.lat, r.lon)
                                  : null;
                              const distStr = distM != null ? formatDistance(distM) : '';
                              const moveTitle =
                                prev == null
                                  ? 'First row'
                                  : sameAsAbove
                                    ? `Stationary (0 m from row above)`
                                    : `Moved ${distStr} from row above`;
                              return (
                              <tr
                                key={i}
                                className={`border-b border-zinc-100 dark:border-zinc-800 ${isOutageBoundary ? 'bg-red-200 dark:bg-red-950/80' : ''}`}
                                title={isFrom ? 'Last read before gap (outage start)' : isTo ? 'First read after gap (outage end)' : undefined}
                              >
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time ?? '—'}
                                </td>
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time_nz ?? '—'}
                                </td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.fence_name ?? '—'}</td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.geofence_type ?? '—'}</td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lat != null ? r.lat : '—'}
                                </td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lon != null ? r.lon : '—'}
                                </td>
                                <td className="px-2 py-1 text-center">
                                  {r.lat != null && r.lon != null ? (
                                    <a
                                      href={`https://www.google.com/maps?q=${r.lat},${r.lon}`}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-blue-600 dark:text-blue-400 hover:underline"
                                      title={`Open ${r.lat}, ${r.lon} in Google Maps`}
                                    >
                                      Map
                                    </a>
                                  ) : (
                                    '—'
                                  )}
                                </td>
                                <td className="px-2 py-1 text-center text-sm font-mono text-xs" title={moveTitle}>
                                  {prev == null ? (
                                    '—'
                                  ) : (
                                    <>
                                      <span className={sameAsAbove ? 'text-zinc-400 dark:text-zinc-500' : 'text-emerald-600 dark:text-emerald-400'}>{moveSymbol}</span>
                                      {' '}
                                      <span className="text-zinc-600 dark:text-zinc-400">{distStr}</span>
                                    </>
                                  )}
                                </td>
                              </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {showDetail && result.detail.length > 0 && (
                <div className="mt-6">
                  <h3 className="text-sm font-medium text-zinc-700 dark:text-zinc-300">All gaps &gt; {result.minGapMinutes} min ({result.detail.length} shown)</h3>
                  <div className="mt-2 max-h-[400px] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                    <table className="w-full min-w-[520px] border-collapse text-left text-sm">
                      <thead className="sticky top-0 bg-zinc-100/95 dark:bg-zinc-800/95">
                        <tr className="border-b border-zinc-200 dark:border-zinc-700">
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap start</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap end</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                            Gap (min)
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.detail.map((row, i) => (
                          <tr
                            key={`${row.device_name}-${row.gap_start}-${i}`}
                            className="border-b border-zinc-100 dark:border-zinc-800"
                          >
                            <td className="px-3 py-1.5 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_start)}
                            </td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_end)}
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-zinc-800 dark:text-zinc-200">
                              {row.gap_minutes}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </section>
        )}

        {activeTab === 'winery-fixes' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Winery/Vineyard Name Fixes</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map old vwork names to new names. Winery fixes use <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_wine_mapp</code> and <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery</code>; vineyard fixes use <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vine_mapp</code> and <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name</code>. Run the SQL migrations once if columns or tables are missing.
          </p>

          <h3 className="mt-6 text-base font-medium text-zinc-800 dark:text-zinc-200">Winery name fixes</h3>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">Run Fixes</code> sets <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery_old</code>, then <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery</code>. Run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_delivery_winery_old_tbl_vworkjobs.sql</code> if needed.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Old name</span>
              <input
                type="text"
                list="wine-mapp-old-list"
                value={wineMappNewOld}
                onChange={(e) => setWineMappNewOld(e.target.value)}
                placeholder="Select or type old winery name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="wine-mapp-old-list">
                {deliveryWineries.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">New name</span>
              <input
                type="text"
                list="wine-mapp-new-list"
                value={wineMappNewNew}
                onChange={(e) => setWineMappNewNew(e.target.value)}
                placeholder="Select or type new winery name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="wine-mapp-new-list">
                {deliveryWineries.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <button
              type="button"
              onClick={createWineMapp}
              disabled={wineMappSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {wineMappSaving ? 'Saving…' : 'Add fix'}
            </button>
            <button
              type="button"
              onClick={runFixes}
              disabled={runFixesLoading || wineMappRows.length === 0}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              title="Update tbl_vworkjobs: set delivery_winery_old = current delivery_winery, delivery_winery = new name where delivery_winery = old name"
            >
              {runFixesLoading ? 'Running…' : 'Run Fixes'}
            </button>
          </div>

          {runFixesResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {runFixesResult.totalUpdated} row(s) in tbl_vworkjobs.
              {runFixesResult.perMapping.length > 0 && (
                <ul className="mt-1 list-inside list-disc">
                  {runFixesResult.perMapping.map((m) => (
                    <li key={m.id}>
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.oldvworkname}</code> → <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.newvworkname}</code>: {m.updated} row(s)
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {wineMappError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {wineMappError}
            </div>
          )}

          <div className="mt-6">
            {wineMappLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Old name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">New name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wineMappRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No winery name fixes yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      wineMappRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {editingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`wine-mapp-edit-old-${row.id}`}
                                  value={editOld}
                                  onChange={(e) => setEditOld(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`wine-mapp-edit-old-${row.id}`}>
                                  {deliveryWineries.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`wine-mapp-edit-new-${row.id}`}
                                  value={editNew}
                                  onChange={(e) => setEditNew(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`wine-mapp-edit-new-${row.id}`}>
                                  {deliveryWineries.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateWineMapp(row.id)}
                                  disabled={wineMappSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.oldvworkname}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.newvworkname}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEdit(row)}
                                  disabled={wineMappSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteWineMapp(row.id)}
                                  disabled={wineMappSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <h3 className="mt-10 border-t border-zinc-200 pt-8 text-base font-medium text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
            Vineyard name fixes
          </h3>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map old vineyard names to new names in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vine_mapp</code>. <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">Run Fixes</code> sets <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name_old</code>, then <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name</code>. Create the table with <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">create_tbl_vine_mapp.sql</code>; run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_vineyard_name_old_tbl_vworkjobs.sql</code> if <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name_old</code> is missing.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Old name</span>
              <input
                type="text"
                list="vine-mapp-old-list"
                value={vineMappNewOld}
                onChange={(e) => setVineMappNewOld(e.target.value)}
                placeholder="Select or type old vineyard name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vine-mapp-old-list">
                {vineyardNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">New name</span>
              <input
                type="text"
                list="vine-mapp-new-list"
                value={vineMappNewNew}
                onChange={(e) => setVineMappNewNew(e.target.value)}
                placeholder="Select or type new vineyard name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vine-mapp-new-list">
                {vineyardNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <button
              type="button"
              onClick={createVineMapp}
              disabled={vineMappSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {vineMappSaving ? 'Saving…' : 'Add fix'}
            </button>
            <button
              type="button"
              onClick={runVineFixes}
              disabled={runVineFixesLoading || vineMappRows.length === 0}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              title="Update tbl_vworkjobs: set vineyard_name_old = current vineyard_name, vineyard_name = new name where vineyard_name = old name"
            >
              {runVineFixesLoading ? 'Running…' : 'Run Fixes'}
            </button>
          </div>

          {runVineFixesResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {runVineFixesResult.totalUpdated} row(s) in tbl_vworkjobs (vineyard_name).
              {runVineFixesResult.perMapping.length > 0 && (
                <ul className="mt-1 list-inside list-disc">
                  {runVineFixesResult.perMapping.map((m) => (
                    <li key={m.id}>
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.oldvworkname}</code> →{' '}
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.newvworkname}</code>: {m.updated}{' '}
                      row(s)
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {vineMappError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {vineMappError}
            </div>
          )}

          <div className="mt-6">
            {vineMappLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading vineyard fixes…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Old name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">New name</th>
                      <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vineMappRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No vineyard name fixes yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      vineMappRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {vineEditingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vine-mapp-edit-old-${row.id}`}
                                  value={vineEditOld}
                                  onChange={(e) => setVineEditOld(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vine-mapp-edit-old-${row.id}`}>
                                  {vineyardNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vine-mapp-edit-new-${row.id}`}
                                  value={vineEditNew}
                                  onChange={(e) => setVineEditNew(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vine-mapp-edit-new-${row.id}`}>
                                  {vineyardNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateVineMapp(row.id)}
                                  disabled={vineMappSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setVineEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.oldvworkname}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.newvworkname}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEditVine(row)}
                                  disabled={vineMappSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteVineMapp(row.id)}
                                  disabled={vineMappSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </section>
        )}

        {activeTab === 'varied' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Varied</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Set <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.trailermode</code> from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">trailer_rego</code>: <strong>TT</strong> when a valid trailer rego is present (truck + trailer), <strong>T</strong> when no trailer (truck only).
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <button
              type="button"
              onClick={runSetTrailerType}
              disabled={setTrailerLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
              title="UPDATE tbl_vworkjobs: trailermode = 'TT' where trailer_rego valid, trailermode = 'T' (truck only) otherwise"
            >
              {setTrailerLoading ? 'Running…' : 'Set Trailer Type'}
            </button>
          </div>

          {setTrailerResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {setTrailerResult.totalUpdated} row(s): {setTrailerResult.updatedTT} set to TT (trailer), {setTrailerResult.updatedT} set to T (truck only).
            </div>
          )}

          {setTrailerError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {setTrailerError}
            </div>
          )}
        </section>
        )}

        {activeTab === 'gps-integrity' && (
        <section className="mt-0 min-w-0 max-w-full rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">GPS Integrity Check</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Rows = dates, columns = all devices (HK). Each cell: 3 checks vs <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking.position_time</code> — 1st time, last time, row count (API vs DB). Green = pass, red = fail, — = no API data.
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From</span>
              <input
                type="date"
                value={gpsIntegrityFrom}
                onChange={(e) => setGpsIntegrityFrom(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To</span>
              <input
                type="date"
                value={gpsIntegrityTo}
                onChange={(e) => setGpsIntegrityTo(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                setGpsIntegrityError(null);
                setGpsIntegrityGrid(null);
                if (!gpsIntegrityFrom || !gpsIntegrityTo) {
                  setGpsIntegrityError('From and To dates required');
                  return;
                }
                setGpsIntegrityLoading(true);
                fetch('/api/admin/data-checks/gps-integrity', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    dateFrom: gpsIntegrityFrom,
                    dateTo: gpsIntegrityTo,
                    grid: true,
                  }),
                })
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: GpsIntegrityGridResponse) => setGpsIntegrityGrid(data))
                  .catch((e) => setGpsIntegrityError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setGpsIntegrityLoading(false));
              }}
              disabled={gpsIntegrityLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {gpsIntegrityLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {gpsIntegrityError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {gpsIntegrityError}
            </div>
          )}
          {gpsIntegrityGrid && gpsIntegrityGrid.rows.length > 0 && (
            <div className="mt-4">
              <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
                Each cell shows 1st time, last time, and row count (or API/DB when mismatch). Green = pass, red = fail. — = no API data.
              </p>
              <div className="max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-max min-w-full border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="sticky left-0 z-10 border-r border-zinc-200 bg-zinc-100/80 px-2 py-2 font-medium text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800/80 dark:text-zinc-300">Date</th>
                      {gpsIntegrityGrid.devices.map((d) => (
                        <th key={d} className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">{d}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {gpsIntegrityGrid.rows.map((row) => (
                      <tr key={row.date} className="border-b border-zinc-100 dark:border-zinc-700">
                        <td className="sticky left-0 z-10 border-r border-zinc-200 bg-white px-2 py-1.5 font-mono text-xs text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300">
                          {row.date}
                        </td>
                        {row.cells.map((cell, j) => {
                          if (cell.status === 'no_data') {
                            return (
                              <td key={j} className="px-2 py-1.5 text-center text-zinc-400 dark:text-zinc-500">
                                —
                              </td>
                            );
                          }
                          const title = `1st: ${cell.firstTime} | Last: ${cell.lastTime} | API: ${cell.apiCount} DB: ${cell.dbCount}`;
                          const firstText = cell.firstOk ? cell.firstTime : `${cell.firstTime} (${cell.dbFirstTime ?? ''})`;
                          const lastText = cell.lastOk ? cell.lastTime : `${cell.lastTime} (${cell.dbLastTime ?? ''})`;
                          const countText = cell.countOk ? String(cell.apiCount) : `${cell.apiCount} / (${cell.dbCount})`;
                          return (
                            <td key={j} className="px-2 py-1.5 font-mono text-xs" title={title}>
                              <div className="flex flex-col gap-0.5">
                                <span className={cell.firstOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="1st in DB">{firstText}</span>
                                <span className={cell.lastOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="Last in DB">{lastText}</span>
                                <span className={cell.countOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="API vs DB row count">{countText}</span>
                              </div>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'vineyard-mappings' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Vineyard Mappings</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map vineyard names to groups in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vineyardgroups</code>. Winery from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.delivery_winery</code>; vineyard name and group are free text with suggestions from vworkjobs.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Winery name</span>
              <select
                value={vgNewWinery}
                onChange={(e) => setVgNewWinery(e.target.value)}
                className="w-48 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              >
                <option value="">— Select —</option>
                {vgDeliveryWineries.map((w) => (
                  <option key={w} value={w}>{w}</option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Vineyard name</span>
              <input
                type="text"
                list="vg-new-vineyard-list"
                value={vgNewVineyard}
                onChange={(e) => setVgNewVineyard(e.target.value)}
                placeholder="Type or pick from list"
                className="w-48 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vg-new-vineyard-list">
                {vgVineyardNames.map((v) => (
                  <option key={v} value={v} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Group</span>
              <input
                type="text"
                value={vgNewGroup}
                onChange={(e) => setVgNewGroup(e.target.value)}
                placeholder="Free text"
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={createVgRow}
              disabled={vgSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {vgSaving ? 'Saving…' : 'Add'}
            </button>
          </div>

          {vgError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {vgError}
            </div>
          )}

          <div className="mt-6">
            {vgLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[520px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Winery</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Vineyard</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Group</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vgRows.length === 0 ? (
                      <tr>
                        <td colSpan={4} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No mappings yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      vgRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {vgEditingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <select
                                  value={vgEditWinery}
                                  onChange={(e) => setVgEditWinery(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                >
                                  <option value="">—</option>
                                  {vgDeliveryWineries.map((w) => (
                                    <option key={w} value={w}>{w}</option>
                                  ))}
                                </select>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vg-edit-vineyard-${row.id}`}
                                  value={vgEditVineyard}
                                  onChange={(e) => setVgEditVineyard(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vg-edit-vineyard-${row.id}`}>
                                  {vgVineyardNames.map((v) => (
                                    <option key={v} value={v} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  value={vgEditGroup}
                                  onChange={(e) => setVgEditGroup(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateVgRow(row.id)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setVgEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.winery_name ?? '—'}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.vineyard_name}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.vineyard_group}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEditVg(row)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => cloneVgRow(row)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                  title="Copy this row into the Add form"
                                >
                                  Clone
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteVgRow(row.id)}
                                  disabled={vgSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="mt-8 border-t border-zinc-200 pt-6 dark:border-zinc-700">
            <h3 className="text-base font-medium text-zinc-900 dark:text-zinc-100">Update Vineyard_Group</h3>
            <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
              Set all <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.vineyard_group</code> to <strong>NA</strong>, then set rows that match <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vineyardgroups</code> (winery + vineyard) to the mapped <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_group</code>. Run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_vineyard_group_tbl_vworkjobs.sql</code> once if the column is missing.
            </p>
            <div className="mt-4">
              <button
                type="button"
                onClick={runUpdateVineyardGroup}
                disabled={vineyardGroupLoading}
                className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                title="SET vineyard_group = 'NA' for all, then set from tbl_vineyardgroups where delivery_winery/vineyard_name match"
              >
                {vineyardGroupLoading ? 'Running…' : 'Update Vineyard_Group'}
              </button>
            </div>
            {vineyardGroupResult != null && (
              <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
                Set {vineyardGroupResult.setToNa} row(s) to NA; {vineyardGroupResult.matched} row(s) updated from tbl_vineyardgroups (total vwork rows: {vineyardGroupResult.totalRows}).
              </div>
            )}
            {vineyardGroupError && (
              <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
                {vineyardGroupError}
              </div>
            )}
          </div>
        </section>
        )}
      </div>
    </div>
  );
}

export default function DataChecksPage() {
  return (
    <Suspense fallback={<div className="min-h-screen bg-zinc-50 dark:bg-zinc-950 flex items-center justify-center"><p className="text-zinc-500 dark:text-zinc-400">Loading…</p></div>}>
      <DataChecksPageContent />
    </Suspense>
  );
}
