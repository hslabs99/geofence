/**
 * Saved “Summary views” for the sidebar (localStorage). Not tied to DB.
 */

export const SUMMARY_HISTORY_STORAGE_KEY = 'geodata_summary_history_v1';
const MAX_ENTRIES = 25;

export type SummaryTabMode = 'season' | 'by_day' | 'by_job' | 'data_audit';

/** Full UI state to restore Summary + sidebar customer (admin/super). */
export type SummaryHistoryPayload = {
  filterActualFrom: string;
  filterActualTo: string;
  filterTemplate: string;
  filterTruckId: string;
  filterWorker: string;
  filterTrailermode: string;
  summaryTab: SummaryTabMode;
  filterWinery: string;
  filterVineyardGroup: string;
  /** Selected vineyard_name values (empty = all). */
  filterVineyards: string[];
  splitByLimits: boolean;
  minsThresholds: Record<string, string>;
  selectedTimeLimitRowId: number | null;
  showLimitsTable: boolean;
  sortKey: string | null;
  sortDir: 'asc' | 'desc';
  sortColumns: [string, string, string];
  jobsPage: number;
  clientCustomer: string;
  /** Filled when leaving Summary via a job ID → Inspect link; used to scroll/highlight that row on Recent restore. */
  focusJobId?: string;
  focusTruckId?: string;
  focusInspectActualFrom?: string;
  focusInspectActualTo?: string;
};

export type SummaryHistoryEntry = {
  id: string;
  createdAt: number;
  label: string;
  payload: SummaryHistoryPayload;
};

function stableStringify(p: SummaryHistoryPayload): string {
  return JSON.stringify(p);
}

export function buildSummaryHistoryLabel(p: SummaryHistoryPayload): string {
  const tab =
    p.summaryTab === 'by_job'
      ? 'Jobs'
      : p.summaryTab === 'by_day'
        ? 'By day'
        : p.summaryTab === 'data_audit'
          ? 'Data audit'
          : 'Season';
  const cust = p.clientCustomer.trim() || '—';
  const tpl = p.filterTemplate.trim() || '—';
  const job = p.focusJobId?.trim();
  const jobSuffix = job ? ` · #${job}` : '';
  return `Summary · ${cust} · ${tpl} · ${tab}${jobSuffix}`;
}

export function isSignificantSummaryState(p: SummaryHistoryPayload): boolean {
  return (
    p.clientCustomer.trim() !== '' ||
    p.filterTemplate.trim() !== '' ||
    p.summaryTab !== 'season' ||
    p.filterActualFrom.trim() !== '' ||
    p.filterActualTo.trim() !== '' ||
    p.filterWinery.trim() !== '' ||
    p.filterVineyardGroup.trim() !== '' ||
    (p.filterVineyards?.length ?? 0) > 0 ||
    p.filterTruckId.trim() !== '' ||
    p.filterWorker.trim() !== '' ||
    p.filterTrailermode.trim() !== ''
  );
}

function readRaw(): SummaryHistoryEntry[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(SUMMARY_HISTORY_STORAGE_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw) as unknown;
    if (!Array.isArray(arr)) return [];
    return arr.filter(
      (x): x is SummaryHistoryEntry =>
        x != null &&
        typeof x === 'object' &&
        typeof (x as SummaryHistoryEntry).id === 'string' &&
        (x as SummaryHistoryEntry).payload != null,
    );
  } catch {
    return [];
  }
}

function writeRaw(entries: SummaryHistoryEntry[]): void {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(SUMMARY_HISTORY_STORAGE_KEY, JSON.stringify(entries.slice(0, MAX_ENTRIES)));
  } catch {
    /* quota */
  }
}

export function listSummaryHistory(): SummaryHistoryEntry[] {
  return readRaw();
}

export function getSummaryHistoryEntry(id: string): SummaryHistoryEntry | null {
  const q = id.trim();
  if (!q) return null;
  return readRaw().find((e) => e.id === q) ?? null;
}

/** Returns true if a new row was added. */
export function appendSummaryHistory(payload: SummaryHistoryPayload): boolean {
  if (!isSignificantSummaryState(payload)) return false;
  const prev = readRaw();
  const label = buildSummaryHistoryLabel(payload);
  const sig = stableStringify(payload);
  if (prev.length > 0) {
    const top = prev[0];
    if (top && stableStringify(top.payload) === sig) return false;
  }
  const id =
    typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
      ? crypto.randomUUID()
      : `h_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
  const entry: SummaryHistoryEntry = {
    id,
    createdAt: Date.now(),
    label,
    payload: { ...payload, minsThresholds: { ...payload.minsThresholds } },
  };
  writeRaw([entry, ...prev.filter((e) => stableStringify(e.payload) !== sig)].slice(0, MAX_ENTRIES));
  return true;
}

export function clearSummaryHistory(): void {
  if (typeof window === 'undefined') return;
  try {
    localStorage.removeItem(SUMMARY_HISTORY_STORAGE_KEY);
  } catch {
    /* ignore */
  }
}
