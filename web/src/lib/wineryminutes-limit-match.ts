/**
 * Shared logic: tbl_wineryminutes rows matched to tbl_vworkjobs for Summary and admin checks.
 * Vineyard group matches after trim, case-insensitively (e.g. NELSON on jobs ↔ Nelson in limits).
 */

export type WineryMinuteLimitRow = {
  id?: number;
  Customer?: string | null;
  Template?: string | null;
  vineyardgroup?: string | null;
  Winery?: string | null;
  TT?: string | null;
};

function norm(s: unknown): string {
  return String(s ?? '').trim();
}

/** Vineyard group compare key: trim + lowercase (empty matches empty). */
function normVg(s: unknown): string {
  return norm(s).toLowerCase();
}

export type MatchingLimitKey = {
  /** When set, limit row Customer must match. Omit when limit rows are already scoped (e.g. Summary). */
  customer?: string;
  /** When set, limit row Template must match (same as trim(COALESCE(template,''))). */
  template?: string;
  winery: string;
  vineyardGroup: string;
  jobTT: string;
};

/**
 * Same as Summary `getMatchingLimitsRow`: winery + VG (case-insensitive) + TT, or TTT fallback for job TT T/TT.
 */
export function findMatchingWineryMinuteRow(
  rows: WineryMinuteLimitRow[],
  key: MatchingLimitKey,
): WineryMinuteLimitRow | null {
  const winery = norm(key.winery);
  if (!winery) return null;
  const jobTT = norm(key.jobTT);

  const matchBase = (r: WineryMinuteLimitRow) => {
    if (key.customer !== undefined && norm(r.Customer) !== norm(key.customer)) return false;
    if (key.template !== undefined && norm(r.Template) !== norm(key.template)) return false;
    if (norm(r.Winery) !== winery) return false;
    if (normVg(r.vineyardgroup) !== normVg(key.vineyardGroup)) return false;
    return true;
  };

  const exact = rows.find((r) => matchBase(r) && norm(r.TT) === jobTT);
  if (exact) return exact;
  if (jobTT === 'T' || jobTT === 'TT') {
    const ttt = rows.find((r) => matchBase(r) && norm(r.TT) === 'TTT');
    if (ttt) return ttt;
  }
  return null;
}
