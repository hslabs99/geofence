/**
 * Shared logic: tbl_wineryminutes rows matched to tbl_vworkjobs for Summary and admin checks.
 * Vineyard group must match exactly after trim (blank limit row ↔ jobs with no vineyard_group only).
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
 * Same as Summary `getMatchingLimitsRow`: winery + exact VG + TT, or TTT fallback for job TT T/TT.
 */
export function findMatchingWineryMinuteRow(
  rows: WineryMinuteLimitRow[],
  key: MatchingLimitKey,
): WineryMinuteLimitRow | null {
  const winery = norm(key.winery);
  if (!winery) return null;
  const vg = norm(key.vineyardGroup);
  const jobTT = norm(key.jobTT);

  const matchBase = (r: WineryMinuteLimitRow) => {
    if (key.customer !== undefined && norm(r.Customer) !== norm(key.customer)) return false;
    if (key.template !== undefined && norm(r.Template) !== norm(key.template)) return false;
    if (norm(r.Winery) !== winery) return false;
    if (norm(r.vineyardgroup) !== vg) return false;
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
