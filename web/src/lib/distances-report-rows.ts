/**
 * Build grouped distance report rows with subtotals.
 * Matches Summary rollup rule: excluded = 1 omitted.
 * Only jobs with reportable km (> 0) appear; groups/subtotals with no km are omitted.
 */

export type DistancesJobRow = Record<string, unknown>;

export type DistancesReportFormat = 'customer_template_winery_truck' | 'customer_template';

export type DistancesReportRow =
  | {
      kind: 'detail';
      customer: string;
      template: string;
      winery: string;
      truck: string;
      distanceKm: number;
    }
  /** Compact format: one row per (customer, template) with jobs and km rolled up (no winery/truck breakdown). */
  | {
      kind: 'template_total';
      customer: string;
      template: string;
      jobCount: number;
      sumKm: number;
    }
  | {
      kind: 'subtotal';
      scope: 'truck' | 'winery' | 'template' | 'customer' | 'grand';
      customer: string;
      template: string;
      winery: string;
      truck?: string;
      label: string;
      jobCount: number;
      sumKm: number;
    };

export function distanceRoundTripKmFromRow(row: DistancesJobRow): number | null {
  const raw = row.distance;
  const n = typeof raw === 'number' ? raw : typeof raw === 'string' ? parseFloat(raw) : NaN;
  return Number.isFinite(n) ? n : null;
}

export function jobIncludedInDistancesRollup(row: DistancesJobRow): boolean {
  const ex = row.excluded;
  if (ex == null || ex === '') return true;
  return Number(ex) !== 1;
}

/** Job contributes to this report only if rollup-eligible and distance is a positive km value. */
export function jobHasReportableDistanceKm(row: DistancesJobRow): boolean {
  if (!jobIncludedInDistancesRollup(row)) return false;
  const k = distanceRoundTripKmFromRow(row);
  return k != null && k > 0;
}

function labelOf(row: DistancesJobRow) {
  return {
    customer: String(row.Customer ?? row.customer ?? '').trim() || '—',
    template: String(row.template ?? '').trim() || '—',
    winery: String(row.delivery_winery ?? '').trim() || '—',
    truck: String(row.truck_id ?? '').trim() || '—',
  };
}

function sortedKeys(m: Map<string, unknown>) {
  return [...m.keys()].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

/** Customer → Template → Winery → Truck (full subtotal ladder). */
function buildCustomerTemplateWineryTruck(withKm: DistancesJobRow[]): DistancesReportRow[] {
  const tree = new Map<string, Map<string, Map<string, Map<string, DistancesJobRow[]>>>>();
  for (const row of withKm) {
    const { customer, template, winery, truck } = labelOf(row);
    if (!tree.has(customer)) tree.set(customer, new Map());
    const tMap = tree.get(customer)!;
    if (!tMap.has(template)) tMap.set(template, new Map());
    const wMap = tMap.get(template)!;
    if (!wMap.has(winery)) wMap.set(winery, new Map());
    const trMap = wMap.get(winery)!;
    if (!trMap.has(truck)) trMap.set(truck, []);
    trMap.get(truck)!.push(row);
  }
  const out: DistancesReportRow[] = [];
  let grandJobs = 0;
  let grandKm = 0;
  for (const c of sortedKeys(tree)) {
    const tMap = tree.get(c)!;
    let custJobs = 0;
    let custKm = 0;
    for (const t of sortedKeys(tMap)) {
      const wMap = tMap.get(t)!;
      let tmplJobs = 0;
      let tmplKm = 0;
      for (const w of sortedKeys(wMap)) {
        const trMap = wMap.get(w)!;
        let winJobs = 0;
        let winKm = 0;
        for (const tr of sortedKeys(trMap)) {
          const bucket = trMap.get(tr)!;
          bucket.sort((a, b) =>
            String(a.job_id ?? '')
              .trim()
              .localeCompare(String(b.job_id ?? '').trim(), undefined, { sensitivity: 'base' }),
          );
          let trJobs = 0;
          let trKm = 0;
          for (const row of bucket) {
            const km = distanceRoundTripKmFromRow(row)!;
            out.push({
              kind: 'detail',
              customer: c,
              template: t,
              winery: w,
              truck: tr,
              distanceKm: km,
            });
            trJobs += 1;
            trKm += km;
          }
          if (trJobs > 0 && trKm > 0) {
            out.push({
              kind: 'subtotal',
              scope: 'truck',
              customer: c,
              template: t,
              winery: w,
              truck: tr,
              label: `Subtotal · truck ${tr}`,
              jobCount: trJobs,
              sumKm: trKm,
            });
            winJobs += trJobs;
            winKm += trKm;
          }
        }
        if (winJobs > 0 && winKm > 0) {
          out.push({
            kind: 'subtotal',
            scope: 'winery',
            customer: c,
            template: t,
            winery: w,
            label: `Subtotal · winery ${w}`,
            jobCount: winJobs,
            sumKm: winKm,
          });
          tmplJobs += winJobs;
          tmplKm += winKm;
        }
      }
      if (tmplJobs > 0 && tmplKm > 0) {
        out.push({
          kind: 'subtotal',
          scope: 'template',
          customer: c,
          template: t,
          winery: '',
          label: `Subtotal · template ${t}`,
          jobCount: tmplJobs,
          sumKm: tmplKm,
        });
        custJobs += tmplJobs;
        custKm += tmplKm;
      }
    }
    if (custJobs > 0 && custKm > 0) {
      out.push({
        kind: 'subtotal',
        scope: 'customer',
        customer: c,
        template: '',
        winery: '',
        label: `Subtotal · customer ${c}`,
        jobCount: custJobs,
        sumKm: custKm,
      });
      grandJobs += custJobs;
      grandKm += custKm;
    }
  }
  if (grandJobs > 0 && grandKm > 0) {
    out.push({
      kind: 'subtotal',
      scope: 'grand',
      customer: '',
      template: '',
      winery: '',
      label: 'Grand total',
      jobCount: grandJobs,
      sumKm: grandKm,
    });
  }
  return out;
}

/** Customer → Template only: km rolled up to template (no per-job rows; no winery/truck columns in UI). */
function buildCustomerTemplateOnly(withKm: DistancesJobRow[]): DistancesReportRow[] {
  const tree = new Map<string, Map<string, DistancesJobRow[]>>();
  for (const row of withKm) {
    const { customer, template } = labelOf(row);
    if (!tree.has(customer)) tree.set(customer, new Map());
    const tMap = tree.get(customer)!;
    if (!tMap.has(template)) tMap.set(template, []);
    tMap.get(template)!.push(row);
  }
  const out: DistancesReportRow[] = [];
  let grandJobs = 0;
  let grandKm = 0;
  for (const c of sortedKeys(tree)) {
    const tMap = tree.get(c)!;
    let custJobs = 0;
    let custKm = 0;
    for (const t of sortedKeys(tMap)) {
      const bucket = tMap.get(t)!;
      let tmplJobs = 0;
      let tmplKm = 0;
      for (const row of bucket) {
        tmplJobs += 1;
        tmplKm += distanceRoundTripKmFromRow(row)!;
      }
      if (tmplJobs > 0 && tmplKm > 0) {
        out.push({
          kind: 'template_total',
          customer: c,
          template: t,
          jobCount: tmplJobs,
          sumKm: tmplKm,
        });
        custJobs += tmplJobs;
        custKm += tmplKm;
      }
    }
    if (custJobs > 0 && custKm > 0) {
      out.push({
        kind: 'subtotal',
        scope: 'customer',
        customer: c,
        template: '',
        winery: '',
        label: `Subtotal · customer ${c}`,
        jobCount: custJobs,
        sumKm: custKm,
      });
      grandJobs += custJobs;
      grandKm += custKm;
    }
  }
  if (grandJobs > 0 && grandKm > 0) {
    out.push({
      kind: 'subtotal',
      scope: 'grand',
      customer: '',
      template: '',
      winery: '',
      label: 'Grand total',
      jobCount: grandJobs,
      sumKm: grandKm,
    });
  }
  return out;
}

export function buildDistancesReportRows(
  jobs: DistancesJobRow[],
  format: DistancesReportFormat = 'customer_template_winery_truck',
): DistancesReportRow[] {
  const withKm = jobs.filter(jobHasReportableDistanceKm);
  if (format === 'customer_template') return buildCustomerTemplateOnly(withKm);
  return buildCustomerTemplateWineryTruck(withKm);
}
