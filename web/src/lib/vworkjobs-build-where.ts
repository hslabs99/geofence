/** Optional date filter on a single timestamp column (columnDateCol). */
const COLUMN_DATE_WHITELIST = new Set([
  'planned_start_time',
  'actual_start_time',
  'actual_end_time',
]);

/** Apply filters (customer, template, date range, etc.) and pagination. Uses trim(t.customer) for customer. Shared by vworkjobs API and admin reports that reuse Inspect filters. */
export function buildWhereAndParams(searchParams: URLSearchParams): {
  conditions: string[];
  values: unknown[];
  debug: Record<string, unknown>;
} {
  const conditions: string[] = [];
  const values: unknown[] = [];
  const debug: Record<string, unknown> = {};
  let idx = 1;

  const dateFromParam = searchParams.get('dateFrom')?.trim();
  const dateToParam = searchParams.get('dateTo')?.trim();
  const dateParam = searchParams.get('date')?.trim();
  if (dateFromParam && dateToParam && /^\d{4}-\d{2}-\d{2}$/.test(dateFromParam) && /^\d{4}-\d{2}-\d{2}$/.test(dateToParam)) {
    conditions.push(`(actual_start_time >= $${idx}::date AND actual_start_time < $${idx + 1}::date + interval '1 day')`);
    values.push(dateFromParam, dateToParam);
    idx += 2;
    debug.dateFrom = dateFromParam;
    debug.dateTo = dateToParam;
  } else if (dateFromParam && /^\d{4}-\d{2}-\d{2}$/.test(dateFromParam)) {
    conditions.push(`(actual_start_time >= $${idx}::date)`);
    values.push(dateFromParam);
    idx++;
    debug.dateFrom = dateFromParam;
  } else if (dateToParam && /^\d{4}-\d{2}-\d{2}$/.test(dateToParam)) {
    conditions.push(`(actual_start_time < $${idx}::date + interval '1 day')`);
    values.push(dateToParam);
    idx++;
    debug.dateTo = dateToParam;
  } else if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    conditions.push(`(actual_start_time >= $${idx}::timestamp AND actual_start_time < $${idx}::timestamp + interval '1 day')`);
    values.push(`${dateParam}T00:00:00`);
    idx++;
    debug.date = dateParam;
  }

  const plannedFrom = searchParams.get('plannedDateFrom')?.trim();
  const plannedTo = searchParams.get('plannedDateTo')?.trim();
  if (plannedFrom && plannedTo && /^\d{4}-\d{2}-\d{2}$/.test(plannedFrom) && /^\d{4}-\d{2}-\d{2}$/.test(plannedTo)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date >= $${idx}::date AND t.planned_start_time::date <= $${idx + 1}::date)`);
    values.push(plannedFrom, plannedTo);
    idx += 2;
    debug.plannedDateFrom = plannedFrom;
    debug.plannedDateTo = plannedTo;
  } else if (plannedFrom && /^\d{4}-\d{2}-\d{2}$/.test(plannedFrom)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date >= $${idx}::date)`);
    values.push(plannedFrom);
    idx++;
    debug.plannedDateFrom = plannedFrom;
  } else if (plannedTo && /^\d{4}-\d{2}-\d{2}$/.test(plannedTo)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date <= $${idx}::date)`);
    values.push(plannedTo);
    idx++;
    debug.plannedDateTo = plannedTo;
  }

  const columnDateCol = searchParams.get('columnDateCol')?.trim();
  const columnDateFrom = searchParams.get('columnDateFrom')?.trim().slice(0, 10);
  const columnDateTo = searchParams.get('columnDateTo')?.trim().slice(0, 10);
  if (columnDateCol && COLUMN_DATE_WHITELIST.has(columnDateCol)) {
    if (columnDateFrom && columnDateTo && /^\d{4}-\d{2}-\d{2}$/.test(columnDateFrom) && /^\d{4}-\d{2}-\d{2}$/.test(columnDateTo)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date >= $${idx}::date AND t.${columnDateCol}::date <= $${idx + 1}::date)`);
      values.push(columnDateFrom, columnDateTo);
      idx += 2;
      debug.columnDateCol = columnDateCol;
      debug.columnDateFrom = columnDateFrom;
      debug.columnDateTo = columnDateTo;
    } else if (columnDateFrom && /^\d{4}-\d{2}-\d{2}$/.test(columnDateFrom)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date >= $${idx}::date)`);
      values.push(columnDateFrom);
      idx++;
      debug.columnDateCol = columnDateCol;
      debug.columnDateFrom = columnDateFrom;
    } else if (columnDateTo && /^\d{4}-\d{2}-\d{2}$/.test(columnDateTo)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date <= $${idx}::date)`);
      values.push(columnDateTo);
      idx++;
      debug.columnDateCol = columnDateCol;
      debug.columnDateTo = columnDateTo;
    }
  }

  const customerParam = searchParams.get('customer')?.trim();
  if (customerParam) {
    conditions.push(`(trim(t.customer) = $${idx})`);
    values.push(customerParam);
    idx++;
    debug.customer = customerParam;
  }

  const templateParam = searchParams.get('template')?.trim();
  if (templateParam) {
    conditions.push(`(trim(t.template) = $${idx})`);
    values.push(templateParam);
    idx++;
    debug.template = templateParam;
  }

  const wineryParam = searchParams.get('winery')?.trim();
  if (wineryParam) {
    conditions.push(`(trim(t.delivery_winery) = $${idx})`);
    values.push(wineryParam);
    idx++;
    debug.winery = wineryParam;
  }

  const vineyardParams = [...new Set(searchParams.getAll('vineyard').map((s) => s.trim()).filter(Boolean))];
  if (vineyardParams.length === 1) {
    conditions.push(`(trim(t.vineyard_name) = $${idx})`);
    values.push(vineyardParams[0]);
    idx++;
    debug.vineyard = vineyardParams[0];
  } else if (vineyardParams.length > 1) {
    const placeholders = vineyardParams.map((_, i) => `$${idx + i}`).join(', ');
    conditions.push(`(trim(t.vineyard_name) IN (${placeholders}))`);
    values.push(...vineyardParams);
    idx += vineyardParams.length;
    debug.vineyard = vineyardParams;
  }

  const vineyardGroupParam = searchParams.get('vineyard_group')?.trim();
  if (vineyardGroupParam) {
    conditions.push(`(trim(COALESCE(t.vineyard_group, '')) = $${idx})`);
    values.push(vineyardGroupParam);
    idx++;
    debug.vineyard_group = vineyardGroupParam;
  }

  const truckParam = searchParams.get('truck_id')?.trim();
  if (truckParam) {
    conditions.push(`(trim(t.truck_id) = $${idx})`);
    values.push(truckParam);
    idx++;
    debug.truck_id = truckParam;
  }

  /** Exact match on worker (e.g. tagging page device dropdown). Mutually exclusive with fuzzy `worker`. */
  const deviceParam = searchParams.get('device')?.trim();
  const workerParam = searchParams.get('worker')?.trim();
  if (deviceParam) {
    conditions.push(`(t.worker IS NOT NULL AND trim(t.worker) = $${idx})`);
    values.push(deviceParam);
    idx++;
    debug.device = deviceParam;
  } else if (workerParam) {
    conditions.push(`(t.worker IS NOT NULL AND trim(t.worker) ILIKE $${idx})`);
    values.push(`%${workerParam}%`);
    idx++;
    debug.worker = workerParam;
  }

  const trailermodeParam = searchParams.get('trailermode')?.trim();
  /** Legacy Inspect `trailertype` param — DB column is `trailermode` only. */
  const trailerTypeParam = searchParams.get('trailertype')?.trim();
  const ttFilter = trailermodeParam ?? trailerTypeParam;
  if (ttFilter) {
    conditions.push(`(trim(COALESCE(t.trailermode::text, '')) = $${idx})`);
    values.push(ttFilter);
    idx++;
    if (trailermodeParam) debug.trailermode = trailermodeParam;
    if (trailerTypeParam) debug.trailertype = trailerTypeParam;
  }

  const loadsizeParam = searchParams.get('loadsize')?.trim();
  if (loadsizeParam) {
    conditions.push(`(t.loadsize IS NOT NULL AND trim(t.loadsize::text) = $${idx})`);
    values.push(loadsizeParam);
    idx++;
    debug.loadsize = loadsizeParam;
  }

  /** Repeated `jobId=` (e.g. Inspect from Data Audit pivot) — IN list; capped for safety. */
  const MAX_JOB_ID_IN = 5000;
  const jobIdList = [...new Set(searchParams.getAll('jobId').map((s) => s.trim()).filter(Boolean))].slice(0, MAX_JOB_ID_IN);
  if (jobIdList.length > 0) {
    if (jobIdList.length === 1) {
      conditions.push(`trim(t.job_id::text) = trim($${idx}::text)`);
      values.push(jobIdList[0]);
      idx++;
    } else {
      const placeholders = jobIdList.map((_, i) => `$${idx + i}`).join(', ');
      conditions.push(`trim(t.job_id::text) IN (${placeholders})`);
      values.push(...jobIdList);
      idx += jobIdList.length;
    }
    debug.jobIdInCount = jobIdList.length;
  } else {
    const jobIdContains = searchParams.get('jobIdContains')?.trim();
    const jobIdExact = searchParams.get('jobIdExact')?.trim();
    if (jobIdExact) {
      conditions.push(`trim(t.job_id::text) = trim($${idx}::text)`);
      values.push(jobIdExact);
      idx++;
      debug.jobIdExact = jobIdExact;
    } else if (jobIdContains) {
      conditions.push(`(trim(t.job_id::text) ILIKE $${idx})`);
      values.push(`%${jobIdContains}%`);
      idx++;
      debug.jobIdContains = jobIdContains;
    }
  }

  const stepsFetchedParam = searchParams.get('stepsFetched');
  if (stepsFetchedParam === 'false') {
    conditions.push(`(t.steps_fetched = false OR t.steps_fetched IS NULL)`);
    debug.stepsFetchedFilter = 'false';
  } else if (stepsFetchedParam === 'true') {
    conditions.push(`t.steps_fetched = true`);
    debug.stepsFetchedFilter = 'true';
  }

  const step4to5Param = searchParams.get('step4to5')?.trim();
  if (step4to5Param === '0') {
    conditions.push(`COALESCE(t.step4to5, 0) = 0`);
    debug.step4to5 = '0';
  } else if (step4to5Param === '1') {
    conditions.push(`COALESCE(t.step4to5, 0) = 1`);
    debug.step4to5 = '1';
  }

  /** Step4→5 Data Checks: jobs that do not qualify for Fix (requires customer + template). */
  const blockedViewParam = searchParams.get('blockedView')?.trim().toLowerCase() ?? '';
  if (blockedViewParam === 'normal') {
    conditions.push(`COALESCE(t.step4to5, 0) = 0`);
    conditions.push(
      `(NOT (trim(COALESCE(t.step_4_name, '')) = 'Job Completed') OR trim(COALESCE(t.step_5_name, '')) = 'Job Completed' OR t.step_5_completed_at IS NOT NULL)`
    );
    debug.blockedView = 'normal';
  } else if (blockedViewParam === 'rerun') {
    conditions.push(`COALESCE(t.step4to5, 0) = 1`);
    conditions.push(
      `NOT (trim(COALESCE(t.step_4_name, '')) = 'Arrive Winery' AND trim(COALESCE(t.step_5_name, '')) = 'Job Completed')`
    );
    debug.blockedView = 'rerun';
  } else if (blockedViewParam === 'ordering') {
    conditions.push(`COALESCE(t.step4to5, 0) = 1`);
    conditions.push(`trim(COALESCE(t.step_4_name, '')) = 'Arrive Winery'`);
    conditions.push(`trim(COALESCE(t.step_5_name, '')) = 'Job Completed'`);
    conditions.push(
      `(t.step_4_completed_at IS NOT NULL AND t.step_5_completed_at IS NOT NULL AND t.step_4_completed_at < t.step_5_completed_at)`
    );
    debug.blockedView = 'ordering';
  }

  return { conditions, values, debug };
}
