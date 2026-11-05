/** VWork API v5 jobs. https://api.vworkapp.com/api/v5/jobs.xml — datetimes stay the raw XML text. */

const DEFAULT_API_BASE = 'https://api.vworkapp.com';
const PER_PAGE = 100;
const MAX_PAGES = 30;

export type VworkFeedRow = {
  job_id: string;
  customer: string | null;
  template: string | null;
  worker: string | null;
  status: string | null;
  third_party_id: string | null;
  planned_start_time: string | null;
  planned_duration: string | null;
  actual_start_time: string | null;
  worker_duration_mins: string | null;
  proof_of_delivery: string | null;
  number_of_steps: string | null;
  pod_signed_by: string | null;
  pod_signed_at: string | null;
  asset_id: string | null;
  asset_name: string | null;
  customer_third_party_id: string | null;
  groups: string | null;
  create_date: string | null;
  source_updated_at: string | null;
  step_1_name: string | null;
  step_1_completed_at: string | null;
  step_1_address: string | null;
  step_2_name: string | null;
  step_2_completed_at: string | null;
  step_2_address: string | null;
  step_3_name: string | null;
  step_3_completed_at: string | null;
  step_3_address: string | null;
  step_4_name: string | null;
  step_4_completed_at: string | null;
  step_4_address: string | null;
  step_5_name: string | null;
  step_5_completed_at: string | null;
  step_5_address: string | null;
  truck_id: string | null;
  truck_rego: string | null;
  trailer_rego: string | null;
  driver_name: string | null;
  vineyard_name: string | null;
  pickup_location_map: string | null;
  field_docket_number: string | null;
  delivery_winery: string | null;
  delivery_location_map: string | null;
  weighbridge_weight: string | null;
  driver_notes: string | null;
  comments: string | null;
  field_notes: string | null;
  contains_mog: string | null;
  number_of_loads: string | null;
  loadsize: string | null;
  custom_fields: string | null;
};

type XmlEl = { name: string; attrs: Record<string, string>; children: XmlEl[]; text: string };

const CUSTOM_TO_COLUMN: Record<string, keyof VworkFeedRow> = {
  truckid: 'truck_id',
  truckrego: 'truck_rego',
  trailerrego: 'trailer_rego',
  drivername: 'driver_name',
  vineyard: 'vineyard_name',
  vineyardname: 'vineyard_name',
  vinename: 'vineyard_name',
  deliverywinery: 'delivery_winery',
  winery: 'delivery_winery',
  fielddocketnumber: 'field_docket_number',
  drivernotes: 'driver_notes',
  comments: 'comments',
  fieldnotes: 'field_notes',
  loadsize: 'loadsize',
  numberofloads: 'number_of_loads',
  pickuplocation: 'pickup_location_map',
  pickuplocationmap: 'pickup_location_map',
  deliverylocation: 'delivery_location_map',
  deliverylocationmap: 'delivery_location_map',
  weighbridgeweight: 'weighbridge_weight',
  containermod: 'contains_mog',
  containsmog: 'contains_mog',
};

function decodeXml(value: string): string {
  return value
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function parseAttrs(raw: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  const re = /([A-Za-z_:][\w:.-]*)\s*=\s*"([^"]*)"/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(raw))) attrs[match[1]] = decodeXml(match[2]);
  return attrs;
}

export function parseXmlElement(xml: string): XmlEl {
  const cleaned = xml.replace(/<\?xml[\s\S]*?\?>/g, '').replace(/<!--[\s\S]*?-->/g, '');
  const re = /<(\/?)([A-Za-z_][\w:.-]*)([^>]*?)(\/?)>/g;
  const root: XmlEl = { name: '#root', attrs: {}, children: [], text: '' };
  const stack = [root];
  let last = 0;
  let match: RegExpExecArray | null;
  while ((match = re.exec(cleaned))) {
    const text = decodeXml(cleaned.slice(last, match.index));
    const parent = stack[stack.length - 1];
    if (text.trim()) parent.text += text;
    const name = match[2];
    if (match[1] === '/') {
      if (stack.length > 1 && stack[stack.length - 1].name === name) stack.pop();
    } else {
      const el: XmlEl = { name, attrs: parseAttrs(match[3]), children: [], text: '' };
      parent.children.push(el);
      if (match[4] !== '/') stack.push(el);
    }
    last = re.lastIndex;
  }
  return root.children[0] ?? root;
}

function childText(el: XmlEl, name: string): string | null {
  const found = el.children.find((child) => child.name === name);
  const text = found?.text.trim() ?? '';
  return text === '' ? null : text;
}

function blankRow(): VworkFeedRow {
  return {
    job_id: '',
    customer: null,
    template: null,
    worker: null,
    status: null,
    third_party_id: null,
    planned_start_time: null,
    planned_duration: null,
    actual_start_time: null,
    worker_duration_mins: null,
    proof_of_delivery: null,
    number_of_steps: null,
    pod_signed_by: null,
    pod_signed_at: null,
    asset_id: null,
    asset_name: null,
    customer_third_party_id: null,
    groups: null,
    create_date: null,
    source_updated_at: null,
    step_1_name: null,
    step_1_completed_at: null,
    step_1_address: null,
    step_2_name: null,
    step_2_completed_at: null,
    step_2_address: null,
    step_3_name: null,
    step_3_completed_at: null,
    step_3_address: null,
    step_4_name: null,
    step_4_completed_at: null,
    step_4_address: null,
    step_5_name: null,
    step_5_completed_at: null,
    step_5_address: null,
    truck_id: null,
    truck_rego: null,
    trailer_rego: null,
    driver_name: null,
    vineyard_name: null,
    pickup_location_map: null,
    field_docket_number: null,
    delivery_winery: null,
    delivery_location_map: null,
    weighbridge_weight: null,
    driver_notes: null,
    comments: null,
    field_notes: null,
    contains_mog: null,
    number_of_loads: null,
    loadsize: null,
    custom_fields: null,
  };
}

function applySteps(row: VworkFeedRow, job: XmlEl): void {
  const stepsEl = job.children.find((child) => child.name === 'steps');
  const steps = stepsEl?.children.filter((child) => child.name === 'step') ?? [];
  row.number_of_steps = String(steps.length);
  steps.slice(0, 5).forEach((step, index) => {
    const n = index + 1;
    const location = step.children.find((child) => child.name === 'location');
    (row as unknown as Record<string, string | null>)[`step_${n}_name`] = childText(step, 'name');
    (row as unknown as Record<string, string | null>)[`step_${n}_completed_at`] = childText(step, 'completed_at');
    (row as unknown as Record<string, string | null>)[`step_${n}_address`] = location ? childText(location, 'formatted_address') : null;
  });
}

function applyCustomFields(row: VworkFeedRow, job: XmlEl): void {
  const wrap = job.children.find((child) => child.name === 'custom_fields');
  const fields = wrap?.children.filter((child) => child.name === 'custom_field') ?? [];
  const extra: Record<string, string> = {};
  const target = row as unknown as Record<string, string | null>;
  for (const field of fields) {
    const name = childText(field, 'name');
    const value = childText(field, 'value');
    if (!name || value == null) continue;
    const key = name.toLowerCase().replace(/[^a-z0-9]/g, '');
    const column = CUSTOM_TO_COLUMN[key];
    if (column && target[column] == null) target[column] = value;
    else extra[name] = value;
  }
  row.custom_fields = Object.keys(extra).length > 0 ? JSON.stringify(extra) : null;
}

export function jobFromXml(job: XmlEl): VworkFeedRow | null {
  const id = childText(job, 'id');
  if (!id) return null;
  const row = blankRow();
  row.job_id = id;
  row.customer = childText(job, 'customer_name');
  row.template = childText(job, 'template_name');
  row.worker = childText(job, 'worker_name');
  row.status = childText(job, 'status');
  row.third_party_id = childText(job, 'third_party_id');
  row.planned_start_time = childText(job, 'planned_start_at');
  row.planned_duration = childText(job, 'planned_duration');
  row.actual_start_time = childText(job, 'actual_start_at');
  row.worker_duration_mins = childText(job, 'worker_duration');
  row.proof_of_delivery = childText(job, 'has_pod');
  row.pod_signed_by = childText(job, 'signed_by');
  row.pod_signed_at = childText(job, 'signed_at');
  row.asset_id = childText(job, 'asset_id');
  row.asset_name = childText(job, 'asset_name');
  row.customer_third_party_id = childText(job, 'customer_third_party_id');
  row.groups = childText(job, 'group_ids');
  row.create_date = childText(job, 'created_at');
  row.source_updated_at = childText(job, 'updated_at');
  applySteps(row, job);
  applyCustomFields(row, job);
  return row;
}

export function parseVworkJobsXml(xml: string): { jobs: VworkFeedRow[]; page: number; totalPages: number; totalEntries: number } {
  const root = parseXmlElement(xml);
  const jobsEl = root.name === 'jobs' ? root : root.children.find((child) => child.name === 'jobs');
  if (!jobsEl) return { jobs: [], page: 1, totalPages: 1, totalEntries: 0 };
  const jobs = jobsEl.children
    .filter((child) => child.name === 'job')
    .map(jobFromXml)
    .filter((job): job is VworkFeedRow => job != null);
  const page = Number(jobsEl.attrs.current_page ?? '1') || 1;
  const totalPages = Number(jobsEl.attrs.total_pages ?? '1') || 1;
  const totalEntries = Number(jobsEl.attrs.total_entries ?? String(jobs.length)) || jobs.length;
  return { jobs, page, totalPages, totalEntries };
}

/** ISO-8601 local clock for the updated_at search bound. Not stored as a job time. */
export function isoLocal(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  const offset = -date.getTimezoneOffset();
  const sign = offset >= 0 ? '+' : '-';
  const abs = Math.abs(offset);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`;
}

export type VworkKeyState = 'missing' | 'placeholder' | 'ready';
export type VworkAccountId = 'delegat' | 'others';

export type VworkAccountStatus = {
  id: VworkAccountId;
  label: string;
  state: VworkKeyState;
};

const ACCOUNT_DEFS: { id: VworkAccountId; label: string; envNames: string[] }[] = [
  { id: 'delegat', label: 'Delegat', envNames: ['VWORK_API_KEY_DELEGAT', 'VWORK_API_KEY'] },
  { id: 'others', label: 'Others', envNames: ['VWORK_API_KEY_OTHERS'] },
];

function keyState(raw: string): VworkKeyState {
  const key = raw.trim();
  if (!key) return 'missing';
  if (/^replace/i.test(key) || key.toLowerCase() === 'placeholder' || key.toLowerCase() === 'your-api-key') return 'placeholder';
  return 'ready';
}

function accountRecords(): (VworkAccountStatus & { apiKey: string | null })[] {
  return ACCOUNT_DEFS.map((def) => {
    const raw = def.envNames.map((name) => process.env[name]?.trim() ?? '').find((value) => value !== '') ?? '';
    const state = keyState(raw);
    return { id: def.id, label: def.label, state, apiKey: state === 'ready' ? raw : null };
  });
}

/** Delegat and Others, without the key values. */
export function vworkAccountStatus(): VworkAccountStatus[] {
  return accountRecords().map(({ id, label, state }) => ({ id, label, state }));
}

export function vworkApiKeyState(): VworkKeyState {
  const accounts = accountRecords();
  if (accounts.some((account) => account.state === 'ready')) return 'ready';
  if (accounts.some((account) => account.state === 'placeholder')) return 'placeholder';
  return 'missing';
}

export function vworkApiConfigured(): boolean {
  return accountRecords().some((account) => account.apiKey);
}

function jobsUrl(): string {
  const base = (process.env.VWORK_API_BASE?.trim() || DEFAULT_API_BASE).replace(/\/$/, '');
  return `${base}/api/v5/jobs.xml`;
}

export function readyVworkAccounts(): { id: VworkAccountId; label: string; apiKey: string }[] {
  return accountRecords().flatMap((account) => (account.apiKey ? [{ id: account.id, label: account.label, apiKey: account.apiKey }] : []));
}

export async function listVworkJobsUpdatedSince(
  updatedAt: string,
  apiKey?: string,
): Promise<{ jobs: VworkFeedRow[]; pages: number; truncated: boolean }> {
  const key = apiKey?.trim() || readyVworkAccounts()[0]?.apiKey;
  if (!key) throw new Error('No VWork API key is ready');
  const jobs: VworkFeedRow[] = [];
  let page = 1;
  let totalPages = 1;
  let pages = 0;
  while (page <= totalPages && pages < MAX_PAGES) {
    const url = new URL(jobsUrl());
    url.searchParams.set('api_key', key);
    url.searchParams.set('updated_at', updatedAt);
    url.searchParams.set('per_page', String(PER_PAGE));
    url.searchParams.set('page', String(page));
    url.searchParams.set('sort_by', 'updated_at');
    url.searchParams.set('sort_dir', 'desc');
    const res = await fetch(url, { headers: { 'Accept-Encoding': 'gzip' }, cache: 'no-store' });
    const xml = await res.text();
    if (!res.ok) {
      throw new Error(`VWork jobs ${res.status}: ${xml.replace(/\s+/g, ' ').slice(0, 240)}`);
    }
    const parsed = parseVworkJobsXml(xml);
    jobs.push(...parsed.jobs);
    totalPages = parsed.totalPages;
    pages += 1;
    page += 1;
  }
  return { jobs, pages, truncated: page <= totalPages };
}

function apiBase(): string {
  return (process.env.VWORK_API_BASE?.trim() || DEFAULT_API_BASE).replace(/\/$/, '');
}

/** Clock digits stay as the phone recorded them. The offset is Pacific/Auckland so VWork shows that same clock. */
export function vworkCompletedAt(phoneStamp: string): string {
  const match = phoneStamp.trim().match(/^(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
  if (!match) throw new Error('Step time must be YYYY-MM-DD HH:MM:SS');
  const year = Number(match[1].slice(0, 4));
  const month = Number(match[1].slice(5, 7));
  const day = Number(match[1].slice(8, 10));
  const hour = Number(match[2]);
  const minute = Number(match[3]);
  const second = Number(match[4]);
  const utc = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', {
      timeZone: 'Pacific/Auckland',
      hour12: false,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    })
      .formatToParts(utc)
      .map((part) => [part.type, part.value]),
  );
  const shownHour = parts.hour === '24' ? 0 : Number(parts.hour);
  const shown = Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day), shownHour, Number(parts.minute), Number(parts.second));
  const minutes = Math.round((shown - utc.getTime()) / 60000);
  const sign = minutes >= 0 ? '+' : '-';
  const abs = Math.abs(minutes);
  const offset = `${sign}${String(Math.floor(abs / 60)).padStart(2, '0')}:${String(abs % 60).padStart(2, '0')}`;
  return `${match[1]}T${match[2]}:${match[3]}:${match[4]}${offset}`;
}

function xmlText(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/** Marks one named step complete on the VWork job. Other fields stay as they are. */
export async function completeVworkJobStep(input: {
  account: VworkAccountId;
  vworkJobId: string;
  stepName: string;
  completedAt: string;
}): Promise<void> {
  const account = readyVworkAccounts().find((item) => item.id === input.account);
  if (!account) throw new Error(`${input.account} API key is not ready`);
  const url = new URL(`${apiBase()}/api/v5/jobs/${encodeURIComponent(input.vworkJobId)}.xml`);
  url.searchParams.set('api_key', account.apiKey);
  const body = `<?xml version="1.0" encoding="utf-8"?><job><steps type="array"><step><name>${xmlText(input.stepName)}</name><completed_at>${xmlText(input.completedAt)}</completed_at></step></steps></job>`;
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Accept-Encoding': 'gzip', 'Content-Type': 'application/xml; charset=utf-8' },
    body,
    cache: 'no-store',
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`VWork step ${res.status}: ${text.replace(/\s+/g, ' ').slice(0, 240)}`);
  }
}

export type VworkUserRow = { id: string; name: string };

export async function listVworkUsers(apiKey: string): Promise<{ users: VworkUserRow[]; pages: number; truncated: boolean }> {
  const users: VworkUserRow[] = [];
  let page = 1;
  let totalPages = 1;
  let pages = 0;
  while (page <= totalPages && pages < MAX_PAGES) {
    const url = new URL(`${apiBase()}/api/v5/users.xml`);
    url.searchParams.set('api_key', apiKey);
    url.searchParams.set('per_page', String(PER_PAGE));
    url.searchParams.set('page', String(page));
    const res = await fetch(url, { headers: { 'Accept-Encoding': 'gzip' }, cache: 'no-store' });
    const xml = await res.text();
    if (!res.ok) throw new Error(`VWork users ${res.status}: ${xml.replace(/\s+/g, ' ').slice(0, 240)}`);
    const root = parseXmlElement(xml);
    const usersEl = root.name === 'users' ? root : root.children.find((child) => child.name === 'users');
    const found = (usersEl?.children ?? root.children).filter((child) => child.name === 'user');
    for (const user of found) {
      const id = childText(user, 'id');
      const name = childText(user, 'name');
      if (id && name) users.push({ id, name });
    }
    totalPages = Number(usersEl?.attrs.total_pages ?? '1') || 1;
    pages += 1;
    page += 1;
  }
  return { users, pages, truncated: page <= totalPages };
}
