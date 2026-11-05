import { query, execute } from '@/lib/db';
import { deleteJobDiversions, listJobDiversions } from '@/lib/driver-diversions';
import { DRIVER_GATES, gateManual, type DriverJob } from '@/lib/driver-jobs';
import { type JobActor } from '@/lib/job-actor';

export type ClickOutcome = 'caution_decline' | 'caution_accept' | 'auto_accept';

export type DriverLogEvent = {
  id: string;
  at: string;
  kind: 'fence' | 'click' | 'ghost';
  title: string;
  detail: string | null;
};

const OUTCOME_LABEL: Record<ClickOutcome, string> = {
  caution_decline: 'Cautionary decline',
  caution_accept: 'Cautionary accept',
  auto_accept: 'Automatic accept',
};

let logEnsured = false;

export async function ensureDriverJobLogSchema(): Promise<void> {
  if (logEnsured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_job_log (
      id             serial PRIMARY KEY,
      driver_job_id  integer NOT NULL,
      at             timestamp without time zone NOT NULL,
      kind           text NOT NULL,
      step           integer,
      outcome        text,
      detail         text
    )`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_job_log_job
      ON tbl_driver_job_log (driver_job_id, id)`);
  await execute(`ALTER TABLE tbl_driver_job_log ADD COLUMN IF NOT EXISTS actor_name text`);
  await execute(`ALTER TABLE tbl_driver_job_log ADD COLUMN IF NOT EXISTS actor_email text`);
  await execute(`ALTER TABLE tbl_driver_job_log ADD COLUMN IF NOT EXISTS actor_role text`);
  logEnsured = true;
}

/** Testing reset: drop stored clicks and hide fence lines from before this clock. */
export async function clearDriverJobLog(driverJobId: number, at: string): Promise<void> {
  await ensureDriverJobLogSchema();
  await execute(`DELETE FROM tbl_driver_job_log WHERE driver_job_id = $1`, [driverJobId]);
  await deleteJobDiversions(driverJobId);
  await execute(
    `UPDATE tbl_driver_jobs
     SET log_cleared_at = CAST($2 AS timestamp without time zone)
     WHERE id = $1`,
    [driverJobId, at],
  );
}

export async function recordDriverClick(input: {
  driverJobId: number;
  at: string;
  step: number;
  outcome: ClickOutcome;
  detail: string | null;
  actor?: JobActor | null;
}): Promise<void> {
  await ensureDriverJobLogSchema();
  await execute(
    `INSERT INTO tbl_driver_job_log (driver_job_id, at, kind, step, outcome, detail, actor_name, actor_email, actor_role)
     VALUES ($1, CAST($2 AS timestamp without time zone), 'click', $3, $4, $5, $6, $7, $8)`,
    [
      input.driverJobId,
      input.at,
      input.step,
      input.outcome,
      input.detail,
      input.actor?.name ?? null,
      input.actor?.email ?? null,
      input.actor?.role ?? null,
    ],
  );
}

/** Admin opened the job, or did something that is not a gate tap. A repeat in the same second is ignored. */
export async function recordStaffControl(input: {
  driverJobId: number;
  at: string;
  detail: string;
  actor: JobActor;
}): Promise<void> {
  await ensureDriverJobLogSchema();
  const existing = await query<{ id: number }>(
    `SELECT id
     FROM tbl_driver_job_log
     WHERE driver_job_id = $1
       AND kind = 'ghost'
       AND lower(btrim(actor_email)) = lower(btrim($2))
       AND at = CAST($3 AS timestamp without time zone)
       AND detail = $4
     LIMIT 1`,
    [input.driverJobId, input.actor.email, input.at, input.detail],
  );
  if (existing.length > 0) return;
  await execute(
    `INSERT INTO tbl_driver_job_log (driver_job_id, at, kind, step, outcome, detail, actor_name, actor_email, actor_role)
     VALUES ($1, CAST($2 AS timestamp without time zone), 'ghost', NULL, NULL, $3, $4, $5, $6)`,
    [input.driverJobId, input.at, input.detail, input.actor.name, input.actor.email, input.actor.role],
  );
}

type StoredClick = {
  id: number;
  at: string;
  kind: string;
  step: number | null;
  outcome: string | null;
  detail: string | null;
  actor_name: string | null;
  actor_role: string | null;
};

function byActor(row: Pick<StoredClick, 'actor_name' | 'actor_role'>): string | null {
  const name = row.actor_name?.trim() || '';
  const role = row.actor_role?.trim() || '';
  if (!name || !role) return null;
  return `By ${role} ${name}`;
}

type FenceFix = {
  id: number;
  gpstime_nz: string | null;
  polltime: string | null;
  fencename: string | null;
};

function namedFence(value: string | null): string | null {
  const name = value?.trim() ?? '';
  if (!name || name.toLowerCase() === 'outside fence') return null;
  return name;
}

/** Calendar day of a stored clock string, so a drive before the start tap still appears. */
function dayStart(stamp: string | null | undefined): string {
  const day = stamp?.trim().slice(0, 10) ?? '';
  return /^\d{4}-\d{2}-\d{2}$/.test(day) ? `${day} 00:00:00` : '';
}

function gateTitle(step: number): string {
  if (step === 0) return 'Confirm job details';
  return DRIVER_GATES.find((gate) => gate.step === step)?.title ?? `Step ${step}`;
}

function isOutcome(value: string | null): value is ClickOutcome {
  return value === 'caution_decline' || value === 'caution_accept' || value === 'auto_accept';
}

/** Clicks stored for this job, plus fence enter/exit taken from this truck's location rows. Newest first. */
export async function readDriverJobLog(job: DriverJob): Promise<DriverLogEvent[]> {
  await ensureDriverJobLogSchema();
  const clearedRows = await query<{ log_cleared_at: string | null }>(
    `SELECT to_char(log_cleared_at, 'YYYY-MM-DD HH24:MI:SS') AS log_cleared_at
     FROM tbl_driver_jobs
     WHERE id = $1`,
    [job.id],
  );
  const clearedAt = clearedRows[0]?.log_cleared_at?.trim() || '';
  const stored = await query<StoredClick>(
    `SELECT id,
            to_char(at, 'YYYY-MM-DD HH24:MI:SS') AS at,
            kind,
            step,
            outcome,
            detail,
            actor_name,
            actor_role
     FROM tbl_driver_job_log
     WHERE driver_job_id = $1 AND kind IN ('click', 'ghost')
     ORDER BY id ASC`,
    [job.id],
  );

  const events: Array<DriverLogEvent & { seq: number }> = [];
  let seq = 0;

  for (const row of stored) {
    if (row.kind === 'ghost') {
      events.push({
        id: `ghost-${row.id}`,
        at: row.at,
        kind: 'ghost',
        title: 'Admin control',
        detail: [byActor(row), row.detail?.trim() || null].filter(Boolean).join(' — ') || 'Admin control',
        seq: seq++,
      });
      continue;
    }
    if (!isOutcome(row.outcome) || row.step == null) continue;
    events.push({
      id: `click-${row.id}`,
      at: row.at,
      kind: 'click',
      title: `Step ${row.step} · ${gateTitle(row.step)}`,
      detail: [byActor(row), OUTCOME_LABEL[row.outcome], row.detail?.trim() || null].filter(Boolean).join(' — '),
      seq: seq++,
    });
  }

  for (const gate of DRIVER_GATES) {
    const at = job[gate.key];
    if (!at) continue;
    const already = stored.some((row) => row.step === gate.step && row.at === at && row.outcome !== 'caution_decline');
    if (already) continue;
    const outcome: ClickOutcome = gateManual(job, gate.step) ? 'caution_accept' : 'auto_accept';
    events.push({
      id: `step-${gate.step}-${at}`,
      at,
      kind: 'click',
      title: `Step ${gate.step} · ${gate.title}`,
      detail: OUTCOME_LABEL[outcome],
      seq: seq++,
    });
  }

  if (job.step_0_click?.trim()) {
    const at = job.step_0_click.trim();
    const already = stored.some((row) => row.step === 0 && row.at === at && row.outcome !== 'caution_decline');
    if (!already) {
      const detail = [job.confirmed_driver, job.truck_rego, job.trailer_rego]
        .map((value) => value?.trim() || '')
        .filter(Boolean)
        .join(' · ');
      events.push({
        id: `step-0-${at}`,
        at,
        kind: 'click',
        title: 'Step 0 · Confirm job details',
        detail: detail || OUTCOME_LABEL.auto_accept,
        seq: seq++,
      });
    }
  }

  const since = dayStart(job.step_1_click) || dayStart(job.planned_start_time);
  const imei = job.imei?.trim() || '';
  const truck = job.truck_id?.trim() || '';
  if (imei || truck) {
    const fixes = await query<FenceFix>(
      `SELECT id,
              to_char(gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz,
              polltime,
              fencename
       FROM tbl_current_locations
       WHERE (
         ($1 <> '' AND btrim(imei) = $1)
         OR ($2 <> '' AND lower(btrim(truck_id)) = lower(btrim($2)))
       )
       ORDER BY id DESC
       LIMIT 500`,
      [imei, truck],
    );
    fixes.reverse();
    let start = 0;
    if (since) {
      const first = fixes.findIndex((fix) => (fix.gpstime_nz || fix.polltime || '') >= since);
      start = first === -1 ? fixes.length : first;
    }
    let current = start > 0 ? namedFence(fixes[start - 1].fencename) : null;
    for (let i = start; i < fixes.length; i++) {
      const fix = fixes[i];
      if (fix.fencename == null) continue;
      const next = namedFence(fix.fencename);
      if (next === current) continue;
      const at = fix.gpstime_nz || fix.polltime || '';
      if (!at) {
        current = next;
        continue;
      }
      if (current && next !== current) {
        events.push({
          id: `fence-${fix.id}-exit`,
          at,
          kind: 'fence',
          title: `Left ${current}`,
          detail: null,
          seq: seq++,
        });
      }
      if (next) {
        events.push({
          id: `fence-${fix.id}-enter`,
          at,
          kind: 'fence',
          title: `Entered ${next}`,
          detail: null,
          seq: seq++,
        });
      }
      current = next;
    }
  }

  const diversions = await listJobDiversions(job.id);
  for (const row of diversions) {
    const note = row.note?.trim() || null;
    const expected = row.expected_duration === '60+'
      ? 'expected more than 60 min'
      : row.expected_duration
        ? `expected ${row.expected_duration} min`
        : null;
    events.push({
      id: `diversion-${row.id}-start`,
      at: row.started_at,
      kind: 'click',
      title: `Diversion · ${row.type_name}`,
      detail: ['Started', expected, note].filter(Boolean).join(' — '),
      seq: seq++,
    });
    if (row.ended_at) {
      const gate = DRIVER_GATES.find((item) => item.step === Number(row.ended_by_step));
      const closed = gate
        ? `Ended when moving to step ${gate.step} · ${gate.title}`
        : 'Ended';
      events.push({
        id: `diversion-${row.id}-end`,
        at: row.ended_at,
        kind: 'click',
        title: `Diversion · ${row.type_name}`,
        detail: [closed, note].filter(Boolean).join(' — '),
        seq: seq++,
      });
    }
  }

  const visible = clearedAt ? events.filter((event) => event.at >= clearedAt) : events;
  visible.sort((a, b) => (a.at < b.at ? 1 : a.at > b.at ? -1 : b.seq - a.seq));
  return visible.map((event) => ({
    id: event.id,
    at: event.at,
    kind: event.kind,
    title: event.title,
    detail: event.detail,
  }));
}
