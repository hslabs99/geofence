import { execute, query } from '@/lib/db';

export type DiversionType = {
  id: number;
  name: string;
  sort_order: number;
};

export type JobDiversion = {
  id: number;
  driver_job_id: number;
  driver: string | null;
  type_name: string;
  note: string | null;
  /** 15, 30, 45, 60, or 60+ for more than 60 minutes. */
  expected_duration: string | null;
  /** Whole seconds from the start clock to the end clock, written when the diversion stops. */
  duration_seconds: number | null;
  started_at: string;
  ended_at: string | null;
  /** Set when a later gate click closed this diversion. Null when the driver stopped it themselves. */
  ended_by_step: number | null;
};

export const EXPECTED_DURATIONS = ['15', '30', '45', '60', '60+'] as const;
export type ExpectedDuration = (typeof EXPECTED_DURATIONS)[number];

export function expectedDurationLabel(value: string | null | undefined): string {
  if (value === '60+') return 'More than 60 min';
  if (value === '15' || value === '30' || value === '45' || value === '60') return `${value} min`;
  return '—';
}

const SEED = ['Breakdown', 'Break / rest', 'Refuel'];

let ensured = false;

export async function ensureDriverDiversionsSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_diversion_types (
      id          serial PRIMARY KEY,
      name        text NOT NULL,
      sort_order  integer NOT NULL DEFAULT 0
    )`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_tbl_driver_diversion_types_name
      ON tbl_driver_diversion_types (lower(btrim(name)))`);
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_job_diversions (
      id             serial PRIMARY KEY,
      driver_job_id  integer NOT NULL,
      driver         text,
      type_name         text NOT NULL,
      note              text,
      expected_duration text,
      duration_seconds  integer,
      started_at        timestamp without time zone NOT NULL,
      ended_at          timestamp without time zone,
      ended_by_step     integer
    )`);
  await execute(`ALTER TABLE tbl_driver_job_diversions ADD COLUMN IF NOT EXISTS expected_duration text`);
  await execute(`ALTER TABLE tbl_driver_job_diversions ADD COLUMN IF NOT EXISTS duration_seconds integer`);
  await execute(`ALTER TABLE tbl_driver_job_diversions ADD COLUMN IF NOT EXISTS ended_by_step integer`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_job_diversions_job
      ON tbl_driver_job_diversions (driver_job_id, id)`);
  const existing = await query<{ n: number }>(`SELECT COUNT(*)::int AS n FROM tbl_driver_diversion_types`);
  if ((existing[0]?.n ?? 0) === 0) {
    for (let i = 0; i < SEED.length; i++) {
      await execute(
        `INSERT INTO tbl_driver_diversion_types (name, sort_order) VALUES ($1, $2)`,
        [SEED[i], i + 1],
      );
    }
  }
  ensured = true;
}

const TYPE_COLUMNS = `id, name, sort_order`;
const DIVERSION_COLUMNS = `
  id,
  driver_job_id,
  driver,
  type_name,
  note,
  expected_duration,
  duration_seconds,
  to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started_at,
  to_char(ended_at, 'YYYY-MM-DD HH24:MI:SS') AS ended_at,
  ended_by_step
`;

export async function listDiversionTypes(): Promise<DiversionType[]> {
  await ensureDriverDiversionsSchema();
  return query<DiversionType>(
    `SELECT ${TYPE_COLUMNS} FROM tbl_driver_diversion_types ORDER BY sort_order, id`,
  );
}

export async function createDiversionType(name: string): Promise<DiversionType> {
  await ensureDriverDiversionsSchema();
  const clean = name.trim();
  if (!clean) throw new Error('Name is required');
  if (clean.length > 80) throw new Error('Name is too long');
  const next = await query<{ n: number }>(`SELECT COALESCE(MAX(sort_order), 0)::int + 1 AS n FROM tbl_driver_diversion_types`);
  const rows = await query<DiversionType>(
    `INSERT INTO tbl_driver_diversion_types (name, sort_order)
     VALUES ($1, $2)
     RETURNING ${TYPE_COLUMNS}`,
    [clean, next[0]?.n ?? 1],
  );
  return rows[0];
}

export async function renameDiversionType(id: number, name: string): Promise<DiversionType | null> {
  await ensureDriverDiversionsSchema();
  const clean = name.trim();
  if (!clean) throw new Error('Name is required');
  if (clean.length > 80) throw new Error('Name is too long');
  const rows = await query<DiversionType>(
    `UPDATE tbl_driver_diversion_types SET name = $2 WHERE id = $1 RETURNING ${TYPE_COLUMNS}`,
    [id, clean],
  );
  return rows[0] ?? null;
}

export async function deleteDiversionType(id: number): Promise<boolean> {
  await ensureDriverDiversionsSchema();
  const n = await execute(`DELETE FROM tbl_driver_diversion_types WHERE id = $1`, [id]);
  return n > 0;
}

export async function listJobDiversions(driverJobId: number): Promise<JobDiversion[]> {
  await ensureDriverDiversionsSchema();
  return query<JobDiversion>(
    `SELECT ${DIVERSION_COLUMNS}
     FROM tbl_driver_job_diversions
     WHERE driver_job_id = $1
     ORDER BY id ASC`,
    [driverJobId],
  );
}

export function isExpectedDuration(value: string): value is ExpectedDuration {
  return (EXPECTED_DURATIONS as readonly string[]).includes(value);
}

/** Seconds between two verbatim clock strings. No timezone shift. */
export function clockSecondsBetween(start: string, end: string): number | null {
  const read = (stamp: string) => {
    const match = stamp.trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
    if (!match) return null;
    return Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]), Number(match[4]), Number(match[5]), Number(match[6]));
  };
  const a = read(start);
  const b = read(end);
  if (a == null || b == null) return null;
  return Math.max(0, Math.round((b - a) / 1000));
}

export async function startJobDiversion(input: {
  driverJobId: number;
  driver: string | null;
  typeName: string;
  note: string | null;
  expected: string;
  at: string;
}): Promise<JobDiversion> {
  await ensureDriverDiversionsSchema();
  const types = await listDiversionTypes();
  const match = types.find((type) => type.name.toLowerCase() === input.typeName.trim().toLowerCase());
  if (!match) throw new Error('Choose a diversion from the list');
  if (!isExpectedDuration(input.expected)) throw new Error('Choose how long you expect this to take');
  const open = await query<{ id: number }>(
    `SELECT id FROM tbl_driver_job_diversions WHERE driver_job_id = $1 AND ended_at IS NULL LIMIT 1`,
    [input.driverJobId],
  );
  if (open.length > 0) throw new Error('A diversion is already running');
  const note = input.note?.trim() ? input.note.trim().slice(0, 500) : null;
  const rows = await query<JobDiversion>(
    `INSERT INTO tbl_driver_job_diversions (driver_job_id, driver, type_name, note, expected_duration, started_at)
     VALUES ($1, $2, $3, $4, $5, CAST($6 AS timestamp without time zone))
     RETURNING ${DIVERSION_COLUMNS}`,
    [input.driverJobId, input.driver, match.name, note, input.expected, input.at],
  );
  return rows[0];
}

export async function openJobDiversion(driverJobId: number): Promise<JobDiversion | null> {
  await ensureDriverDiversionsSchema();
  const rows = await query<JobDiversion>(
    `SELECT ${DIVERSION_COLUMNS}
     FROM tbl_driver_job_diversions
     WHERE driver_job_id = $1 AND ended_at IS NULL
     ORDER BY id DESC
     LIMIT 1`,
    [driverJobId],
  );
  return rows[0] ?? null;
}

export async function stopJobDiversion(
  driverJobId: number,
  at: string,
  endedByStep: number | null = null,
): Promise<JobDiversion | null> {
  await ensureDriverDiversionsSchema();
  const current = await openJobDiversion(driverJobId);
  if (!current) return null;
  const duration = clockSecondsBetween(current.started_at, at);
  const rows = await query<JobDiversion>(
    `UPDATE tbl_driver_job_diversions
     SET ended_at = CAST($2 AS timestamp without time zone),
         duration_seconds = $3,
         ended_by_step = $4
     WHERE id = $1 AND ended_at IS NULL
     RETURNING ${DIVERSION_COLUMNS}`,
    [current.id, at, duration, endedByStep],
  );
  return rows[0] ?? null;
}

export async function deleteJobDiversions(driverJobId: number): Promise<void> {
  await ensureDriverDiversionsSchema();
  await execute(`DELETE FROM tbl_driver_job_diversions WHERE driver_job_id = $1`, [driverJobId]);
}
