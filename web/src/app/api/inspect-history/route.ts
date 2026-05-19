import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

const MAX_HISTORY = 10;
const NOTE_MAX_LEN = 500;

type InspectHistoryRow = {
  id: number;
  job_id: string;
  delivery_winery: string | null;
  vineyard_name: string | null;
  worker: string | null;
  actual_start_time: string | null;
  truck_id: string | null;
  note: string | null;
};

const rowSelect = `
  h.id,
  h.job_id,
  v.delivery_winery,
  v.vineyard_name,
  v.worker,
  to_char(v.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
  v.truck_id::text AS truck_id,
  h.note
`;

async function pruneUnpinnedHistory() {
  await execute(
    `DELETE FROM tbl_inspect_history
     WHERE note IS NULL
       AND id NOT IN (
         SELECT id
         FROM (
           SELECT DISTINCT ON (job_id) id, job_id, created_at
           FROM tbl_inspect_history
           WHERE note IS NULL
           ORDER BY job_id, created_at DESC
         ) latest_unpinned
         ORDER BY created_at DESC
         LIMIT ${MAX_HISTORY}
       )`
  );
}

/** Bump unpinned “opened this job” row only; never touches pinned rows. */
async function bumpUnpinnedVisit(jobId: string) {
  await execute('DELETE FROM tbl_inspect_history WHERE job_id = $1 AND note IS NULL', [jobId]);
  await execute('INSERT INTO tbl_inspect_history (job_id, note) VALUES ($1, NULL)', [jobId]);
  await pruneUnpinnedHistory();
}

/** GET: All pin rows (append-only; same job may appear multiple times) plus last 10 unpinned jobs (one row per job_id). */
export async function GET() {
  try {
    const pinned = await query<InspectHistoryRow>(
      `SELECT ${rowSelect}
       FROM tbl_inspect_history h
       LEFT JOIN tbl_vworkjobs v ON v.job_id::text = h.job_id
       WHERE h.note IS NOT NULL
       ORDER BY h.created_at DESC`
    );
    const recent = await query<InspectHistoryRow>(
      `WITH latest_unpinned AS (
         SELECT DISTINCT ON (h.job_id)
                h.id,
                h.job_id,
                v.delivery_winery,
                v.vineyard_name,
                v.worker,
                to_char(v.actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
                v.truck_id::text AS truck_id,
                h.note,
                h.created_at
         FROM tbl_inspect_history h
         LEFT JOIN tbl_vworkjobs v ON v.job_id::text = h.job_id
         WHERE h.note IS NULL
         ORDER BY h.job_id, h.created_at DESC
       )
       SELECT id, job_id, delivery_winery, vineyard_name, worker, actual_start_time, truck_id, note
       FROM latest_unpinned
       ORDER BY created_at DESC
       LIMIT ${MAX_HISTORY}`
    );
    /** @deprecated combined list; prefer `pinned` + `recent` */
    const entries = [...pinned, ...recent];
    return NextResponse.json({ pinned, recent, entries });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: (1) Visit: `{ job_id }` — bump unpinned recent only, leaves all pins. (2) Pin: `{ job_id, note }` — append one pin row (never replaces other pins). (3) Unpin: `{ job_id, note: null }` — remove all pin rows for job, bump unpinned once. */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const jobId = typeof body?.job_id === 'string' ? body.job_id.trim() : '';
    if (!jobId) {
      return NextResponse.json({ error: 'job_id required' }, { status: 400 });
    }
    const hasNoteKey = body != null && typeof body === 'object' && Object.prototype.hasOwnProperty.call(body, 'note');
    if (!hasNoteKey) {
      await bumpUnpinnedVisit(jobId);
      return NextResponse.json({ ok: true });
    }
    const rawNote = (body as { note?: unknown }).note;
    const noteToStore = rawNote == null ? null : typeof rawNote === 'string' ? rawNote.trim() : '';
    if (noteToStore != null && noteToStore.length > NOTE_MAX_LEN) {
      return NextResponse.json({ error: `note max ${NOTE_MAX_LEN} characters` }, { status: 400 });
    }
    if (noteToStore == null) {
      await execute('DELETE FROM tbl_inspect_history WHERE job_id = $1 AND note IS NOT NULL', [jobId]);
      await bumpUnpinnedVisit(jobId);
      return NextResponse.json({ ok: true });
    }
    await execute('INSERT INTO tbl_inspect_history (job_id, note) VALUES ($1, $2)', [jobId, noteToStore]);
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
