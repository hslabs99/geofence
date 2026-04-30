import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

const MAX_HISTORY = 10;
const NOTE_MAX_LEN = 500;

/** GET: Last 10 inspect history entries (one per job_id, most recent first) with job details from tbl_vworkjobs. */
export async function GET() {
  try {
    const rows = await query<{
      job_id: string;
      delivery_winery: string | null;
      vineyard_name: string | null;
      worker: string | null;
      actual_start_time: string | null;
      truck_id: string | null;
      note: string | null;
    }>(
      `SELECT * FROM (
         SELECT DISTINCT ON (h.job_id)
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
         ORDER BY h.job_id, h.created_at DESC
       ) t
       ORDER BY created_at DESC
       LIMIT ${MAX_HISTORY}`
    );
    return NextResponse.json({ entries: rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: Add a job to history (bump if exists), then prune to last 10. Body: { job_id: string, note?: string }. If `note` is omitted, any existing note for that job_id is kept. */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const jobId = typeof body?.job_id === 'string' ? body.job_id.trim() : '';
    if (!jobId) {
      return NextResponse.json({ error: 'job_id required' }, { status: 400 });
    }
    const hasNoteKey = body != null && typeof body === 'object' && Object.prototype.hasOwnProperty.call(body, 'note');
    let noteToStore: string | null = null;
    if (hasNoteKey) {
      const raw = typeof (body as { note?: unknown }).note === 'string' ? (body as { note: string }).note.trim() : '';
      noteToStore = raw === '' ? null : raw;
      if (noteToStore != null && noteToStore.length > NOTE_MAX_LEN) {
        return NextResponse.json({ error: `note max ${NOTE_MAX_LEN} characters` }, { status: 400 });
      }
    } else {
      const existing = await query<{ note: string | null }>(
        `SELECT note FROM tbl_inspect_history WHERE job_id = $1 ORDER BY created_at DESC LIMIT 1`,
        [jobId]
      );
      noteToStore = existing[0]?.note ?? null;
    }
    await execute('DELETE FROM tbl_inspect_history WHERE job_id = $1', [jobId]);
    await execute('INSERT INTO tbl_inspect_history (job_id, note) VALUES ($1, $2)', [jobId, noteToStore]);
    const toKeep = await query<{ id: number }>(
      `SELECT id FROM tbl_inspect_history ORDER BY created_at DESC LIMIT ${MAX_HISTORY}`
    );
    if (toKeep.length > 0) {
      const ids = toKeep.map((r) => r.id).join(',');
      await execute(`DELETE FROM tbl_inspect_history WHERE id NOT IN (${ids})`);
    }
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
