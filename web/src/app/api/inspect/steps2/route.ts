/**
 * POST /api/inspect/steps2 — **Steps+** preview (legacy path name "steps2"; not VWork steps 1–5).
 * Buffered-fence stays for device/window/fences. Read-only; does not update any table.
 * Body: { device, startTime, endTime, fenceNames: string[], bufferMeters?: number }
 * Returns: { fenceNamesIn, stays, maxTimePerFence } where stays = segments >= 300s.
 */

import { NextResponse } from 'next/server';
import { runStepsPlusQuery } from '@/lib/steps-plus-query';

/** Normalize to YYYY-MM-DD HH:mm:ss for DB (same idea as tracking API). */
function toTimestampLiteral(s: string | null): string | null {
  if (!s || typeof s !== 'string') return null;
  const t = s.trim();
  const dmy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})/);
  if (dmy) {
    const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
    return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')} ${dmy[4].padStart(2, '0')}:${dmy[5]}:${dmy[6]}`;
  }
  const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (iso) {
    const h = iso[4]?.padStart(2, '0') ?? '00';
    const m = iso[5]?.padStart(2, '0') ?? '00';
    const sec = iso[6]?.padStart(2, '0') ?? '00';
    return `${iso[1]}-${iso[2]}-${iso[3]} ${h}:${m}:${sec}`;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(t)) return t.slice(0, 19);
  return t;
}

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const device = typeof body?.device === 'string' ? body.device.trim() : '';
    const startTime = toTimestampLiteral(body?.startTime ?? null) ?? (body?.startTime ?? '').trim();
    const endTime = toTimestampLiteral(body?.endTime ?? null) ?? (body?.endTime ?? '').trim();
    const fenceNames = Array.isArray(body?.fenceNames)
      ? (body.fenceNames as string[]).map((s: unknown) => String(s).trim()).filter(Boolean)
      : [];
    const bufferMeters = typeof body?.bufferMeters === 'number' && body.bufferMeters >= 0
      ? body.bufferMeters
      : 10;

    if (!device || !startTime || !endTime) {
      return NextResponse.json(
        { error: 'device, startTime, and endTime are required' },
        { status: 400 }
      );
    }

    const rows = await runStepsPlusQuery(device, startTime, endTime, fenceNames, bufferMeters);

    const duration = (r: { duration_seconds: string | number }) => Number(r.duration_seconds);
    const stays = rows.filter((r) => duration(r) >= 300);

    const maxByFence: { fence_name: string; max_seconds: number }[] = [];
    for (const name of fenceNames) {
      const segs = rows.filter((r) => r.fence_name === name);
      const maxSec = segs.length ? Math.max(...segs.map(duration)) : 0;
      maxByFence.push({ fence_name: name, max_seconds: maxSec });
    }

    const esc = (s: string) => `'${String(s).replace(/'/g, "''")}'`;
    const fenceNamesIn =
      fenceNames.length > 0
        ? `fence_name IN (${fenceNames.map(esc).join(', ')})`
        : '';

    return NextResponse.json({
      fenceNamesIn,
      windowFrom: startTime,
      windowTo: endTime,
      stays: stays.map((r) => ({
        fence_name: r.fence_name,
        enter_time: r.enter_time,
        exit_time: r.exit_time,
        duration_seconds: duration(r),
      })),
      maxTimePerFence: maxByFence,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
