import { NextResponse } from 'next/server';
import { logAuto, getAutoThreeDayRangeUTC } from '@/lib/auto-log';

const ENDPOINT = 'fencetags';

function getBaseUrl(): string {
  const u = process.env.APP_URL ?? process.env.VERCEL_URL;
  if (u) return u.startsWith('http') ? u : `https://${u}`;
  return 'http://localhost:3000';
}

export async function GET() {
  try {
    const { dateFrom, dateTo, dates } = getAutoThreeDayRangeUTC();
    await logAuto(ENDPOINT, 'start', { dateFrom, dateTo, dates, scope: 'only unattempted' });

    const base = getBaseUrl();
    const res = await fetch(`${base}/api/admin/tracking/store-fences-for-date-range`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        dateFrom,
        dateTo,
        forceUpdate: false,
        reprocessMissedOnly: false,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      let errData: unknown = text;
      try {
        errData = JSON.parse(text);
      } catch {
        // use text
      }
      await logAuto(ENDPOINT, 'error', { status: res.status, body: errData });
      return NextResponse.json(
        { ok: false, error: typeof errData === 'object' && errData && 'error' in errData ? (errData as { error: string }).error : text },
        { status: res.status }
      );
    }

    if (!res.body) {
      await logAuto(ENDPOINT, 'done', { message: 'No response body' });
      return NextResponse.json({ ok: true, message: 'No response body' });
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let totalUpdated = 0;
    const dayUpdates: { date: string; updated: number }[] = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const ev = JSON.parse(line) as { type: string; date?: string; updated?: number; totalUpdated?: number; message?: string };
          if (ev.type === 'position_time_nz') {
            await logAuto(ENDPOINT, 'position_time_nz', { updated: ev.updated ?? 0 });
          } else if (ev.type === 'day' && ev.date) {
            dayUpdates.push({ date: ev.date, updated: ev.updated ?? 0 });
          } else if (ev.type === 'done') {
            totalUpdated = ev.totalUpdated ?? 0;
          } else if (ev.type === 'error') {
            await logAuto(ENDPOINT, 'error', { message: ev.message });
          }
        } catch {
          // skip malformed line
        }
      }
    }

    if (buffer.trim()) {
      try {
        const ev = JSON.parse(buffer) as { type: string; date?: string; updated?: number; totalUpdated?: number };
        if (ev.type === 'day' && ev.date) dayUpdates.push({ date: ev.date, updated: ev.updated ?? 0 });
        else if (ev.type === 'done') totalUpdated = ev.totalUpdated ?? 0;
      } catch {
        // ignore
      }
    }

    await logAuto(ENDPOINT, 'done', { totalUpdated, dayUpdates });
    return NextResponse.json({ ok: true, dateFrom, dateTo, totalUpdated, dayUpdates });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logAuto(ENDPOINT, 'error', { message });
    console.error('[auto/fencetags]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST() {
  return GET();
}
