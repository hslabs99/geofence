import { NextResponse } from 'next/server';
import { runForDate, type ProgressEvent } from '@/lib/entryexit';

const DEFAULT_BUFFER_HOURS = 1;

function parseBody(body: unknown): {
  dateFrom: string;
  dateTo: string;
  deviceNames: string[];
  graceSeconds: number;
  bufferHours: number;
} | null {
  if (!body || typeof body !== 'object') return null;
  const b = body as Record<string, unknown>;
  const deviceNames = Array.isArray(b.deviceNames)
    ? (b.deviceNames as unknown[]).map((d) => (typeof d === 'string' ? d.trim() : '')).filter(Boolean)
    : null;
  let graceSeconds = 300;
  if (typeof b.graceSeconds === 'number' && !Number.isNaN(b.graceSeconds)) {
    graceSeconds = Math.max(0, Math.min(86400, b.graceSeconds));
  } else if (typeof b.graceSeconds === 'string') {
    const n = parseInt(b.graceSeconds, 10);
    if (!Number.isNaN(n)) graceSeconds = Math.max(0, Math.min(86400, n));
  }
  let bufferHours = DEFAULT_BUFFER_HOURS;
  if (typeof b.bufferHours === 'number' && !Number.isNaN(b.bufferHours)) {
    bufferHours = Math.max(0, Math.min(24, b.bufferHours));
  } else if (typeof b.bufferHours === 'string') {
    const n = parseInt(b.bufferHours, 10);
    if (!Number.isNaN(n)) bufferHours = Math.max(0, Math.min(24, n));
  }

  const dateX = typeof b.dateX === 'string' ? b.dateX.trim() : null;
  const dateFrom = typeof b.dateFrom === 'string' ? b.dateFrom.trim() : dateX;
  const dateTo = typeof b.dateTo === 'string' ? b.dateTo.trim() : dateX;

  const re = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateFrom || !re.test(dateFrom) || !dateTo || !re.test(dateTo) || !deviceNames?.length) return null;
  if (new Date(dateFrom + 'T00:00:00Z').getTime() > new Date(dateTo + 'T00:00:00Z').getTime()) return null;

  return { dateFrom, dateTo, deviceNames, graceSeconds, bufferHours };
}

export async function POST(request: Request) {
  let parsed: ReturnType<typeof parseBody>;
  try {
    const body = await request.json();
    parsed = parseBody(body);
  } catch {
    return NextResponse.json(
      {
        error:
          'Invalid JSON body. Required: { dateFrom, dateTo } or { dateX } (YYYY-MM-DD), deviceNames: string[], graceSeconds?: number, bufferHours?: number }',
      },
      { status: 400 }
    );
  }
  if (!parsed) {
    return NextResponse.json(
      {
        error:
          'Missing or invalid dateFrom/dateTo (or dateX) (YYYY-MM-DD), or deviceNames (non-empty array), or dateFrom > dateTo',
      },
      { status: 400 }
    );
  }

  const { dateFrom, dateTo, deviceNames, graceSeconds, bufferHours } = parsed;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        await runForDate({
          dateFrom,
          dateTo,
          deviceNames,
          graceSeconds,
          bufferHours,
          onProgress(ev: ProgressEvent) {
            controller.enqueue(encoder.encode(JSON.stringify(ev) + '\n'));
          },
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        controller.enqueue(
          encoder.encode(JSON.stringify({ stage: 'error', message }) + '\n')
        );
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Cache-Control': 'no-store',
    },
  });
}
