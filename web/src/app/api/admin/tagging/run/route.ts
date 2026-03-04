import { NextResponse } from 'next/server';
import { runForDate, type ProgressEvent } from '@/lib/entryexit';

function parseBody(body: unknown): {
  dateX: string;
  deviceNames: string[];
  graceSeconds: number;
} | null {
  if (!body || typeof body !== 'object') return null;
  const b = body as Record<string, unknown>;
  const dateX = typeof b.dateX === 'string' ? b.dateX.trim() : null;
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
  if (!dateX || !/^\d{4}-\d{2}-\d{2}$/.test(dateX) || !deviceNames?.length) return null;
  return { dateX, deviceNames, graceSeconds };
}

export async function POST(request: Request) {
  let parsed: ReturnType<typeof parseBody>;
  try {
    const body = await request.json();
    parsed = parseBody(body);
  } catch {
    return NextResponse.json(
      { error: 'Invalid JSON body. Required: { dateX: "YYYY-MM-DD", deviceNames: string[], graceSeconds?: number }' },
      { status: 400 }
    );
  }
  if (!parsed) {
    return NextResponse.json(
      { error: 'Missing or invalid dateX (YYYY-MM-DD) or deviceNames (non-empty array)' },
      { status: 400 }
    );
  }

  const { dateX, deviceNames, graceSeconds } = parsed;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        await runForDate({
          dateX,
          deviceNames,
          graceSeconds,
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
