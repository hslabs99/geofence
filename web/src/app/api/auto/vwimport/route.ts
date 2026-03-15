import { NextResponse } from 'next/server';
import { logAuto } from '@/lib/auto-log';

const base = process.env.IMPORT_SERVICE_URL ?? 'http://localhost:8080';
const driveFolderId = process.env.DRIVE_FOLDER_ID ?? '1EcWF7MEx6hi3unN4TTh9z7PuUyiZTgZD';
const ENDPOINT = 'vwimport';

export async function GET() {
  try {
    await logAuto(ENDPOINT, 'start', { driveFolderId, importServiceUrl: base });

    const res = await fetch(`${base}/import/vwork`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ driveFolderId }),
    });

    let data: { error?: string; ok?: boolean; results?: unknown[] };
    try {
      data = (await res.json()) as { error?: string; ok?: boolean; results?: unknown[] };
    } catch {
      const text = await res.text();
      data = { error: `Import service returned non-JSON (${res.status}): ${text.slice(0, 300)}` };
    }

    if (!res.ok) {
      await logAuto(ENDPOINT, 'error', { status: res.status, ...data });
      return NextResponse.json(
        { ok: false, error: data.error ?? `Import service returned ${res.status}` },
        { status: res.status }
      );
    }

    await logAuto(ENDPOINT, 'done', {
      ok: true,
      resultCount: Array.isArray(data.results) ? data.results.length : 0,
      results: data.results,
    });
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logAuto(ENDPOINT, 'error', { message });
    console.error('[auto/vwimport]', message);
    return NextResponse.json(
      { ok: false, error: message },
      { status: 500 }
    );
  }
}

export async function POST() {
  return GET();
}
