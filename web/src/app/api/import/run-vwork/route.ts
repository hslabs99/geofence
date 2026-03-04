import { NextResponse } from 'next/server';

export async function POST() {
  const base = process.env.IMPORT_SERVICE_URL ?? 'http://localhost:8080';
  const driveFolderId = process.env.DRIVE_FOLDER_ID ?? '1EcWF7MEx6hi3unN4TTh9z7PuUyiZTgZD';

  try {
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
      data = { error: `Import service returned non-JSON (${res.status}): ${text.slice(0, 200)}` };
    }

    if (!res.ok) {
      console.error('[run-vwork] Import service error:', res.status, data);
      return NextResponse.json(
        { error: data.error ?? `Import service returned ${res.status}` },
        { status: res.status }
      );
    }
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[run-vwork] Fetch failed:', message);
    return NextResponse.json(
      { error: `Cannot reach import service at ${base}. ${message}` },
      { status: 500 }
    );
  }
}
