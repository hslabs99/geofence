import { NextResponse } from 'next/server';

const DEFAULT_FOLDER = '1EcWF7MEx6hi3unN4TTh9z7PuUyiZTgZD';

type ListOk = {
  ok: true;
  driveFolderId: string;
  filesTotal: number;
  files: { id: string; name: string }[];
};

type FileOk = {
  ok: true;
  driveFolderId: string;
  fileIndex: number;
  filesTotal: number;
  fileName: string;
  driveFileId: string;
  rowsInFile: number;
  rowsIndevinCustomer: number;
  rowsUpdated: number;
  rowsSkippedNoJobId: number;
  rowsSkippedNoLoadSize: number;
  rowsNotFoundInDb: number;
  errors: string[];
  errorsTruncated: boolean;
  done: boolean;
};

type ErrBody = { error: string };

/**
 * POST JSON body forwarded to import service:
 * { action: "list", driveFolderId?: string }
 * { action: "file", fileIndex: number, driveFolderId?: string }
 * { action: "sample", driveFolderId?: string, maxRows?: number }
 */
export async function POST(request: Request) {
  const base = process.env.IMPORT_SERVICE_URL ?? 'http://localhost:8080';
  const defaultFolder = process.env.DRIVE_FOLDER_ID ?? DEFAULT_FOLDER;

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' } satisfies ErrBody, { status: 400 });
  }

  const o = body != null && typeof body === 'object' ? (body as Record<string, unknown>) : {};
  const action =
    o.action === 'list' || o.action === 'file' || o.action === 'sample' ? o.action : null;
  if (!action) {
    return NextResponse.json(
      { error: 'action must be "list", "file", or "sample"' } satisfies ErrBody,
      { status: 400 }
    );
  }

  const driveFolderId =
    typeof o.driveFolderId === 'string' && o.driveFolderId.trim() ? o.driveFolderId.trim() : defaultFolder;

  let payload: { action: string; driveFolderId: string; fileIndex?: number; maxRows?: number };
  if (action === 'list') {
    payload = { action: 'list', driveFolderId };
  } else if (action === 'file') {
    const fileIndex = Number(o.fileIndex);
    if (!Number.isInteger(fileIndex)) {
      return NextResponse.json({ error: 'fileIndex must be an integer' } satisfies ErrBody, { status: 400 });
    }
    payload = { action: 'file', driveFolderId, fileIndex };
  } else {
    const maxRows = Number(o.maxRows);
    payload = {
      action: 'sample',
      driveFolderId,
      ...(Number.isFinite(maxRows) ? { maxRows: Math.floor(maxRows) } : {}),
    };
  }

  try {
    const res = await fetch(`${base}/import/vwork-backfill-loadsize`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    let data: unknown;
    try {
      data = await res.json();
    } catch {
      const text = await res.text();
      return NextResponse.json(
        { error: `Import service returned non-JSON (${res.status}): ${text.slice(0, 200)}` } satisfies ErrBody,
        { status: 502 }
      );
    }

    if (!res.ok) {
      const err = (data as ErrBody)?.error ?? `Import service returned ${res.status}`;
      return NextResponse.json({ error: err } satisfies ErrBody, { status: res.status >= 500 ? 502 : 400 });
    }

    if (data && typeof data === 'object' && 'error' in data && typeof (data as ErrBody).error === 'string') {
      return NextResponse.json(data as ErrBody, { status: 400 });
    }

    if (action === 'list') {
      return NextResponse.json(data as ListOk);
    }
    if (action === 'sample') {
      return NextResponse.json(data);
    }
    return NextResponse.json(data as FileOk);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json(
      { error: `Cannot reach import service at ${base}. ${msg}` } satisfies ErrBody,
      { status: 503 }
    );
  }
}
