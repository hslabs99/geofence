import { NextResponse } from 'next/server';

const DEFAULT_FOLDER = '1EcWF7MEx6hi3unN4TTh9z7PuUyiZTgZD';
const SAMPLE_MAX_ROWS = 15_000;

function importHostOnly(base: string): string {
  try {
    return new URL(base).host;
  } catch {
    return '(invalid IMPORT_SERVICE_URL)';
  }
}

type SampleOk = {
  ok: true;
  driveFolderId: string;
  filesTotal: number;
  filesTouched: number;
  rowsScanned: number;
  rowsWithCustomerText: number;
  indevinMatchCount: number;
  firstMatchCustomerPreview: string | null;
  maxRowsBudget: number;
  skippedNoFiles: boolean;
};

type CustomerScanResult = {
  ok: boolean;
  skippedNoFiles?: boolean;
  notRun?: boolean;
  notRunReason?: string;
  error?: string;
  rowsScanned?: number;
  filesTouched?: number;
  rowsWithCustomerText?: number;
  indevinMatchCount?: number;
  firstMatchCustomerPreview?: string | null;
  maxRowsBudget?: number;
  failedResolver?: boolean;
  failedIndevinSubstring?: boolean;
  warning?: string;
};

function evaluateCustomerScan(data: SampleOk): CustomerScanResult {
  if (data.skippedNoFiles) {
    return {
      ok: true,
      skippedNoFiles: true,
      rowsScanned: 0,
      indevinMatchCount: 0,
      warning: 'No export CSVs in folder — Customer / Indevin check skipped.',
    };
  }

  const failedResolver = data.rowsScanned >= 80 && data.rowsWithCustomerText === 0;
  if (failedResolver) {
    return {
      ok: false,
      failedResolver: true,
      rowsScanned: data.rowsScanned,
      filesTouched: data.filesTouched,
      rowsWithCustomerText: data.rowsWithCustomerText,
      indevinMatchCount: data.indevinMatchCount,
      error:
        'Scanned many rows but never resolved Customer / Client name (same logic as backfill). Check CSV headers and tbl_mappings.',
    };
  }

  const failedIndevin =
    data.rowsScanned >= 500 &&
    data.rowsWithCustomerText > 0 &&
    data.indevinMatchCount === 0;

  if (failedIndevin) {
    return {
      ok: false,
      failedIndevinSubstring: true,
      rowsScanned: data.rowsScanned,
      filesTouched: data.filesTouched,
      rowsWithCustomerText: data.rowsWithCustomerText,
      indevinMatchCount: 0,
      maxRowsBudget: data.maxRowsBudget,
      error: `No row contained "indevin" in Customer (case-insensitive) after scanning ${data.rowsScanned.toLocaleString()} row(s). Backfill would skip all Indevin rows.`,
    };
  }

  let warning: string | undefined;
  if (data.indevinMatchCount === 0 && data.rowsScanned > 0 && data.rowsScanned < 500) {
    warning =
      'Fewer than 500 rows in sample; no Indevin match yet — inconclusive if your folder is small or Indevin only appears in later files.';
  }

  return {
    ok: true,
    rowsScanned: data.rowsScanned,
    filesTouched: data.filesTouched,
    rowsWithCustomerText: data.rowsWithCustomerText,
    indevinMatchCount: data.indevinMatchCount,
    firstMatchCustomerPreview: data.firstMatchCustomerPreview,
    maxRowsBudget: data.maxRowsBudget,
    warning,
  };
}

/**
 * GET: Smoke test — health, Drive list, then sample parse for Customer + substring "indevin" (same as backfill).
 */
export async function GET() {
  const baseRaw = process.env.IMPORT_SERVICE_URL ?? 'http://localhost:8080';
  const base = baseRaw.replace(/\/$/, '');
  const driveFolderId = process.env.DRIVE_FOLDER_ID ?? DEFAULT_FOLDER;

  const health: {
    ok: boolean;
    status?: number;
    error?: string;
  } = { ok: false };

  try {
    const r = await fetch(`${base}/health`, {
      method: 'GET',
      cache: 'no-store',
      signal: AbortSignal.timeout(15_000),
    });
    let bodyOk = false;
    try {
      const j = (await r.json()) as { ok?: boolean };
      bodyOk = j?.ok === true;
    } catch {
      /* ignore */
    }
    health.ok = r.ok && bodyOk;
    health.status = r.status;
    if (!health.ok) {
      health.error = `HTTP ${r.status}, body ok=${bodyOk}`;
    }
  } catch (err) {
    health.error = err instanceof Error ? err.message : String(err);
  }

  const driveList: {
    ok: boolean;
    status?: number;
    filesTotal?: number;
    sampleNames?: string[];
    driveFolderId?: string;
    error?: string;
  } = { ok: false, driveFolderId };

  try {
    const r = await fetch(`${base}/import/vwork-backfill-loadsize`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'list', driveFolderId }),
      cache: 'no-store',
      signal: AbortSignal.timeout(120_000),
    });
    driveList.status = r.status;
    const data = (await r.json()) as {
      ok?: boolean;
      filesTotal?: number;
      files?: { name?: string }[];
      error?: string;
    };
    if (!r.ok || data.error) {
      driveList.error = data.error ?? `HTTP ${r.status}`;
    } else if (data.ok === true && typeof data.filesTotal === 'number') {
      driveList.ok = true;
      driveList.filesTotal = data.filesTotal;
      driveList.sampleNames = (data.files ?? [])
        .map((f) => (typeof f.name === 'string' ? f.name : ''))
        .filter(Boolean)
        .slice(0, 8);
    } else {
      driveList.error = 'Unexpected list response shape';
    }
  } catch (err) {
    driveList.error = err instanceof Error ? err.message : String(err);
  }

  let customerScan: CustomerScanResult = {
    ok: false,
    notRun: true,
    notRunReason: 'Prerequisites not met',
  };

  if (!health.ok) {
    customerScan = {
      ok: false,
      notRun: true,
      notRunReason: 'Import service health check failed',
    };
  } else if (!driveList.ok) {
    customerScan = {
      ok: false,
      notRun: true,
      notRunReason: driveList.error ?? 'Drive list failed',
    };
  } else if ((driveList.filesTotal ?? 0) === 0) {
    customerScan = {
      ok: true,
      skippedNoFiles: true,
      rowsScanned: 0,
      indevinMatchCount: 0,
      warning: 'No export files — Customer / Indevin scan skipped.',
    };
  } else {
    try {
      const r = await fetch(`${base}/import/vwork-backfill-loadsize`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'sample',
          driveFolderId,
          maxRows: SAMPLE_MAX_ROWS,
        }),
        cache: 'no-store',
        signal: AbortSignal.timeout(300_000),
      });
      const raw = (await r.json()) as SampleOk & { error?: string };
      if (!r.ok || raw.error) {
        customerScan = {
          ok: false,
          error: raw.error ?? `Sample scan HTTP ${r.status}`,
        };
      } else if (raw.ok === true) {
        customerScan = evaluateCustomerScan(raw as SampleOk);
      } else {
        customerScan = { ok: false, error: 'Unexpected sample response' };
      }
    } catch (err) {
      customerScan = {
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  const ok = health.ok && driveList.ok && customerScan.ok;

  let hint: string;
  if (!health.ok) {
    hint = 'Fix import service reachability /health first.';
  } else if (!driveList.ok) {
    hint = 'Fix Drive list (folder id, permissions) before running backfill.';
  } else if (!customerScan.ok) {
    hint = customerScan.error ?? 'Customer / Indevin sample check failed — do not run full backfill until this passes.';
  } else if (customerScan.warning) {
    hint = `${customerScan.warning} Otherwise you can run the backfill.`;
  } else {
    hint = 'Health, Drive list, and Indevin-in-Customer sample check passed. Safe to run full backfill.';
  }

  return NextResponse.json({
    ok,
    importHost: importHostOnly(base),
    health,
    driveList,
    customerScan,
    hint,
  });
}
