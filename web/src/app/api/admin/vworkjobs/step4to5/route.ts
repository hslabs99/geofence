import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';
import {
  SYNTH_STEP4_AT_BEFORE_STEP5,
  eligiblePredicateNoAliasNormal,
  buildUpdateSqlNormalWhere,
  executeNormalStep4to5Update,
} from '@/lib/step4to5-fix';

/** Substitute $n from highest index first so $10 is not broken by replacing $1. */
function interpolateSqlPlaceholders(sql: string, values: unknown[]): string {
  let out = sql;
  for (let i = values.length; i >= 1; i--) {
    const v = values[i - 1];
    const rep =
      v === null || v === undefined
        ? 'NULL'
        : typeof v === 'string'
          ? `'${String(v).replace(/'/g, "''")}'`
          : typeof v === 'number' || typeof v === 'bigint'
            ? String(v)
            : `'${String(v).replace(/'/g, "''")}'`;
    out = out.split(`$${i}`).join(rep);
  }
  return out;
}

const MATCH_T = `trim(t.customer) = $1 AND trim(t.template) = $2`;
const MATCH_T_INLINE = MATCH_T.replace(/\s+/g, ' ').trim();
const MATCH_NO_ALIAS = `trim(customer) = $1 AND trim(template) = $2`;
const MATCH_NO_ALIAS_INLINE = MATCH_NO_ALIAS.replace(/\s+/g, ' ').trim();

const T_STEP4_JOB_COMPLETED = `trim(COALESCE(t.step_4_name, '')) = 'Job Completed'`;

const T_STEP5_NOT_JOB_COMPLETED = `(t.step_5_name IS NULL OR trim(COALESCE(t.step_5_name, '')) <> 'Job Completed')`;

const T_STEP5_JOB_COMPLETED = `trim(COALESCE(t.step_5_name, '')) = 'Job Completed'`;
const T_STEP5_COMPLETED_AT_EMPTY = `t.step_5_completed_at IS NULL`;

const T_STEP4_ARRIVE_WINERY = `trim(COALESCE(t.step_4_name, '')) = 'Arrive Winery'`;

/** Done row where step 4 is not strictly before step 5 (includes null step_4 when step_5 is set). */
const T_ORDERING_BAD = `(t.step_5_completed_at IS NOT NULL AND (t.step_4_completed_at IS NULL OR t.step_4_completed_at >= t.step_5_completed_at))`;

/** Done row with valid ordering (step 4 strictly before step 5). */
const T_ORDERING_OK = `(t.step_4_completed_at IS NOT NULL AND t.step_5_completed_at IS NOT NULL AND t.step_4_completed_at < t.step_5_completed_at)`;

function potentialWhereTInlineNormal(): string {
  return `${MATCH_T_INLINE} AND COALESCE(t.step4to5, 0) = 0`;
}

/** step4to5=0 but cannot run normal Fix (wrong step 4, step 5 name/time already set — never push 4→5 if step_5_completed_at has data). */
function blockedWhereTInlineNormal(): string {
  return `${potentialWhereTInlineNormal()} AND (NOT (${T_STEP4_JOB_COMPLETED}) OR ${T_STEP5_JOB_COMPLETED} OR t.step_5_completed_at IS NOT NULL)`;
}

function eligibleWhereTNormal(): string {
  return `${MATCH_T}
  AND COALESCE(t.step4to5, 0) = 0
  AND ${T_STEP4_JOB_COMPLETED}
  AND ${T_STEP5_NOT_JOB_COMPLETED}
  AND ${T_STEP5_COMPLETED_AT_EMPTY}
  AND t.step_4_completed_at IS NOT NULL`;
}

function eligibleWhereTInlineNormal(): string {
  return eligibleWhereTNormal().replace(/\s+/g, ' ').trim();
}

function eligibleWhereNoAliasInlineNormal(): string {
  return `${MATCH_NO_ALIAS_INLINE} AND ${eligiblePredicateNoAliasNormal()}`.replace(/\s+/g, ' ').trim();
}

/** Rerun: "done" rows — only recalc synthetic step 4 time. */
function potentialWhereTInlineRerun(): string {
  return `${MATCH_T_INLINE} AND COALESCE(t.step4to5, 0) = 1 AND ${T_STEP4_ARRIVE_WINERY} AND ${T_STEP5_JOB_COMPLETED}`;
}

/** step4to5=1 but not Arrive Winery + Job Completed on 5. */
function blockedWhereTInlineRerun(): string {
  return `${MATCH_T_INLINE} AND COALESCE(t.step4to5, 0) = 1 AND NOT (${T_STEP4_ARRIVE_WINERY} AND ${T_STEP5_JOB_COMPLETED})`;
}

function eligibleWhereTRerun(): string {
  return `${MATCH_T}
  AND COALESCE(t.step4to5, 0) = 1
  AND ${T_STEP4_ARRIVE_WINERY}
  AND ${T_STEP5_JOB_COMPLETED}`;
}

function eligibleWhereTInlineRerun(): string {
  return eligibleWhereTRerun().replace(/\s+/g, ' ').trim();
}

function eligibleWhereNoAliasInlineRerun(): string {
  return `${MATCH_NO_ALIAS_INLINE} AND COALESCE(step4to5, 0) = 1 AND trim(COALESCE(step_4_name, '')) = 'Arrive Winery' AND trim(COALESCE(step_5_name, '')) = 'Job Completed'`;
}

/** Order fix: same done pool as rerun; blocked = already step_4 &lt; step_5. */
function blockedWhereTInlineOrdering(): string {
  return `${potentialWhereTInlineRerun()} AND ${T_ORDERING_OK}`;
}

function eligibleWhereTOrdering(): string {
  return `${eligibleWhereTRerun()} AND ${T_ORDERING_BAD}`;
}

function eligibleWhereTInlineOrdering(): string {
  return eligibleWhereTOrdering().replace(/\s+/g, ' ').trim();
}

function eligibleWhereNoAliasInlineOrdering(): string {
  return `${eligibleWhereNoAliasInlineRerun()} AND step_5_completed_at IS NOT NULL AND (step_4_completed_at IS NULL OR step_4_completed_at >= step_5_completed_at)`;
}

function buildUpdateSqlNormal(): string {
  return buildUpdateSqlNormalWhere(eligibleWhereNoAliasInlineNormal());
}

function buildUpdateSqlRerunOrOrdering(): string {
  const where = eligibleWhereNoAliasInlineRerun();
  return `UPDATE tbl_vworkjobs SET
  step_4_completed_at = ${SYNTH_STEP4_AT_BEFORE_STEP5}
WHERE ${where}`;
}

function buildUpdateSqlOrderingOnly(): string {
  const where = eligibleWhereNoAliasInlineOrdering();
  return `UPDATE tbl_vworkjobs SET
  step_4_completed_at = ${SYNTH_STEP4_AT_BEFORE_STEP5}
WHERE ${where}`;
}

function buildUpdateSql(rerun: boolean, orderingFix: boolean): string {
  if (orderingFix) return buildUpdateSqlOrderingOnly();
  if (rerun) return buildUpdateSqlRerunOrOrdering();
  return buildUpdateSqlNormal();
}

const VALUES_TWO = (customer: string, template: string): [string, string] => [customer, template];

function parseRerun(searchParams: URLSearchParams): boolean {
  const v = searchParams.get('rerun')?.trim().toLowerCase() ?? '';
  return v === '1' || v === 'true' || v === 'yes';
}

function parseOrderingFix(searchParams: URLSearchParams): boolean {
  const v = searchParams.get('orderingFix')?.trim().toLowerCase() ?? '';
  return v === '1' || v === 'true' || v === 'yes';
}

function parseCustomerTemplateFromUrl(
  request: Request
): { customer: string; template: string; rerun: boolean; orderingFix: boolean } | { error: string } {
  const url = new URL(request.url);
  const customer = url.searchParams.get('customer')?.trim() ?? '';
  const template = url.searchParams.get('template')?.trim() ?? '';
  if (!customer) return { error: 'customer is required' };
  if (!template) return { error: 'template is required' };
  const orderingFix = parseOrderingFix(url.searchParams);
  const rerun = !orderingFix && parseRerun(url.searchParams);
  return { customer, template, rerun, orderingFix };
}

async function runPreview(customer: string, template: string, rerun: boolean, orderingFix: boolean) {
  const vals = VALUES_TWO(customer, template);

  let potInline: string;
  let blkInline: string;
  let eligInline: string;
  let eligibleWhereMultiline: string;
  let selectListSql: string;

  if (orderingFix) {
    potInline = potentialWhereTInlineRerun();
    blkInline = blockedWhereTInlineOrdering();
    eligInline = eligibleWhereTInlineOrdering();
    eligibleWhereMultiline = eligibleWhereTOrdering();
    selectListSql = `SELECT t.job_id, t.customer, t.template, t.step_4_completed_at, t.step_5_completed_at, t.step_4_name, t.step_5_name, t.step4to5
FROM tbl_vworkjobs t
WHERE ${eligibleWhereMultiline}
ORDER BY t.job_id
LIMIT 200`;
  } else if (rerun) {
    potInline = potentialWhereTInlineRerun();
    blkInline = blockedWhereTInlineRerun();
    eligInline = eligibleWhereTInlineRerun();
    eligibleWhereMultiline = eligibleWhereTRerun();
    selectListSql = `SELECT t.job_id, t.customer, t.template, t.step_4_completed_at, t.step_4_name, t.step_5_name, t.step_5_completed_at, t.step4to5
FROM tbl_vworkjobs t
WHERE ${eligibleWhereMultiline}
ORDER BY t.job_id
LIMIT 200`;
  } else {
    potInline = potentialWhereTInlineNormal();
    blkInline = blockedWhereTInlineNormal();
    eligInline = eligibleWhereTInlineNormal();
    eligibleWhereMultiline = eligibleWhereTNormal();
    selectListSql = `SELECT t.job_id, t.customer, t.template, t.step_4_completed_at, t.step_4_name, t.step_5_name, t.step_5_completed_at, t.step4to5
FROM tbl_vworkjobs t
WHERE ${eligibleWhereMultiline}
ORDER BY t.job_id
LIMIT 200`;
  }

  const countPotentialSql = `SELECT COUNT(*)::int AS cnt FROM tbl_vworkjobs t WHERE ${potInline}`;
  const countBlockedSql = `SELECT COUNT(*)::int AS cnt FROM tbl_vworkjobs t WHERE ${blkInline}`;
  const countEligibleSql = `SELECT COUNT(*)::int AS cnt FROM tbl_vworkjobs t WHERE ${eligInline}`;

  const updateSql = buildUpdateSql(rerun, orderingFix);

  const [potentialRows, blockedRows, eligibleRows] = await Promise.all([
    query<{ cnt: number }>(countPotentialSql, vals),
    query<{ cnt: number }>(countBlockedSql, vals),
    query<{ cnt: number }>(countEligibleSql, vals),
  ]);

  const potentialCount = Number(potentialRows[0]?.cnt ?? 0);
  const blockedCount = Number(blockedRows[0]?.cnt ?? 0);
  const todoCount = Number(eligibleRows[0]?.cnt ?? 0);
  const willDoCount =
    rerun || orderingFix ? todoCount : Math.max(0, potentialCount - blockedCount);

  const updateSqlLiteral = interpolateSqlPlaceholders(updateSql, vals);
  const selectSqlLiteral = interpolateSqlPlaceholders(selectListSql.trim(), vals);

  return {
    ok: true as const,
    rerun,
    orderingFix,
    customer,
    template,
    potentialCount,
    blockedCount,
    willDoCount,
    todoCount,
    selectSql: selectListSql,
    selectSqlParams: vals,
    selectSqlLiteral,
    updateSql,
    updateSqlParams: vals,
    updateSqlLiteral,
    countPotentialSql,
    countPotentialSqlLiteral: interpolateSqlPlaceholders(countPotentialSql, vals),
    countBlockedSql,
    countBlockedSqlLiteral: interpolateSqlPlaceholders(countBlockedSql, vals),
    countEligibleSql,
    countEligibleSqlLiteral: interpolateSqlPlaceholders(countEligibleSql, vals),
  };
}

/**
 * GET ?customer=&template=&rerun=1&orderingFix=1
 * Normal: step4to5=0, step_4_name Job Completed, step_5_name not Job Completed → full migrate.
 * Rerun: step4to5=1, step_4 Arrive Winery, step_5 Job Completed → only step_4_completed_at recalc (VWork synthetic vs step_5_completed_at).
 * Ordering fix: same UPDATE as rerun, only rows where step_4 is null or not strictly before step_5_completed_at.
 */
export async function GET(request: Request) {
  try {
    const parsed = parseCustomerTemplateFromUrl(request);
    if ('error' in parsed) {
      return NextResponse.json({ ok: false, error: parsed.error }, { status: 400 });
    }
    const body = await runPreview(parsed.customer, parsed.template, parsed.rerun, parsed.orderingFix);
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/vworkjobs/step4to5 GET]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

/**
 * POST JSON { customer, template, rerun?: boolean, orderingFix?: boolean }
 */
export async function POST(request: Request) {
  try {
    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return NextResponse.json({ ok: false, error: 'Invalid JSON body' }, { status: 400 });
    }
    const o = body != null && typeof body === 'object' ? (body as Record<string, unknown>) : {};
    const customer = String(o.customer ?? '').trim();
    const template = String(o.template ?? '').trim();
    const orderingFix =
      o.orderingFix === true ||
      o.orderingFix === 1 ||
      String(o.orderingFix ?? '').toLowerCase() === 'true';
    const rerun =
      !orderingFix &&
      (o.rerun === true || o.rerun === 1 || String(o.rerun ?? '').toLowerCase() === 'true');

    if (!customer) return NextResponse.json({ ok: false, error: 'customer is required' }, { status: 400 });
    if (!template) return NextResponse.json({ ok: false, error: 'template is required' }, { status: 400 });

    const updateSql = buildUpdateSql(rerun, orderingFix);

    let updated: number;
    if (orderingFix) {
      updated = await execute(updateSql, VALUES_TWO(customer, template));
    } else if (rerun) {
      updated = await execute(updateSql, VALUES_TWO(customer, template));
    } else {
      updated = await executeNormalStep4to5Update(updateSql, VALUES_TWO(customer, template));
    }

    const preview = await runPreview(customer, template, rerun, orderingFix);

    return NextResponse.json({
      ok: true,
      updated,
      customer,
      template,
      rerun,
      orderingFix,
      afterPreview: preview,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/vworkjobs/step4to5 POST]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
