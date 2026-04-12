/** Human-readable row feedback from node-pg QueryResult. */
export function summarizePgResult(r: { command: string; rowCount: number | null }): string {
  const n = r.rowCount ?? 0;
  const c = (r.command || '').toUpperCase();
  if (c === 'SELECT') return `SELECT returned ${n} row(s)`;
  if (c === 'UPDATE') return `UPDATE affected ${n} row(s)`;
  if (c === 'INSERT') return `INSERT completed (${n} row(s))`;
  if (c === 'DELETE') return `DELETE affected ${n} row(s)`;
  if (c === 'COPY') return `COPY completed (${n} row(s))`;
  return `${c}${n ? ` — ${n} row(s)` : ''}`;
}
