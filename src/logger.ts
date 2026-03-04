/** Structured logs for Cloud Logging. Use level+context+message for fast debugging. */
type LogContext = Record<string, unknown>;

function tsLiteral(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function log(level: string, context: LogContext, message: string): void {
  const entry = { level, ...context, message, ts: tsLiteral() };
  const out = JSON.stringify(entry);
  if (level === 'ERROR') console.error(out);
  else console.log(out);
}

export const logger = {
  db: (ctx: LogContext, msg: string) => log('DB', { ...ctx, layer: 'db' }, msg),
  file: (ctx: LogContext, msg: string) => log('FILE', { ...ctx, layer: 'file' }, msg),
  row: (ctx: LogContext, msg: string) => log('ROW', { ...ctx, layer: 'row' }, msg),
  error: (ctx: LogContext, msg: string) => log('ERROR', { ...ctx, layer: 'error' }, msg),
};
