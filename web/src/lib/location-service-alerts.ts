import { execute, getClient, query } from '@/lib/db';
import { currentLocationPollStatus } from '@/lib/current-location-poll';
import { sendGridConfigured, sendPlainEmail } from '@/lib/sendgrid-mail';

const SETTINGS_TYPE = 'LocationServiceAlerts';
const LOGTYPE = 'location_service_alert';

export type LocationAlertSettings = {
  alertEmails: string;
  checkerIntervalSeconds: number;
};

const DEFAULT_SETTINGS: LocationAlertSettings = {
  alertEmails: '',
  checkerIntervalSeconds: 60,
};

type CheckerState = {
  timer: ReturnType<typeof setInterval> | null;
  running: boolean;
  settings: LocationAlertSettings;
  /** True after the checker has emailed for the current stretch of the poll being stopped. */
  outageNotified: boolean;
  lastCheckAt: string | null;
  lastResult: string | null;
  lastEmailAt: string | null;
  lastEmailError: string | null;
};

const g = globalThis as unknown as { __geodataLocationServiceChecker?: CheckerState };

function state(): CheckerState {
  if (!g.__geodataLocationServiceChecker) {
    g.__geodataLocationServiceChecker = {
      timer: null,
      running: false,
      settings: { ...DEFAULT_SETTINGS },
      outageNotified: false,
      lastCheckAt: null,
      lastResult: null,
      lastEmailAt: null,
      lastEmailError: null,
    };
  }
  return g.__geodataLocationServiceChecker;
}

function clock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

function clampInt(value: unknown, min: number, max: number, fallback: number): number {
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** Split a comma-separated list. Invalid tokens are returned so the save can be rejected. */
export function parseAlertEmails(raw: string): { emails: string[]; invalid: string[] } {
  const emails: string[] = [];
  const invalid: string[] = [];
  const seen = new Set<string>();
  for (const part of raw.split(',')) {
    const email = part.trim();
    if (!email) continue;
    if (!EMAIL_RE.test(email)) {
      invalid.push(email);
      continue;
    }
    const key = email.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    emails.push(email);
  }
  return { emails, invalid };
}

function envFallbackEmails(): string {
  return process.env.CONTACT_NOTIFY_EMAIL?.trim() || process.env.SENDGRID_TEST_RECIPIENT?.trim() || '';
}

function normalizeSettings(input?: Partial<LocationAlertSettings>): LocationAlertSettings {
  const raw = typeof input?.alertEmails === 'string' ? input.alertEmails.trim() : DEFAULT_SETTINGS.alertEmails;
  return {
    alertEmails: raw,
    checkerIntervalSeconds: clampInt(input?.checkerIntervalSeconds, 10, 3600, DEFAULT_SETTINGS.checkerIntervalSeconds),
  };
}

async function readSettings(): Promise<LocationAlertSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue
     FROM tbl_settings
     WHERE type = $1 AND settingname IN ('alertEmails', 'checkerIntervalSeconds')`,
    [SETTINGS_TYPE],
  );
  const found: Partial<LocationAlertSettings> = {};
  let emailsStored = false;
  for (const row of rows) {
    if (row.settingname === 'alertEmails') {
      found.alertEmails = row.settingvalue ?? '';
      emailsStored = true;
    }
    if (row.settingname === 'checkerIntervalSeconds') found.checkerIntervalSeconds = Number(row.settingvalue);
  }
  const settings = normalizeSettings({ ...DEFAULT_SETTINGS, ...found });
  if (!emailsStored) settings.alertEmails = envFallbackEmails();
  return settings;
}

async function writeSettings(settings: LocationAlertSettings): Promise<void> {
  const client = await getClient();
  try {
    await client.query('BEGIN');
    for (const [name, value] of Object.entries(settings)) {
      await client.query('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [SETTINGS_TYPE, name]);
      await client.query(
        'INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)',
        [SETTINGS_TYPE, name, String(value)],
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

async function writeLog(stage: string, details: Record<string, unknown>): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE, 'all', stage, JSON.stringify(details)],
    );
  } catch (err) {
    console.error('[location-service-alerts] log failed', err);
  }
}

async function deliver(stage: string, subject: string, text: string): Promise<{ ok: boolean; error?: string }> {
  const s = state();
  const { emails, invalid } = parseAlertEmails(s.settings.alertEmails);
  if (invalid.length > 0 || emails.length === 0) {
    const error = emails.length === 0 ? 'No alert email addresses' : `Invalid email: ${invalid.join(', ')}`;
    s.lastEmailError = error;
    await writeLog('email-failed', { at: clock(), stage, error });
    return { ok: false, error };
  }
  const result = await sendPlainEmail({ to: emails, subject, text });
  const at = clock();
  if (result.ok) {
    s.lastEmailAt = at;
    s.lastEmailError = null;
    await writeLog(stage, { at, subject, to: emails });
    return { ok: true };
  }
  s.lastEmailError = result.error;
  await writeLog('email-failed', { at, stage, error: result.error, to: emails });
  return result;
}

/** Manual start of the location poll. One email. */
export async function notifyPrimaryStarted(): Promise<{ ok: boolean; error?: string }> {
  await loadLocationAlertSettings();
  const s = state();
  s.outageNotified = false;
  const at = clock();
  return deliver(
    'started',
    'Location service started',
    `The location poll was started at ${at}. It polls every ${currentLocationPollStatus().settings.pollIntervalSeconds} seconds.`,
  );
}

/** Manual stop of the location poll. One email. */
export async function notifyPrimaryStopped(): Promise<{ ok: boolean; error?: string }> {
  await loadLocationAlertSettings();
  const at = clock();
  return deliver(
    'stopped',
    'Location service stopped',
    `The location poll was stopped at ${at}. Stop the service checker before a planned stop so the checker does not also report the poll as down.`,
  );
}

async function runCheck(): Promise<void> {
  const s = state();
  if (!s.running) return;
  s.lastCheckAt = clock();
  if (currentLocationPollStatus().running) {
    s.outageNotified = false;
    s.lastResult = 'Primary service is running';
    return;
  }
  if (s.outageNotified) {
    s.lastResult = 'Primary service is stopped. Alert already sent.';
    return;
  }
  const result = await deliver(
    'checker-stopped',
    'ALERT: location poll is down',
    `Service checker found the location poll stopped at ${s.lastCheckAt}. The checker is running and this alert is sent once until the poll is started again.`,
  );
  if (result.ok) s.outageNotified = true;
  s.lastResult = result.ok ? 'Primary service is stopped. Alert sent.' : `Primary service is stopped. ${result.error ?? 'Email failed'}`;
}

function armChecker(s: CheckerState): void {
  if (s.timer) clearInterval(s.timer);
  const ms = s.settings.checkerIntervalSeconds * 1000;
  s.timer = setInterval(() => {
    void runCheck();
  }, ms);
}

export async function loadLocationAlertSettings(): Promise<LocationAlertSettings> {
  const settings = await readSettings();
  state().settings = settings;
  return settings;
}

export async function saveLocationAlertSettings(
  input: Partial<LocationAlertSettings>,
): Promise<LocationAlertSettings> {
  const parsed = parseAlertEmails(typeof input.alertEmails === 'string' ? input.alertEmails : state().settings.alertEmails);
  if (parsed.invalid.length > 0) {
    throw new Error(`Invalid email: ${parsed.invalid.join(', ')}`);
  }
  const settings = normalizeSettings({
    ...state().settings,
    ...input,
    alertEmails: parsed.emails.join(', '),
  });
  await writeSettings(settings);
  const s = state();
  s.settings = settings;
  if (s.running && s.timer) armChecker(s);
  return settings;
}

export async function startLocationServiceChecker(
  input?: Partial<LocationAlertSettings>,
): Promise<LocationAlertStatus> {
  if (input) await saveLocationAlertSettings(input);
  else await loadLocationAlertSettings();
  const s = state();
  const already = s.running && s.timer != null;
  s.running = true;
  if (!already) armChecker(s);
  if (!already) void runCheck();
  return locationAlertStatus();
}

export function stopLocationServiceChecker(): LocationAlertStatus {
  const s = state();
  if (s.timer) clearInterval(s.timer);
  s.timer = null;
  s.running = false;
  s.lastResult = 'Checker stopped';
  return locationAlertStatus();
}

export type LocationAlertStatus = {
  alertEmails: string;
  checkerIntervalSeconds: number;
  checkerRunning: boolean;
  sendGridConfigured: boolean;
  lastCheckAt: string | null;
  lastResult: string | null;
  lastEmailAt: string | null;
  lastEmailError: string | null;
};

export function locationAlertStatus(): LocationAlertStatus {
  const s = state();
  return {
    alertEmails: s.settings.alertEmails,
    checkerIntervalSeconds: s.settings.checkerIntervalSeconds,
    checkerRunning: s.running && s.timer != null,
    sendGridConfigured: sendGridConfigured(),
    lastCheckAt: s.lastCheckAt,
    lastResult: s.lastResult,
    lastEmailAt: s.lastEmailAt,
    lastEmailError: s.lastEmailError,
  };
}
