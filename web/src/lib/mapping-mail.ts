import { execute, query } from '@/lib/db';
import { parseAlertEmails } from '@/lib/location-service-alerts';
import { sendGridConfigured, sendPlainEmail } from '@/lib/sendgrid-mail';

const SETTINGS_TYPE = 'VworkMappingMail';
const LOGTYPE = 'mapping_mail';
const CLOUD_ORIGIN = 'https://geofence--cel-geosystem.us-east4.hosted.app';
const LOCAL_ORIGIN = 'http://localhost:3000';
const PATH = '/resolve-mappings';

export type MappingMailSettings = {
  subject: string;
  body: string;
  recipients: string;
};

const DEFAULTS: MappingMailSettings = {
  subject: 'VWork mappings to resolve',
  body: 'A VWork import has mappings that still need a choice. Open the link to resolve every open winery, vineyard, and driver issue. The first person to save a mapping resolves it for everyone else.',
  recipients: '',
};

/** Localhost while developing. The cloud app address once this process is running in production. */
export function mappingPageUrl(): string {
  const configured = process.env.PUBLIC_APP_URL?.trim().replace(/\/$/, '');
  if (configured) return `${configured}${PATH}`;
  if (process.env.NODE_ENV === 'production') return `${CLOUD_ORIGIN}${PATH}`;
  return `${LOCAL_ORIGIN}${PATH}`;
}

export async function readMappingMailSettings(): Promise<MappingMailSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue FROM tbl_settings WHERE type = $1 AND settingname = ANY($2::text[])`,
    [SETTINGS_TYPE, ['subject', 'body', 'recipients', 'lastNotifiedIds']],
  );
  const found: Partial<MappingMailSettings> = {};
  for (const row of rows) {
    if (row.settingname === 'subject' || row.settingname === 'body' || row.settingname === 'recipients') {
      found[row.settingname] = row.settingvalue ?? '';
    }
  }
  return {
    subject: found.subject?.trim() || DEFAULTS.subject,
    body: found.body?.trim() || DEFAULTS.body,
    recipients: found.recipients ?? '',
  };
}

async function writeSetting(name: string, value: string): Promise<void> {
  await execute('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [SETTINGS_TYPE, name]);
  await execute('INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)', [
    SETTINGS_TYPE,
    name,
    value,
  ]);
}

export async function saveMappingMailSettings(input: MappingMailSettings): Promise<MappingMailSettings> {
  const subject = input.subject.trim();
  const body = input.body.trim();
  const parsed = parseAlertEmails(input.recipients.split(/[\n;]+/).join(','));
  if (!subject) throw new Error('Subject is required');
  if (!body) throw new Error('Body is required');
  if (parsed.invalid.length > 0) throw new Error(`Check these email addresses: ${parsed.invalid.join(', ')}`);
  const settings = { subject, body, recipients: parsed.emails.join(', ') };
  await writeSetting('subject', settings.subject);
  await writeSetting('body', settings.body);
  await writeSetting('recipients', settings.recipients);
  return settings;
}

function messageText(body: string, url: string): string {
  const text = body.includes('{url}') ? body.replaceAll('{url}', url) : `${body}\n\n${url}`;
  return text;
}

type OpenMapping = { id: number; kind: string; name: string; jobIds: string[] };

async function openMappings(): Promise<OpenMapping[]> {
  const issues = await query<{ id: number; kind: string; vwork_name: string }>(
    `SELECT id, kind, vwork_name FROM vwork_job_queue_issues WHERE status = 'open' ORDER BY id`,
  );
  const rows: OpenMapping[] = [];
  for (const issue of issues) {
    const nameColumn =
      issue.kind === 'Winery' ? 'winery_name' : issue.kind === 'Vineyard' ? 'vineyard_name' : issue.kind === 'Driver' ? 'driver' : null;
    if (!nameColumn) {
      rows.push({ id: issue.id, kind: issue.kind, name: issue.vwork_name, jobIds: [] });
      continue;
    }
    const jobs = await query<{ job_id: string }>(
      `SELECT job_id FROM tbl_driver_jobs
       WHERE vwork_job_id IS NOT NULL AND lower(btrim(coalesce(${nameColumn}, ''))) = lower($1)
       ORDER BY job_id`,
      [issue.vwork_name],
    );
    rows.push({ id: issue.id, kind: issue.kind, name: issue.vwork_name, jobIds: jobs.map((job) => job.job_id) });
  }
  return rows;
}

export type MappingMailAudit = {
  id: number;
  at: string;
  outcome: string;
  trigger: string;
  from: string | null;
  to: string[];
  subject: string | null;
  url: string | null;
  sendgridStatus: number | null;
  sendgridMessageId: string | null;
  error: string | null;
  openMappings: OpenMapping[];
};

async function writeAudit(details: Record<string, unknown>, outcome: string, trigger: string): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE, outcome, trigger, JSON.stringify(details)],
    );
  } catch (err) {
    console.error('[mapping-mail] audit log failed', err);
  }
}

export async function listMappingMailAudit(limit = 20): Promise<MappingMailAudit[]> {
  const rows = await query<{
    logid: number;
    logdatetime: string | null;
    logcat1: string | null;
    logcat2: string | null;
    logdetails: string | null;
  }>(
    `SELECT logid, logdatetime::text AS logdatetime, logcat1, logcat2, logdetails
     FROM tbl_logs
     WHERE logtype = $1
     ORDER BY logid DESC
     LIMIT $2`,
    [LOGTYPE, limit],
  );
  return rows.map((row) => {
    let parsed: Record<string, unknown> = {};
    try {
      parsed = JSON.parse(row.logdetails ?? '{}') as Record<string, unknown>;
    } catch {
      parsed = {};
    }
    const to = Array.isArray(parsed.to) ? parsed.to.map((item) => String(item)) : [];
    const open = Array.isArray(parsed.openMappings) ? (parsed.openMappings as OpenMapping[]) : [];
    return {
      id: row.logid,
      at: row.logdatetime ?? '',
      outcome: row.logcat1 ?? '',
      trigger: row.logcat2 ?? '',
      from: typeof parsed.from === 'string' ? parsed.from : null,
      to,
      subject: typeof parsed.subject === 'string' ? parsed.subject : null,
      url: typeof parsed.url === 'string' ? parsed.url : null,
      sendgridStatus: typeof parsed.sendgridStatus === 'number' ? parsed.sendgridStatus : null,
      sendgridMessageId: typeof parsed.sendgridMessageId === 'string' ? parsed.sendgridMessageId : null,
      error: typeof parsed.error === 'string' ? parsed.error : null,
      openMappings: open,
    };
  });
}

export async function sendMappingMail(
  trigger: 'manual' | 'vwork-run',
): Promise<{ sent: boolean; to: string[]; messageId: string | null; error: string | null }> {
  const settings = await readMappingMailSettings();
  const parsed = parseAlertEmails(settings.recipients.split(/[\n;]+/).join(','));
  const url = mappingPageUrl();
  const from = process.env.SENDGRID_FROM_EMAIL?.trim() || null;
  const waiting = await openMappings();
  const base = {
    from,
    to: parsed.emails,
    subject: settings.subject,
    url,
    openMappings: waiting,
  };
  if (parsed.emails.length === 0) {
    await writeAudit({ ...base, error: 'Add at least one recipient' }, 'failed', trigger);
    return { sent: false, to: [], messageId: null, error: 'Add at least one recipient' };
  }
  if (!sendGridConfigured()) {
    await writeAudit({ ...base, error: 'SendGrid is not configured' }, 'failed', trigger);
    return { sent: false, to: parsed.emails, messageId: null, error: 'SendGrid is not configured' };
  }
  const result = await sendPlainEmail({
    to: parsed.emails,
    subject: settings.subject,
    text: messageText(settings.body, url),
  });
  if (!result.ok) {
    await writeAudit({ ...base, sendgridStatus: result.status, error: result.error }, 'failed', trigger);
    return { sent: false, to: parsed.emails, messageId: null, error: result.error };
  }
  await writeAudit(
    { ...base, sendgridStatus: result.status, sendgridMessageId: result.messageId, accepted: true },
    'accepted',
    trigger,
  );
  return { sent: true, to: parsed.emails, messageId: result.messageId, error: null };
}

async function settingValue(name: string): Promise<string> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [SETTINGS_TYPE, name],
  );
  return rows[0]?.settingvalue ?? '';
}

/** After a VWork run, email once while this set of open mappings is waiting. A new mapping sends again. */
export async function notifyUnresolvedMappings(): Promise<void> {
  const open = await query<{ id: number }>(
    `SELECT id FROM vwork_job_queue_issues WHERE status = 'open' ORDER BY id`,
  );
  if (open.length === 0) return;
  const signature = open.map((row) => String(row.id)).join(',');
  if ((await settingValue('lastNotifiedIds')) === signature) return;
  if ((await settingValue('lastUnsentIds')) === signature) return;
  const sent = await sendMappingMail('vwork-run');
  if (!sent.sent) {
    if (sent.error === 'Add at least one recipient') await writeSetting('lastUnsentIds', signature);
    else if (sent.error) console.error('[mapping-mail]', sent.error);
    return;
  }
  await writeSetting('lastNotifiedIds', signature);
  await writeSetting('lastUnsentIds', '');
}
