import { NextResponse } from 'next/server';
import {
  listMappingMailAudit,
  mappingPageUrl,
  readMappingMailSettings,
  saveMappingMailSettings,
  sendMappingMail,
} from '@/lib/mapping-mail';
import { sendGridConfigured } from '@/lib/sendgrid-mail';

/** GET: subject, body, and recipients for the mapping email. */
export async function GET() {
  try {
    const settings = await readMappingMailSettings();
    const audit = await listMappingMailAudit();
    return NextResponse.json({ ...settings, link: mappingPageUrl(), sendGrid: sendGridConfigured(), audit });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: save the email, or { send: true } to send the current email now. */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      send?: boolean;
      subject?: string;
      body?: string;
      recipients?: string;
    };
    if (body.send) {
      if (typeof body.subject === 'string' || typeof body.body === 'string' || typeof body.recipients === 'string') {
        await saveMappingMailSettings({
          subject: body.subject ?? '',
          body: body.body ?? '',
          recipients: body.recipients ?? '',
        });
      }
      const result = await sendMappingMail('manual');
      const audit = await listMappingMailAudit();
      if (!result.sent) return NextResponse.json({ error: result.error ?? 'Email was not sent', audit }, { status: 400 });
      return NextResponse.json({ ok: true, to: result.to, messageId: result.messageId, link: mappingPageUrl(), audit });
    }
    const settings = await saveMappingMailSettings({
      subject: body.subject ?? '',
      body: body.body ?? '',
      recipients: body.recipients ?? '',
    });
    return NextResponse.json({ ok: true, ...settings, link: mappingPageUrl() });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
