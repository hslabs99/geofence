/** Send one plain-text message through the SendGrid v3 API. The key stays in the request header and is never logged. */

export type SendGridResult =
  | { ok: true; status: 202; messageId: string | null }
  | { ok: false; status: number | null; error: string };

export async function sendPlainEmail(input: {
  to: string[];
  subject: string;
  text: string;
}): Promise<SendGridResult> {
  const key = process.env.SENDGRID_API_KEY?.trim();
  const from = process.env.SENDGRID_FROM_EMAIL?.trim();
  const fromName = process.env.SENDGRID_FROM_NAME?.trim() || 'GEO APP';
  if (!key || !from) return { ok: false, status: null, error: 'SendGrid is not configured' };
  if (input.to.length === 0) return { ok: false, status: null, error: 'No alert email addresses' };

  const res = await fetch('https://api.sendgrid.com/v3/mail/send', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${key}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      personalizations: [{ to: input.to.map((email) => ({ email })) }],
      from: { email: from, name: fromName },
      subject: input.subject,
      content: [{ type: 'text/plain', value: input.text }],
    }),
  });
  if (res.status === 202) {
    return { ok: true, status: 202, messageId: res.headers.get('x-message-id') };
  }
  const body = (await res.text()).slice(0, 300);
  return { ok: false, status: res.status, error: `SendGrid ${res.status}${body ? `: ${body}` : ''}` };
}

export function sendGridConfigured(): boolean {
  return Boolean(process.env.SENDGRID_API_KEY?.trim() && process.env.SENDGRID_FROM_EMAIL?.trim());
}
