'use client';

import { useCallback, useEffect, useState } from 'react';

type IssueJob = { id: number; jobId: string; driver: string | null; plannedStart: string | null };

type IssueRow = {
  id: number;
  kind: 'Winery' | 'Vineyard' | 'Driver';
  vworkName: string;
  candidates: string[];
  createdAt: string;
  jobs: IssueJob[];
};

type DriverChoice = { label: string; imei: string };

type ResolvedRow = {
  id: number;
  kind: 'Winery' | 'Vineyard' | 'Driver';
  vworkName: string;
  mappedTo: string;
  resolvedAt: string;
};

type IssuesResponse = {
  open?: number;
  fences?: string[];
  drivers?: DriverChoice[];
  rows?: IssueRow[];
  resolved?: ResolvedRow[];
  error?: string;
};

type MailAudit = {
  id: number;
  at: string;
  outcome: string;
  trigger: string;
  to: string[];
  sendgridStatus: number | null;
  sendgridMessageId: string | null;
  error: string | null;
  openMappings: { kind: string; name: string; jobIds: string[] }[];
};

function CogIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" className="h-4 w-4" aria-hidden="true">
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M10.3 3.2h3.4l.5 2.1a7 7 0 0 1 1.6.9l2-.8 1.7 2.9-1.5 1.4a7 7 0 0 1 0 1.8l1.5 1.4-1.7 2.9-2-.8a7 7 0 0 1-1.6.9l-.5 2.1h-3.4l-.5-2.1a7 7 0 0 1-1.6-.9l-2 .8-1.7-2.9 1.5-1.4a7 7 0 0 1 0-1.8L4.5 8.3l1.7-2.9 2 .8a7 7 0 0 1 1.6-.9l.5-2.1z"
      />
      <circle cx="12" cy="12" r="2.3" />
    </svg>
  );
}

function EmailSettingsButton() {
  const [open, setOpen] = useState(false);
  return (
    <div className="flex w-full max-w-md flex-col items-end gap-2">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="inline-flex items-center gap-1.5 rounded border border-zinc-300 bg-white px-2.5 py-1.5 text-xs font-medium text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200"
      >
        <CogIcon />
        Email Settings
      </button>
      {open ? <MappingMailConfig /> : null}
    </div>
  );
}

function MappingMailConfig() {
  const [subject, setSubject] = useState('');
  const [body, setBody] = useState('');
  const [recipients, setRecipients] = useState('');
  const [link, setLink] = useState('');
  const [audit, setAudit] = useState<MailAudit[]>([]);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const loadMail = useCallback(() => {
    fetch('/api/driver-jobs/queue-issues/mail', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as {
          subject?: string;
          body?: string;
          recipients?: string;
          link?: string;
          audit?: MailAudit[];
          error?: string;
        };
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setSubject(data.subject ?? '');
        setBody(data.body ?? '');
        setRecipients(data.recipients ?? '');
        setLink(data.link ?? '');
        setAudit(data.audit ?? []);
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)));
  }, []);

  useEffect(() => {
    loadMail();
  }, [loadMail]);

  async function save(send: boolean) {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const response = await fetch('/api/driver-jobs/queue-issues/mail', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subject, body, recipients, send }),
      });
      const data = (await response.json()) as { error?: string; to?: string[]; link?: string; messageId?: string | null; audit?: MailAudit[] };
      if (data.audit) setAudit(data.audit);
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      if (data.link) setLink(data.link);
      setNote(send ? `SendGrid accepted the email${data.messageId ? ` (${data.messageId})` : ''}` : 'Saved');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form
      className="w-full max-w-md rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900"
      onSubmit={(event) => {
        event.preventDefault();
        void save(false);
      }}
    >
      <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Mapping email</h2>
      <p className="mt-1 text-xs text-zinc-500">Sent from the SendGrid from address when a VWork run still has unresolved mappings.</p>
      <label className="mt-2 block text-xs text-zinc-600 dark:text-zinc-300">
        Subject
        <input
          value={subject}
          onChange={(event) => setSubject(event.target.value)}
          className="mt-1 block w-full rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
        />
      </label>
      <label className="mt-2 block text-xs text-zinc-600 dark:text-zinc-300">
        Body
        <textarea
          value={body}
          onChange={(event) => setBody(event.target.value)}
          rows={4}
          className="mt-1 block w-full rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
        />
      </label>
      <label className="mt-2 block text-xs text-zinc-600 dark:text-zinc-300">
        Recipients
        <textarea
          value={recipients}
          onChange={(event) => setRecipients(event.target.value)}
          rows={2}
          placeholder="one@example.com, two@example.com"
          className="mt-1 block w-full rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
        />
      </label>
      {link && <p className="mt-2 break-all text-xs text-zinc-500">Link: {link}</p>}
      {error && <p className="mt-2 text-xs text-red-600 dark:text-red-400">{error}</p>}
      {note && <p className="mt-2 text-xs text-emerald-700 dark:text-emerald-300">{note}</p>}
      <div className="mt-3 flex gap-2">
        <button type="submit" disabled={busy} className="rounded bg-zinc-800 px-3 py-1 text-xs font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900">
          {busy ? 'Saving…' : 'Save'}
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void save(true)}
          className="rounded border border-zinc-300 px-3 py-1 text-xs font-medium disabled:opacity-50 dark:border-zinc-600"
        >
          Send now
        </button>
      </div>
      {audit.length > 0 && (
        <div className="mt-3 border-t border-zinc-200 pt-2 dark:border-zinc-700">
          <h3 className="text-xs font-semibold text-zinc-700 dark:text-zinc-300">Send audit</h3>
          <ul className="mt-1 space-y-2">
            {audit.slice(0, 8).map((entry) => (
              <li key={entry.id} className="text-xs text-zinc-600 dark:text-zinc-400">
                <span className={entry.outcome === 'accepted' ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-600 dark:text-red-400'}>
                  {entry.outcome === 'accepted' ? 'Accepted' : 'Not sent'}
                </span>
                {' '}
                {entry.at} · {entry.trigger} · {entry.to.length > 0 ? entry.to.join(', ') : 'no recipients'}
                {entry.sendgridStatus != null ? ` · SendGrid ${entry.sendgridStatus}` : ''}
                {entry.sendgridMessageId ? ` · ${entry.sendgridMessageId}` : ''}
                {entry.error ? ` · ${entry.error}` : ''}
                {entry.openMappings.length > 0 && (
                  <span className="mt-0.5 block">
                    Open: {entry.openMappings.map((issue) => `${issue.kind} ${issue.name}`).join('; ')}
                  </span>
                )}
              </li>
            ))}
          </ul>
          <p className="mt-1 text-xs text-zinc-500">Full history is in Logs, type mapping_mail.</p>
        </div>
      )}
    </form>
  );
}

export function JobQueueIssues({
  onChange,
  onResolved,
  showMailConfig = false,
}: {
  onChange?: (open: number) => void;
  onResolved?: (saved: { kind: 'Winery' | 'Vineyard' | 'Driver'; vworkName: string; fenceName: string }) => void;
  showMailConfig?: boolean;
}) {
  const [rows, setRows] = useState<IssueRow[]>([]);
  const [resolved, setResolved] = useState<ResolvedRow[]>([]);
  const [fences, setFences] = useState<string[]>([]);
  const [drivers, setDrivers] = useState<DriverChoice[]>([]);
  const [picks, setPicks] = useState<Record<number, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(() => {
    fetch('/api/driver-jobs/queue-issues', { cache: 'no-store' })
      .then(async (response) => {
        const data = (await response.json()) as IssuesResponse;
        if (!response.ok) throw new Error(data.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setResolved(data.resolved ?? []);
        setFences(data.fences ?? []);
        setDrivers(data.drivers ?? []);
        onChange?.(data.open ?? data.rows?.length ?? 0);
        setError(null);
        setLoaded(true);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : String(err));
        setLoaded(true);
      });
  }, [onChange]);

  useEffect(() => {
    load();
    const timer = setInterval(load, 15000);
    return () => clearInterval(timer);
  }, [load]);

  async function resolve(issue: IssueRow) {
    const fenceName = picks[issue.id] ?? '';
    if (!fenceName) return;
    setBusyId(issue.id);
    setError(null);
    try {
      const response = await fetch('/api/driver-jobs/queue-issues', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: issue.id, fenceName }),
      });
      const data = (await response.json()) as IssuesResponse;
      if (!response.ok) throw new Error(data.error ?? response.statusText);
      setRows(data.rows ?? []);
      setFences(data.fences ?? []);
      setDrivers(data.drivers ?? []);
      onChange?.(data.open ?? data.rows?.length ?? 0);
      onResolved?.({ kind: issue.kind, vworkName: issue.vworkName, fenceName });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-4">
        <p className="max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          A VWork winery, vineyard, or driver name is stored as typed. A fence is filled only when the place name maps to one fence, and an IMEI is filled only when the driver name maps to one driver in the TrackSolid driver table. Anything else stays here until you pick the match. That choice is kept for this VWork import.
        </p>
        {showMailConfig && <EmailSettingsButton />}
      </div>
      {error && <div className="mb-4 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>}
      {!loaded ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">Checking fence names…</p>
      ) : rows.length === 0 ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">No open fence names.</p>
      ) : (
        <div className="space-y-3">
          {rows.map((issue) => (
            <div key={issue.id} className="rounded-lg border border-amber-300 bg-white p-3 dark:border-amber-700 dark:bg-zinc-900">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                  {issue.kind}: {issue.vworkName}
                </h2>
                <p className="text-xs text-zinc-500">{issue.createdAt}</p>
              </div>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                {issue.kind === 'Driver'
                  ? 'This driver name does not match one driver ID with a TrackSolid IMEI.'
                  : issue.candidates.length > 0
                    ? `Mapped fences: ${issue.candidates.join(', ')}`
                    : 'No fence mapping for this name.'}
              </p>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                Jobs waiting: {issue.jobs.length === 0 ? '—' : issue.jobs.map((job) => job.jobId).join(', ')}
              </p>
              <div className="mt-3 flex flex-wrap items-end gap-2">
                <label className="text-xs text-zinc-600 dark:text-zinc-300">
                  {issue.kind === 'Driver' ? 'Driver' : `${issue.kind} geo fence`}
                  <select
                    value={picks[issue.id] ?? ''}
                    onChange={(event) => setPicks((current) => ({ ...current, [issue.id]: event.target.value }))}
                    className="mt-1 block w-72 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  >
                    <option value="">{issue.kind === 'Driver' ? 'Select driver' : 'Select fence'}</option>
                    {issue.kind === 'Driver' ? (
                      drivers.map((driver) => (
                        <option key={driver.label} value={driver.label}>
                          {driver.label} · {driver.imei}
                        </option>
                      ))
                    ) : (
                      <>
                        {issue.candidates.length > 0 && (
                          <optgroup label="Mapped fences">
                            {issue.candidates.map((name) => (
                              <option key={`mapped-${name}`} value={name}>
                                {name}
                              </option>
                            ))}
                          </optgroup>
                        )}
                        <optgroup label="All geo fences">
                          {fences.map((name) => (
                            <option key={name} value={name}>
                              {name}
                            </option>
                          ))}
                        </optgroup>
                      </>
                    )}
                  </select>
                </label>
                <button
                  type="button"
                  disabled={busyId === issue.id || !(picks[issue.id] ?? '')}
                  onClick={() => void resolve(issue)}
                  className="rounded bg-blue-600 px-3 py-1 text-xs font-medium text-white disabled:opacity-50"
                >
                  {busyId === issue.id ? 'Saving…' : 'Save mapping'}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
      {loaded && resolved.length > 0 && (
        <div className="mt-6 space-y-2">
          <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Resolved</h2>
          {resolved.map((issue) => (
            <div key={issue.id} className="rounded-lg border border-emerald-300 bg-white p-3 dark:border-emerald-800 dark:bg-zinc-900">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <p className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
                  {issue.kind}: {issue.vworkName}
                </p>
                <p className="text-xs text-emerald-700 dark:text-emerald-300">Mapped and resolved</p>
              </div>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                {issue.kind === 'Driver' ? 'Driver' : 'Geo fence'}: {issue.mappedTo || '—'}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
