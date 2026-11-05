'use client';

import { useEffect, useState } from 'react';

type DriverLogEvent = {
  id: string;
  at: string;
  kind: 'fence' | 'click' | 'ghost';
  title: string;
  detail: string | null;
};

type DiversionRow = {
  id: number;
  type_name: string;
  note: string | null;
  expected_duration: string | null;
  duration_seconds: number | null;
  started_at: string;
  ended_at: string | null;
  ended_by_step: number | null;
};

function expectedLabel(value: string | null): string {
  if (value === '60+') return 'More than 60 min';
  if (value === '15' || value === '30' || value === '45' || value === '60') return `${value} min`;
  return '—';
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return '—';
  const s = Math.max(0, Number(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h} h ${m} min`;
  if (m > 0) return sec > 0 ? `${m} min ${sec} s` : `${m} min`;
  return `${sec} s`;
}

const POLL_MS = 30_000;

/** Fence crossings and gate clicks for one job. Newest first. */
export default function DriverJobLog({ jobId }: { jobId: number }) {
  const [events, setEvents] = useState<DriverLogEvent[]>([]);
  const [diversions, setDiversions] = useState<DiversionRow[]>([]);
  const [tab, setTab] = useState<'log' | 'diversions'>('log');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState(POLL_MS);

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    async function load() {
      try {
        const res = await fetch(`/api/driver-jobs/${jobId}/log`, { cache: 'no-store' });
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        if (cancelled) return;
        setEvents(Array.isArray(data?.events) ? (data.events as DriverLogEvent[]) : []);
        setDiversions(Array.isArray(data?.diversions) ? (data.diversions as DiversionRow[]) : []);
        const seconds = data?.driverAppPollSeconds;
        if (typeof seconds === 'number' && seconds >= 5) setPollMs(seconds * 1000);
        setError(null);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    const timer = setInterval(() => void load(), pollMs);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [jobId, pollMs]);

  if (loading) return <p className="px-4 text-base text-[#667085]">Loading log…</p>;
  if (error) return <p className="mx-4 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>;

  const newestFirst = [...diversions].reverse();

  return (
    <div>
      <div className="mx-4 mb-3 grid grid-cols-2 rounded-xl bg-[#e4e7ec] p-1">
        {(['log', 'diversions'] as const).map((name) => (
          <button
            key={name}
            type="button"
            onClick={() => setTab(name)}
            className={
              tab === name
                ? 'h-9 rounded-lg bg-white text-sm font-semibold text-[#1c2430] shadow-sm'
                : 'h-9 rounded-lg text-sm font-medium text-[#667085]'
            }
          >
            {name === 'log' ? 'Log' : 'Diversions'}
          </button>
        ))}
      </div>
      {tab === 'log' ? (
        events.length === 0 ? (
          <p className="px-4 text-base leading-relaxed text-[#667085]">No fence crossings or gate clicks yet.</p>
        ) : (
          <ul className="flex flex-col gap-2 px-4 pb-[max(1.25rem,env(safe-area-inset-bottom))]">
            {events.map((event) => (
              <li key={event.id} className="rounded-2xl bg-white px-4 py-3 shadow-[0_1px_2px_rgba(16,24,40,0.06)]">
                <p className="text-xs tabular-nums text-[#667085]">{event.at}</p>
                <p className={`mt-0.5 text-base font-semibold ${event.kind === 'fence' ? 'text-[#175cd3]' : event.kind === 'ghost' ? 'text-[#b54708]' : 'text-[#1c2430]'}`}>
                  {event.title}
                </p>
                {event.detail ? <p className="mt-0.5 text-sm leading-snug text-[#475467]">{event.detail}</p> : null}
              </li>
            ))}
          </ul>
        )
      ) : newestFirst.length === 0 ? (
        <p className="px-4 text-base leading-relaxed text-[#667085]">No diversions yet.</p>
      ) : (
        <ul className="flex flex-col gap-2 px-4 pb-[max(1.25rem,env(safe-area-inset-bottom))]">
          {newestFirst.map((row) => (
            <li key={row.id} className="rounded-2xl bg-white px-4 py-3 shadow-[0_1px_2px_rgba(16,24,40,0.06)]">
              <p className="text-base font-semibold text-[#1c2430]">{row.type_name}</p>
              <p className="mt-1 text-sm text-[#475467]">Expected {expectedLabel(row.expected_duration)}</p>
              <p className="mt-1 text-sm tabular-nums text-[#475467]">Started {row.started_at}</p>
              <p className="mt-0.5 text-sm tabular-nums text-[#475467]">
                {row.ended_at
                  ? `Ended ${row.ended_at}${row.ended_by_step ? ` · closed by step ${row.ended_by_step}` : ''}`
                  : 'Still running'}
              </p>
              <p className="mt-1 text-sm font-semibold text-[#1c2430]">
                {row.ended_at ? `Duration ${formatDuration(row.duration_seconds)}` : 'Duration still open'}
              </p>
              {row.note ? <p className="mt-1 text-sm leading-snug text-[#344054]">{row.note}</p> : null}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
