'use client';

import { useEffect, useState } from 'react';
import { locationAgeSeconds } from '@/components/driver/useLocationAge';
import { phoneLocalStamp } from '@/lib/driver-jobs';
import { staffActorPayload } from '@/lib/staff-session';

type DiversionType = { id: number; name: string };
type JobDiversion = {
  id: number;
  type_name: string;
  note: string | null;
  expected_duration: string | null;
  started_at: string;
  ended_at: string | null;
};

const EXPECTED = [
  { id: '15', label: '15 min' },
  { id: '30', label: '30 min' },
  { id: '45', label: '45 min' },
  { id: '60', label: '60 min' },
  { id: '60+', label: '60+ min' },
] as const;

function formatMinutes(seconds: number): string {
  const mins = Math.floor(Math.max(0, seconds) / 60);
  return mins === 1 ? '1 min' : `${mins} mins`;
}

function CenterDialog({ children }: { children: React.ReactNode }) {
  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/45 p-4" role="dialog" aria-modal="true">
      <div className="w-full max-w-[22rem] rounded-2xl bg-white p-5 shadow-xl">{children}</div>
    </div>
  );
}

/** Start or stop a diversion clock. Shown at the top right of the gate buttons. */
export default function DriverDiversionControl({
  jobId,
  refreshKey = 0,
  embedded = false,
  readOnly = false,
  control = false,
}: {
  jobId: number;
  refreshKey?: number;
  embedded?: boolean;
  /** Staff watch mode. The button stays visible and does not start or stop anything. */
  readOnly?: boolean;
  /** Staff control mode. Starts and stops are stored as the admin. */
  control?: boolean;
}) {
  const [types, setTypes] = useState<DiversionType[]>([]);
  const [open, setOpen] = useState<JobDiversion | null>(null);
  const [starting, setStarting] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [typeName, setTypeName] = useState('');
  const [expected, setExpected] = useState('');
  const [note, setNote] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);

  async function load() {
    const res = await fetch(`/api/driver-jobs/${jobId}/diversions`, { cache: 'no-store' });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.error ?? res.statusText);
    setTypes(Array.isArray(data?.types) ? (data.types as DiversionType[]) : []);
    const rows = Array.isArray(data?.diversions) ? (data.diversions as JobDiversion[]) : [];
    setOpen(rows.find((row) => !row.ended_at) ?? null);
  }

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    load().catch((err) => {
      if (!cancelled) setError(err instanceof Error ? err.message : String(err));
    });
    return () => {
      cancelled = true;
    };
  }, [jobId, refreshKey]);

  useEffect(() => {
    if (!open?.started_at) return;
    const tick = () => {
      const age = locationAgeSeconds(open.started_at);
      setElapsedSeconds(age ?? 0);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [open?.started_at]);

  function staffFields(): { control?: true; actor?: { email: string; role: 'Super Admin' | 'Admin' } } {
    if (!control) return {};
    const actor = staffActorPayload();
    return actor ? { control: true, actor } : {};
  }

  async function start() {
    if (readOnly) return;
    if (control && !staffActorPayload()) {
      setError('Sign in as an admin to control this job.');
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`/api/driver-jobs/${jobId}/diversions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ typeName, note, expected, at: phoneLocalStamp(), ...staffFields() }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setStarting(false);
      setTypeName('');
      setExpected('');
      setNote('');
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function stop() {
    if (readOnly) return;
    if (control && !staffActorPayload()) {
      setError('Sign in as an admin to control this job.');
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`/api/driver-jobs/${jobId}/diversions/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ at: phoneLocalStamp(), ...staffFields() }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setStopping(false);
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <div className={embedded ? 'shrink-0' : 'flex justify-end px-4 pt-1'}>
        {open ? (
          <button
            type="button"
            tabIndex={-1}
            onMouseDown={(event) => event.preventDefault()}
            onClick={() => {
              if (readOnly) return;
              setError(null);
              setStopping(true);
            }}
            className="cursor-pointer select-none whitespace-nowrap rounded-full bg-[#b54708] px-2.5 py-1 text-xs font-semibold text-white outline-none"
          >
            {open.type_name} · {formatMinutes(elapsedSeconds)}
          </button>
        ) : (
          <button
            type="button"
            tabIndex={-1}
            onMouseDown={(event) => event.preventDefault()}
            onClick={() => {
              if (readOnly) return;
              setError(null);
              setStarting(true);
            }}
            className="cursor-pointer select-none whitespace-nowrap rounded-full border border-[#d0d5dd] bg-white px-2.5 py-1 text-xs font-semibold text-[#1c2430] outline-none"
          >
            Diversion
          </button>
        )}
      </div>
      {starting && (
        <CenterDialog>
          <p className="text-lg font-semibold text-[#1c2430]">Start a diversion</p>
          <p className="mt-1 text-sm text-[#475467]">The clock runs until you stop it. Moving to the next step asks you to end it first.</p>
          <div className="mt-4 flex flex-col gap-2">
            {types.length === 0 ? (
              <p className="text-sm text-[#667085]">No diversions are set up yet.</p>
            ) : (
              types.map((type) => (
                <button
                  key={type.id}
                  type="button"
                  onClick={() => setTypeName(type.name)}
                  className={
                    typeName === type.name
                      ? 'h-11 rounded-xl bg-[#1f9d55] px-3 text-left text-base font-semibold text-white'
                      : 'h-11 rounded-xl bg-[#f2f4f7] px-3 text-left text-base font-medium text-[#1c2430]'
                  }
                >
                  {type.name}
                </button>
              ))
            )}
          </div>
          <p className="mt-4 text-sm font-medium text-[#344054]">Expected duration</p>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {EXPECTED.map((band) => (
              <button
                key={band.id}
                type="button"
                onClick={() => setExpected(band.id)}
                className={
                  expected === band.id
                    ? 'rounded-full bg-[#1f9d55] px-2.5 py-1 text-xs font-semibold text-white'
                    : 'rounded-full bg-[#f2f4f7] px-2.5 py-1 text-xs font-medium text-[#344054]'
                }
              >
                {band.label}
              </button>
            ))}
          </div>
          <label className="mt-4 block text-sm font-medium text-[#344054]">
            Note
            <textarea
              value={note}
              onChange={(event) => setNote(event.target.value)}
              rows={3}
              placeholder="What happened?"
              className="mt-1 w-full resize-none rounded-xl border border-[#d0d5dd] px-3 py-2 text-base text-[#1c2430]"
            />
          </label>
          {error && <p className="mt-2 text-sm text-[#b42318]">{error}</p>}
          <div className="mt-5 flex gap-3">
            <button
              type="button"
              onClick={() => {
                setStarting(false);
                setError(null);
              }}
              className="h-12 flex-1 rounded-2xl bg-[#e4e7ec] text-base font-semibold text-[#1c2430]"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={busy || !typeName || !expected}
              onClick={() => void start()}
              className="h-12 flex-1 rounded-2xl bg-[#1f9d55] text-base font-semibold text-white disabled:opacity-60"
            >
              {busy ? 'Starting…' : 'Start'}
            </button>
          </div>
        </CenterDialog>
      )}
      {stopping && open && (
        <CenterDialog>
          <p className="text-lg font-semibold text-[#1c2430]">Stop {open.type_name}?</p>
          <p className="mt-2 text-base text-[#475467]">
            Running for {formatMinutes(elapsedSeconds)}. Expected {open.expected_duration === '60+' ? 'more than 60 min' : open.expected_duration ? `${open.expected_duration} min` : '—'}. Stopping returns you to travel time.
          </p>
          {open.note ? <p className="mt-2 text-sm text-[#344054]">{open.note}</p> : null}
          {error && <p className="mt-2 text-sm text-[#b42318]">{error}</p>}
          <div className="mt-5 flex gap-3">
            <button
              type="button"
              onClick={() => {
                setStopping(false);
                setError(null);
              }}
              className="h-12 flex-1 rounded-2xl bg-[#e4e7ec] text-base font-semibold text-[#1c2430]"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => void stop()}
              className="h-12 flex-1 rounded-2xl bg-[#b54708] text-base font-semibold text-white disabled:opacity-60"
            >
              {busy ? 'Stopping…' : 'Stop'}
            </button>
          </div>
        </CenterDialog>
      )}
    </>
  );
}
