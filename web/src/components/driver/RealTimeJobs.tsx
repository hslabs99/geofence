'use client';

import { useEffect, useMemo, useState } from 'react';
import DriverJobLog from '@/components/driver/DriverJobLog';
import GateTrack, { type CheckpointMark, type TruckMeters } from '@/components/driver/GateTrack';
import { locationAgeSeconds } from '@/components/driver/useLocationAge';
import {
  DRIVER_GATES,
  completedGateCount,
  gateClock,
  gateManual,
  minutesBetween,
  truckStage,
  wineryArrivalEta,
  type DriverJob,
} from '@/lib/driver-jobs';
import type { LiveDriverJob, LivePlace } from '@/lib/live-driver-jobs';

const POLL_MS = 30_000;

const selectCls =
  'w-full max-w-[16rem] rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800';

const MODES = [
  { id: 'to-winery', title: 'To winery', match: (done: number) => done === 3 },
  { id: 'unloading', title: 'Unloading', match: (done: number) => done === 4 },
  { id: 'loading', title: 'Loading', match: (done: number) => done === 2 },
  { id: 'to-vineyard', title: 'To vineyard', match: (done: number) => done === 1 },
] as const;

function jobMode(done: number): string | null {
  return MODES.find((mode) => mode.match(done))?.title ?? null;
}

function RefreshClock({
  secondsLeft,
  pollSeconds,
  refreshing,
  onRefresh,
}: {
  secondsLeft: number | null;
  pollSeconds: number;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  const fraction =
    secondsLeft == null || pollSeconds <= 0 ? 0 : Math.min(1, Math.max(0, secondsLeft / pollSeconds));
  const radius = 14;
  const circumference = 2 * Math.PI * radius;
  const label =
    refreshing || (secondsLeft != null && secondsLeft <= 0)
      ? 'Refreshing'
      : secondsLeft == null
        ? 'Waiting for the next refresh'
        : `Next refresh in ${secondsLeft} seconds`;

  return (
    <div className="flex items-center gap-2">
      <div className="relative h-9 w-9 shrink-0" role="timer" aria-label={label} title={label}>
        <svg viewBox="0 0 36 36" className="h-9 w-9 -rotate-90" aria-hidden="true">
          <circle cx="18" cy="18" r={radius} fill="none" className="stroke-zinc-200 dark:stroke-zinc-700" strokeWidth="3" />
          <circle
            cx="18"
            cy="18"
            r={radius}
            fill="none"
            className="stroke-blue-600 transition-[stroke-dashoffset] duration-1000 ease-linear dark:stroke-blue-400"
            strokeWidth="3"
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={circumference * (1 - fraction)}
          />
        </svg>
        <span className="absolute inset-0 flex items-center justify-center text-[11px] font-semibold tabular-nums text-zinc-900 dark:text-zinc-100">
          {secondsLeft == null ? '…' : secondsLeft}
        </span>
      </div>
      <button
        type="button"
        onClick={onRefresh}
        disabled={refreshing}
        className="rounded border border-zinc-300 px-2.5 py-1 text-xs font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:text-zinc-100 dark:hover:bg-zinc-800"
      >
        {refreshing ? 'Refreshing…' : 'Refresh now'}
      </button>
    </div>
  );
}

/** Started, and step 5 has not been recorded. */
function isLiveJob(job: DriverJob): boolean {
  return Boolean(job.step_1_click?.trim()) && !job.step_5_click?.trim();
}

/** Share of this road already behind the truck. Outbound uses the start fix. The return uses the two fences. */
function journeyShare(total: number | null, remaining: number | null): number | null {
  if (total == null || remaining == null || total <= 0) return null;
  return Math.min(100, Math.max(0, Math.round((1 - remaining / total) * 100)));
}

function minuteLabel(seconds: number | null): string | null {
  if (seconds == null) return null;
  const mins = Math.max(0, Math.round(seconds / 60));
  return mins === 1 ? '1 min' : `${mins} min`;
}

/** Minutes still to go, from the time already spent on the share already covered. */
function etaMinutesLeft(legStart: string | null | undefined, percent: number | null, now: Date): number | null {
  if (percent == null || percent < 5 || percent >= 100) return null;
  const elapsed = locationAgeSeconds(legStart, now);
  if (elapsed == null || elapsed < 60) return null;
  const remainingSec = (elapsed * (100 - percent)) / percent;
  if (!Number.isFinite(remainingSec)) return null;
  return Math.max(0, Math.round(remainingSec / 60));
}

/** Rough arrival clock from the time already spent on the share already covered. */
function progressEta(legStart: string | null | undefined, percent: number | null, now: Date): string | null {
  const mins = etaMinutesLeft(legStart, percent, now);
  if (mins == null) return null;
  const eta = new Date(now.getTime() + mins * 60 * 1000);
  const clock = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Pacific/Auckland',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(eta);
  const day = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Pacific/Auckland',
    day: '2-digit',
    month: '2-digit',
  }).format(eta);
  const today = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Pacific/Auckland',
    day: '2-digit',
    month: '2-digit',
  }).format(now);
  return day === today ? clock : `${day} ${clock}`;
}

function textOf(value: string | null | undefined): string {
  const s = value == null ? '' : String(value).trim();
  return s || '—';
}

function distinct(rows: LiveDriverJob[], key: 'driver' | 'winery_name' | 'vineyard_name'): string[] {
  const seen = new Set<string>();
  for (const row of rows) {
    const value = row[key]?.trim();
    if (value) seen.add(value);
  }
  return [...seen].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

function metersOrInfinity(meters: number | null): number {
  return meters == null ? Number.POSITIVE_INFINITY : meters;
}

/** Coming-in trucks first, then at the winery, at the vineyard, then outbound. */
function stageRank(done: number): number {
  if (done === 3) return 0;
  if (done === 4) return 1;
  if (done === 2) return 2;
  if (done === 1) return 3;
  return 4;
}

function compareJobs(a: LiveDriverJob, b: LiveDriverJob): number {
  const doneA = completedGateCount(a);
  const doneB = completedGateCount(b);
  const liveA = isLiveJob(a);
  const liveB = isLiveJob(b);
  if (liveA !== liveB) return liveA ? -1 : 1;
  if (!liveA) {
    const endedA = a.step_5_click?.trim() || '';
    const endedB = b.step_5_click?.trim() || '';
    if (endedA !== endedB) return endedB.localeCompare(endedA);
    return a.job_id.localeCompare(b.job_id, undefined, { sensitivity: 'base' });
  }
  const rank = stageRank(doneA) - stageRank(doneB);
  if (rank !== 0) return rank;
  if (doneA === 3) {
    const delta = metersOrInfinity(a.place.wineryMeters) - metersOrInfinity(b.place.wineryMeters);
    if (delta !== 0) return delta;
  }
  if (doneA === 1) {
    const delta = metersOrInfinity(a.place.vineyardMeters) - metersOrInfinity(b.place.vineyardMeters);
    if (delta !== 0) return delta;
  }
  return a.job_id.localeCompare(b.job_id, undefined, { sensitivity: 'base' });
}

function manualLabels(job: DriverJob): string[] {
  return DRIVER_GATES.filter((gate) => gateManual(job, gate.step)).map((gate) => `step ${gate.step} ${gate.title}`);
}

function checkpointText(
  step: 2 | 3 | 4 | null,
  alert: 'yellow' | 'red' | null,
  crossedAt: string | null,
  remind: boolean,
): string | null {
  if (step == null || alert == null) return null;
  if (alert === 'yellow') return `Soft alert · close to the ${step === 4 ? 'winery' : 'vineyard'}`;
  if (step === 3 && remind) return 'Hard alert · heading to the winery';
  if (step === 3) {
    const clock = gateClock(crossedAt);
    return clock === '--:--' ? 'Hard alert · left the vineyard' : `Hard alert · left the vineyard · ${clock}`;
  }
  return step === 4 ? 'Hard alert · inside the winery' : 'Hard alert · inside the vineyard';
}

function travelMeters(job: DriverJob, place: LivePlace): TruckMeters | null {
  const done = completedGateCount(job);
  if (done === 1) return { meters: place.vineyardMeters, inside: place.vineyardInside };
  if (done === 3) return { meters: place.wineryMeters, inside: place.wineryInside };
  return null;
}

function expectedLabel(value: string | null): string {
  if (value === '60+') return 'more than 60 min';
  if (value === '15' || value === '30' || value === '45' || value === '60') return `${value} min`;
  return '—';
}

function formatClock(seconds: number): string {
  const s = Math.max(0, seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const pad = (n: number) => String(n).padStart(2, '0');
  if (h > 0) return `${h}:${pad(m)}:${pad(sec)}`;
  return `${pad(m)}:${pad(sec)}`;
}

function JobRow({ job }: { job: LiveDriverJob }) {
  const done = completedGateCount(job);
  const stage = truckStage(done);
  const manual = manualLabels(job);
  const finished = Boolean(job.step_5_click?.trim());
  const status = finished ? 'Finished' : jobMode(done) ?? (stage ? stage.label.replace('\n', ' ') : done === 0 ? 'Not started' : 'Open');
  const [openDetail, setOpenDetail] = useState(false);
  const [logOpen, setLogOpen] = useState(false);
  const [elapsed, setElapsed] = useState('00:00');
  const [nowMs, setNowMs] = useState(() => Date.now());
  const running = job.diversion;
  const now = new Date(nowMs);
  const travelling = done === 1 || done === 3;
  const remaining = done === 1 ? job.place.vineyardMeters : done === 3 ? job.place.wineryMeters : null;
  const total = done === 1 ? job.place.outMeters : done === 3 ? job.place.backMeters : null;
  const share = travelling ? journeyShare(total, remaining) : null;
  const legStart = done === 1 ? job.step_1_click : done === 2 ? job.step_2_click : done === 3 ? job.step_3_click : done === 4 ? job.step_4_click : null;
  const travelledMin = minuteLabel(locationAgeSeconds(legStart, now));
  const etaMins = travelling ? etaMinutesLeft(legStart, share, now) : null;
  const liveEta = travelling ? progressEta(legStart, share, now) : null;
  const fallbackEta = !liveEta && done === 3 ? wineryArrivalEta(job) : null;
  const travelled =
    total != null && remaining != null ? Math.min(total, Math.max(0, total - remaining)) : null;
  const place = done === 0 ? job.vineyard_name : done === 1 || done === 2 ? job.vineyard_name : job.winery_name;
  const leg = finished
    ? 'Step 5'
    : done === 1
      ? 'Step 1 → 2'
      : done === 2
        ? 'Step 2'
        : done === 3
          ? 'Step 3 → 4'
          : done === 4
            ? 'Step 4'
            : done === 0
              ? 'Waiting'
              : 'Step';
  const jobMinutes = finished ? minutesBetween(job.step_1_click, job.step_5_click) : null;
  const mapHref = job.place.lat != null && job.place.lng != null ? `https://www.google.com/maps?q=${job.place.lat},${job.place.lng}` : null;
  const imei = job.imei?.trim() ?? '';
  const truckId = job.truck_id?.trim() ?? '';
  const currentHref = imei
    ? `/api/current-locations/map?imei=${encodeURIComponent(imei)}`
    : truckId
      ? `/api/current-locations/map?truck=${encodeURIComponent(truckId)}`
      : null;
  const currentTitle = imei
    ? `Latest GPS fix for IMEI ${imei}. Opens in Google Maps, including before the job starts.`
    : truckId
      ? `This job has no IMEI. Opens the latest GPS fix for truck ${truckId} in Google Maps.`
      : 'This job has no IMEI';
  const alertText = checkpointText(
    job.place.checkpointStep,
    job.place.checkpointAlert,
    job.place.checkpointCrossedAt,
    job.place.checkpointRemind,
  );
  const checkpoint: CheckpointMark =
    job.place.checkpointStep == null
      ? null
      : { step: job.place.checkpointStep, alert: job.place.checkpointAlert };

  useEffect(() => {
    if (!running) return;
    const tick = () => setElapsed(formatClock(locationAgeSeconds(running.startedAt) ?? 0));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [running]);

  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <article className="overflow-hidden rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-1 px-3 py-2">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
            {textOf(job.truck_id)}
            <span className="ml-2 font-normal text-zinc-600 dark:text-zinc-300">{textOf(job.driver)}</span>
          </p>
          <div className="mt-0.5 text-xs text-zinc-600 dark:text-zinc-400">
            {job.company_name?.trim() ? (
              <div className="font-medium text-zinc-700 dark:text-zinc-200">{job.company_name}</div>
            ) : null}
            {job.group_name?.trim() ? <div>{job.group_name}</div> : null}
            <div>
              <span className="font-mono text-zinc-800 dark:text-zinc-200">{textOf(job.job_id)}</span>
              {' · '}
              {textOf(job.vineyard_name)} → {textOf(job.winery_name)}
            </div>
          </div>
        </div>
        <div className="text-right text-xs">
          <p className="flex items-center justify-end gap-3">
            <a
              href={`/driver/job/${job.id}?mode=watch`}
              target="_blank"
              rel="noopener noreferrer"
              className="font-semibold text-[#175cd3] hover:underline"
              title="Open a read-only view of this driver's buttons and fence alerts"
            >
              Watch
            </a>
            <a
              href={`/driver/job/${job.id}?mode=control`}
              target="_blank"
              rel="noopener noreferrer"
              className="font-semibold text-[#b54708] hover:underline"
              title="Open this job and act for the driver. Your name is stored in the job log."
            >
              Control
            </a>
            <button type="button" onClick={() => setLogOpen(true)} className="font-semibold text-[#175cd3] hover:underline">
              Job log
            </button>
            {mapHref ? (
              <a href={mapHref} target="_blank" rel="noopener noreferrer" className="font-semibold text-[#175cd3] hover:underline">
                Map
              </a>
            ) : (
              <span className="text-zinc-400">Map</span>
            )}
            {currentHref ? (
              <a
                href={currentHref}
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-[#175cd3] hover:underline"
                title={currentTitle}
              >
                Current location
              </a>
            ) : (
              <span className="text-zinc-400" title={currentTitle}>
                Current location
              </span>
            )}
          </p>
          {liveEta && (
            <p className="mt-1 font-semibold text-[#1f9d55]" title="Rough estimate from the share of the journey already covered">
              {done === 1 ? 'Vineyard' : 'Winery'} ETA {liveEta}
            </p>
          )}
          {fallbackEta && <p className="mt-1 font-semibold text-[#1f9d55]">Winery ETA {fallbackEta}</p>}
          <p className={manual.length ? 'font-semibold text-[#b54708]' : 'text-zinc-500 dark:text-zinc-400'}>
            {manual.length ? `Manual override: ${manual.join(', ')}` : 'No manual override'}
          </p>
          <p className="tabular-nums text-zinc-700 dark:text-zinc-200">
            {job.place.gpstimeNz ? `Last GPS point ${job.place.gpstimeNz}` : 'No GPS fix'}
          </p>
          {running && (
            <button
              type="button"
              onClick={() => setOpenDetail((value) => !value)}
              className="mt-1 rounded-full bg-[#b54708] px-2.5 py-1 text-left text-[11px] font-semibold text-white"
            >
              Diversion · {running.typeName} · {elapsed}
            </button>
          )}
        </div>
      </div>
      {running && openDetail && (
        <div className="border-t border-zinc-200 px-3 py-2 text-xs text-zinc-700 dark:border-zinc-700 dark:text-zinc-200">
          <p>Running for {elapsed}. Expected {expectedLabel(running.expectedDuration)}.</p>
          <p className="mt-1 tabular-nums">Started {running.startedAt}</p>
          {running.note ? <p className="mt-1">{running.note}</p> : <p className="mt-1 text-zinc-500">No note.</p>}
        </div>
      )}
      <div className="flex items-stretch bg-[#f3f5f7]">
        <div className="flex w-[14.5rem] shrink-0 flex-col justify-center gap-1 border-r border-[#e4e7ec] px-3 py-3">
          <p className="text-left text-xs font-semibold text-[#175cd3]">{status}</p>
          <p className="text-xs font-semibold text-zinc-800">{leg}</p>
          <p className="truncate text-[11px] text-zinc-600" title={place ?? ''}>{textOf(place)}</p>
          {travelling ? (
            <>
              <p className="text-[11px] tabular-nums text-zinc-800">
                {travelled != null && total != null
                  ? `${travelled.toLocaleString('en-NZ')} of ${total.toLocaleString('en-NZ')} m`
                  : 'Metres not available'}
              </p>
              <p className="text-[11px] text-zinc-600">{travelledMin ? `Travelling ${travelledMin}` : 'Travelling —'}</p>
              <p className="text-[11px] font-semibold text-[#1f9d55]" title="Rough estimate from the share of the journey already covered">
                {etaMins != null ? `${etaMins === 1 ? '1 min' : `${etaMins} min`} to ETA` : 'ETA —'}
              </p>
            </>
          ) : finished ? (
            <>
              <p className="text-[11px] tabular-nums text-zinc-800">Ended {gateClock(job.step_5_click)}</p>
              <p className="text-[11px] text-zinc-600">
                {jobMinutes != null ? `${jobMinutes === 1 ? '1 min' : `${jobMinutes} min`} from start to end` : 'Start to end —'}
              </p>
            </>
          ) : (
            <p className="text-[11px] text-zinc-600">
              {done === 0 ? 'Not started' : done === 2 ? 'Loading' : done === 4 ? 'Unloading' : 'On this step'}{' '}
              {done === 0 ? '' : (travelledMin ?? '—')}
            </p>
          )}
          <p className="text-[11px] tabular-nums text-zinc-700">
            {job.place.gpstimeNz ? `Last GPS ${job.place.gpstimeNz}` : 'No GPS fix'}
          </p>
          {alertText ? (
            <p className={`text-[11px] font-semibold ${job.place.checkpointAlert === 'red' ? 'text-[#b42318]' : 'text-[#b54708]'}`}>
              {alertText}
            </p>
          ) : null}
        </div>
        <div className="min-w-0 flex-1">
      <GateTrack
        live
        checkpoint={checkpoint}
        job={job}
        travelMeters={travelMeters(job, job.place)}
        placeNames={{ vineyard: job.vineyard_name, winery: job.winery_name }}
        journey={{
          outMeters: done >= 1 ? job.place.outMeters : null,
          backMeters: job.place.backMeters,
          active: done === 1 ? 'out' : done === 3 ? 'back' : null,
          remaining,
          eta: liveEta,
        }}
        ghost={job.place.ghost}
      />
        </div>
      </div>
      {logOpen && (
        <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/45 p-4" role="dialog" aria-modal="true">
          <div className="flex max-h-[80vh] w-full max-w-lg flex-col overflow-hidden rounded-2xl bg-white shadow-xl">
            <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3">
              <p className="text-sm font-semibold text-zinc-900">Job log · {textOf(job.job_id)}</p>
              <button type="button" onClick={() => setLogOpen(false)} className="text-sm font-semibold text-[#175cd3]">
                Close
              </button>
            </div>
            <div className="min-h-0 flex-1 overflow-auto bg-[#f4f7f8]">
              <DriverJobLog jobId={job.id} />
            </div>
          </div>
        </div>
      )}
    </article>
  );
}

export default function RealTimeJobs() {
  const [rows, setRows] = useState<LiveDriverJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState(POLL_MS);
  const [filterDriver, setFilterDriver] = useState('');
  const [filterWinery, setFilterWinery] = useState('');
  const [filterVineyard, setFilterVineyard] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [onlyLive, setOnlyLive] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [nextRefreshAt, setNextRefreshAt] = useState<number | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    let cancelled = false;
    setNextRefreshAt(Date.now() + pollMs);

    async function load() {
      setRefreshing(true);
      try {
        const res = await fetch('/api/driver-jobs/live', { cache: 'no-store' });
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        if (cancelled) return;
        setRows(Array.isArray(data?.rows) ? (data.rows as LiveDriverJob[]) : []);
        setError(null);
        const seconds = data?.driverAppPollSeconds;
        if (typeof seconds === 'number' && seconds >= 5) setPollMs(seconds * 1000);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) {
          setRefreshing(false);
          setLoading(false);
        }
      }
    }

    void load();
    const timer = setInterval(() => {
      setNextRefreshAt(Date.now() + pollMs);
      void load();
    }, pollMs);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [pollMs, refreshNonce]);

  const pollSeconds = Math.max(1, Math.round(pollMs / 1000));
  const secondsLeft =
    nextRefreshAt == null ? null : Math.min(pollSeconds, Math.max(0, Math.ceil((nextRefreshAt - nowMs) / 1000)));
  const refreshLabel =
    loading && nextRefreshAt == null ? 'Loading…' : secondsLeft == null ? '…' : secondsLeft <= 0 ? 'Refreshing…' : `Next refresh in ${secondsLeft}s`;

  const drivers = useMemo(() => distinct(rows, 'driver'), [rows]);
  const wineries = useMemo(() => distinct(rows, 'winery_name'), [rows]);
  const vineyards = useMemo(() => distinct(rows, 'vineyard_name'), [rows]);

  const visible = useMemo(() => {
    return rows
      .filter((row) => {
        if (filterDriver && row.driver?.trim() !== filterDriver) return false;
        if (filterWinery && row.winery_name?.trim() !== filterWinery) return false;
        if (filterVineyard && row.vineyard_name?.trim() !== filterVineyard) return false;
        if (filterStatus && jobMode(completedGateCount(row)) !== filterStatus) return false;
        if (onlyLive && !isLiveJob(row)) return false;
        return true;
      })
      .sort(compareJobs);
  }, [rows, filterDriver, filterWinery, filterVineyard, filterStatus, onlyLive]);

  return (
    <div>
      <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
        Every job stays on this list, including ones that have finished step 5, so the log is still here for a debrief.
        Turn on Only live jobs to hide finished jobs and jobs that have not started. The long gaps are the roads to and from the vineyard.
        Steps 2 and 3 sit inside the vineyard wheel, with a clock for how long the truck has been there.
        Steps 4 and 5 sit inside the winery wheel.
        On the road to the vineyard, the truck steps along in 20% increments of the gap from its location at job start to the vineyard fence. On the road back, it uses the gap between the vineyard fence and the winery fence. Soft and hard marks match the alert on the driver button.
        A recorded tap is Hard inside the fence, Soft inside the soft band, and Manual only when the truck is outside the soft fence as well.
        The clock in the circle is the button tap. The clock under Soft or Hard is the GPS time for that fence. Hover the gate to see the soft time, the hard time, and the gap.
        Step 1 shows the day and date the job started. Each row shows the last GPS point received for that truck. Once the job has finished, that time stops at the end of the job, so a later ping on the same truck does not move it. Current location opens the latest stored fix for that IMEI in a new Google Maps tab, including when the job has not started. The ring at the top right counts down to the next refresh. Refresh now loads the list immediately.
        Filter by mode together with a vineyard or winery to see what is about to arrive. Live trucks are listed first, then finished jobs with the latest ending time first.
      </p>

      <div className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Filters</h2>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Driver</label>
            <select value={filterDriver} onChange={(e) => setFilterDriver(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {drivers.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Winery</label>
            <select value={filterWinery} onChange={(e) => setFilterWinery(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {wineries.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Mode</label>
            <select value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {MODES.map((mode) => (
                <option key={mode.id} value={mode.title}>
                  {mode.title}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Vineyard</label>
            <select value={filterVineyard} onChange={(e) => setFilterVineyard(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {vineyards.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <label className="flex items-center gap-2 pb-1 text-xs font-medium text-zinc-700 dark:text-zinc-200">
            <input
              type="checkbox"
              checked={onlyLive}
              onChange={(e) => setOnlyLive(e.target.checked)}
              className="h-4 w-4 rounded border-zinc-300"
            />
            Only live jobs
          </label>
        </div>
      </div>

      <div className="sticky top-0 z-10 mb-3 flex items-center justify-between rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900">
        <span className="text-zinc-600 dark:text-zinc-300">
          {loading
            ? 'Loading jobs…'
            : `${visible.length} ${onlyLive ? 'live ' : ''}job${visible.length === 1 ? '' : 's'}`}
        </span>
        <RefreshClock
          secondsLeft={secondsLeft}
          pollSeconds={pollSeconds}
          refreshing={refreshing}
          onRefresh={() => setRefreshNonce((nonce) => nonce + 1)}
        />
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}

      {loading ? (
        <p className="text-zinc-600 dark:text-zinc-400">Loading jobs…</p>
      ) : visible.length === 0 ? (
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          {onlyLive
            ? 'No live jobs. A live job has started and has not finished step 5. Turn off Only live jobs to see finished jobs.'
            : 'No jobs.'}
        </p>
      ) : (
        <div className="flex flex-col gap-3">
          {visible.map((job) => (
            <JobRow key={job.id} job={job} />
          ))}
        </div>
      )}

      <p className="mt-3 text-xs text-zinc-500 dark:text-zinc-400">
        {loading ? '…' : `${visible.length} ${onlyLive ? 'live ' : ''}job${visible.length === 1 ? '' : 's'}`}
        {` · ${refreshLabel}`}
      </p>
    </div>
  );
}
