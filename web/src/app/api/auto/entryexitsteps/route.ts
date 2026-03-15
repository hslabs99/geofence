import { NextResponse } from 'next/server';
import { logAuto, getAutoThreeDayRangeUTC } from '@/lib/auto-log';
import { runFetchStepsForJobs } from '@/lib/fetch-steps';

const ENDPOINT = 'entryexitsteps';
const GRACE_SECONDS = 300;
const BUFFER_HOURS = 1;
const STEPS_START_LESS_MINUTES = 60;
const STEPS_END_PLUS_MINUTES = 60;

function getBaseUrl(): string {
  const u = process.env.APP_URL ?? process.env.VERCEL_URL;
  if (u) return u.startsWith('http') ? u : `https://${u}`;
  return 'http://localhost:3000';
}

export async function GET() {
  const base = getBaseUrl();
  try {
    const { dateFrom, dateTo, dates } = getAutoThreeDayRangeUTC();
    await logAuto(ENDPOINT, 'start', { dateFrom, dateTo, dates, stepsForce: false, startLessMinutes: STEPS_START_LESS_MINUTES, endPlusMinutes: STEPS_END_PLUS_MINUTES });

    const summary: { date: string; devices: number; tagOk: boolean; jobs: number; stepsOk: number; stepsError: number }[] = [];

    for (let i = 0; i < dates.length; i++) {
      const date = dates[i];
      await logAuto(ENDPOINT, 'day_start', { date, dayIndex: i + 1, totalDays: dates.length });

      const devRes = await fetch(`${base}/api/admin/tracking/devices-for-date?date=${encodeURIComponent(date)}`);
      const devData = await devRes.json();
      const dayDevices: string[] = devData?.ok && Array.isArray(devData.devices) ? devData.devices : [];

      await logAuto(ENDPOINT, 'devices', { date, count: dayDevices.length, devices: dayDevices });

      let tagOk = true;
      if (dayDevices.length > 0) {
        const tagRes = await fetch(`${base}/api/admin/tagging/run`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            dateFrom: date,
            dateTo: date,
            deviceNames: dayDevices,
            graceSeconds: GRACE_SECONDS,
            bufferHours: BUFFER_HOURS,
          }),
        });
        tagOk = tagRes.ok;
        if (!tagRes.ok) {
          const text = await tagRes.text();
          await logAuto(ENDPOINT, 'tag_error', { date, status: tagRes.status, body: text.slice(0, 500) });
        } else if (tagRes.body) {
          const reader = tagRes.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
          }
        }
      }

      const stepsRes = await fetch(`${base}/api/vworkjobs?date=${encodeURIComponent(date)}&stepsFetched=false`);
      const stepsData = await stepsRes.json();
      const jobs: Record<string, unknown>[] = Array.isArray(stepsData?.rows) ? stepsData.rows : [];

      await logAuto(ENDPOINT, 'steps_jobs', { date, jobCount: jobs.length });

      let stepsOk = 0;
      let stepsError = 0;
      if (jobs.length > 0) {
        const result = await runFetchStepsForJobs({
          jobs,
          startLessMinutes: STEPS_START_LESS_MINUTES,
          endPlusMinutes: STEPS_END_PLUS_MINUTES,
        });
        stepsOk = result.log.filter((e) => e.status === 'ok').length;
        stepsError = result.log.filter((e) => e.status === 'error').length;
        await logAuto(ENDPOINT, 'steps_result', { date, ok: stepsOk, error: stepsError, log: result.log });
      }

      summary.push({ date, devices: dayDevices.length, tagOk, jobs: jobs.length, stepsOk, stepsError });
    }

    await logAuto(ENDPOINT, 'done', { summary });
    return NextResponse.json({ ok: true, dateFrom, dateTo, summary });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logAuto(ENDPOINT, 'error', { message });
    console.error('[auto/entryexitsteps]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST() {
  return GET();
}
