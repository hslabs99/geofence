'use client';

import { useEffect, useState } from 'react';

/** Calendar numbers only, so two New Zealand wall clocks can be subtracted without a timezone shift. */
function wallMs(stamp: string | null | undefined): number | null {
  if (stamp == null) return null;
  const match = String(stamp).trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  return Date.UTC(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    Number(match[4]),
    Number(match[5]),
    Number(match[6]),
  );
}

function nzNowStamp(now: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  const offsetMin = -now.getTimezoneOffset();
  // This PC is already on New Zealand time (+12 standard, +13 daylight). Use that clock so the age matches the taskbar.
  if (offsetMin === 720 || offsetMin === 780) {
    return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  }
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Pacific/Auckland',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now);
  const pick = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? '00';
  return `${pick('year')}-${pick('month')}-${pick('day')} ${pick('hour')}:${pick('minute')}:${pick('second')}`;
}

/** Seconds since this New Zealand clock stamp. Display only; nothing is stored. */
export function locationAgeSeconds(nzStamp: string | null | undefined, now = new Date()): number | null {
  const then = wallMs(nzStamp);
  const current = wallMs(nzNowStamp(now));
  if (then == null || current == null) return null;
  return Math.max(0, Math.round((current - then) / 1000));
}

export function useLocationAge(nzStamp: string | null | undefined): number | null {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  if (!nzStamp?.trim()) return null;
  return locationAgeSeconds(nzStamp, new Date(now));
}
