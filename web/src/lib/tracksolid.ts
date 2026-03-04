import { createHash } from 'crypto';
import { join } from 'path';
import { loadEnvConfig } from '@next/env';
import { dateToLiteralUTC } from './utils';
import { query, execute } from './db';

const projectDir = process.cwd();
loadEnvConfig(projectDir);
if (!process.env.TRACKSOLID_APP_KEY) {
  loadEnvConfig(join(projectDir, 'web'));
}

export const TRACKSOLID_ENDPOINTS = {
  global: 'http://open.10000track.com/route/rest',
  hk: 'https://hk-open.tracksolidpro.com/route/rest',
  eu: 'https://eu-open.tracksolidpro.com/route/rest',
  us: 'https://us-open.tracksolidpro.com/route/rest',
} as const;

export type TracksolidEndpointKey = keyof typeof TRACKSOLID_ENDPOINTS;

function getBaseUrl(override?: string | null): string {
  const key = override?.toLowerCase().trim();
  if (key && key in TRACKSOLID_ENDPOINTS) {
    return TRACKSOLID_ENDPOINTS[key as TracksolidEndpointKey];
  }
  if (override?.startsWith('http')) {
    return override;
  }
  return process.env.TRACKSOLID_BASE_URL?.trim() || TRACKSOLID_ENDPOINTS.global;
}

const TOKEN_SETTINGS_TYPE = 'TracksolidToken';

/** Same key for same baseUrl so all processes share one token per endpoint. */
function getTokenCacheKey(baseUrl: string): string {
  const entries = Object.entries(TRACKSOLID_ENDPOINTS) as [string, string][];
  const found = entries.find(([, url]) => url === baseUrl);
  return found ? found[0] : 'default';
}

const FULL_REDACT_KEYS = new Set(['sign', 'user_pwd_md5', 'access_token']);
const PARTIAL_REDACT_KEYS = new Set(['app_key', 'app_secret']);

export interface TracksolidDebug {
  endpoint: string;
  method: string;
  requestParamsRedacted: Record<string, string>;
  requestBodyLength: number;
  httpStatus: number;
  responseBody: string;
  timestamp: string;
  /** Full parsed response (code, message, result) – show everything. */
  fullResponse?: unknown;
  /** Device name for display (e.g. in step 3 debug; not sent to API). */
  requestedDeviceName?: string;
}

export class TracksolidApiError extends Error {
  constructor(
    message: string,
    public debug: TracksolidDebug
  ) {
    super(message);
    this.name = 'TracksolidApiError';
  }
}

function redactParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    const val = v ?? '';
    if (FULL_REDACT_KEYS.has(k) || k === 'app_secret') {
      out[k] = val ? `${val.slice(0, 4)}***` : '(empty)';
    } else if (PARTIAL_REDACT_KEYS.has(k)) {
      out[k] = val ? `${val.slice(0, 8)}*** (len=${val.length})` : '(empty)';
    } else {
      out[k] = val;
    }
  }
  return out;
}

export interface TracksolidConfig {
  appKey: string;
  appSecret: string;
  account: string;
  passwordMd5: string;
}

function getConfig(): TracksolidConfig {
  const appKey = process.env.TRACKSOLID_APP_KEY?.trim() ?? '';
  const appSecret = process.env.TRACKSOLID_APP_SECRET?.trim() ?? '';
  const account = process.env.TRACKSOLID_ACCOUNT?.trim() ?? '';
  const passwordMd5 = process.env.TRACKSOLID_PASSWORD_MD5?.trim() ?? '';
  const missing: string[] = [];
  if (!appKey) missing.push('TRACKSOLID_APP_KEY');
  if (!appSecret) missing.push('TRACKSOLID_APP_SECRET');
  if (!account) missing.push('TRACKSOLID_ACCOUNT');
  if (!passwordMd5) missing.push('TRACKSOLID_PASSWORD_MD5');
  if (missing.length) {
    throw new Error(
      `Missing Tracksolid env (add to web/.env.local and restart dev server): ${missing.join(', ')}`
    );
  }
  return { appKey, appSecret, account, passwordMd5 };
}

export function signParams(params: Record<string, string>, appSecret: string): string {
  const sorted = Object.keys(params)
    .filter((k) => k !== 'sign')
    .sort();
  const concat = sorted
    .filter((k) => (params[k] ?? '') !== '')
    .map((k) => k + (params[k] ?? ''))
    .join('');
  const toSign = appSecret + concat + appSecret;
  return createHash('md5').update(toSign, 'utf8').digest('hex').toUpperCase();
}

function utcTimestamp(): string {
  return dateToLiteralUTC(new Date());
}

function commonParams(config: TracksolidConfig): Record<string, string> {
  return {
    method: '',
    timestamp: utcTimestamp(),
    app_key: config.appKey,
    sign_method: 'md5',
    v: '1.0',
    format: 'json',
  };
}

function buildDebug(
  baseUrl: string,
  method: string,
  formParams: Record<string, string>,
  bodyLength: number,
  httpStatus: number,
  raw: string,
  fullResponse: unknown
): TracksolidDebug {
  return {
    endpoint: baseUrl,
    method,
    requestParamsRedacted: redactParams(formParams),
    requestBodyLength: bodyLength,
    httpStatus,
    responseBody: raw,
    timestamp: utcTimestamp(),
    fullResponse,
  };
}

async function post(
  params: Record<string, string>,
  config: TracksolidConfig,
  context: { method: string },
  baseUrl: string
): Promise<{ code: number; message?: string; result?: unknown; _raw: string; _status: number; _debug: TracksolidDebug }> {
  const sign = signParams(params, config.appSecret);
  const formParams: Record<string, string> = { ...params, sign };
  const body = new URLSearchParams(formParams).toString();
  const res = await fetch(baseUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  const raw = await res.text();
  let data: { code?: number; message?: string; result?: unknown };
  try {
    data = JSON.parse(raw) as { code?: number; message?: string; result?: unknown };
  } catch {
    data = { code: -1, message: 'Response was not JSON' };
  }
  const code = data.code ?? -1;
  const debug = buildDebug(baseUrl, context.method, formParams, body.length, res.status, raw, data);
  if (code !== 0) {
    throw new TracksolidApiError(
      `Tracksolid ${context.method}: code=${code} message=${data.message ?? 'unknown'}`,
      debug
    );
  }
  return {
    code,
    message: data.message,
    result: data.result,
    _raw: raw,
    _status: res.status,
    _debug: debug,
  };
}

const TOKEN_EXPIRES_IN_SEC = 500;
const TOKEN_REUSE_BUFFER_SEC = 60;

const tokenCache = new Map<string, { token: string; expiresAt: number }>();

/** Clear cached token so the next request gets a fresh one. Call when token is expired or you rotated credentials. */
export async function clearTokenCache(endpoint?: string | null): Promise<void> {
  if (endpoint != null && endpoint !== '') {
    const baseUrl = getBaseUrl(endpoint);
    tokenCache.delete(baseUrl);
    const key = getTokenCacheKey(baseUrl);
    try {
      await execute('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [TOKEN_SETTINGS_TYPE, key]);
    } catch {
      // ignore DB errors
    }
  } else {
    tokenCache.clear();
    try {
      await execute('DELETE FROM tbl_settings WHERE type = $1', [TOKEN_SETTINGS_TYPE]);
    } catch {
      // ignore
    }
  }
}

async function getTokenFromDb(cacheKey: string): Promise<{ token: string; expiresAt: number } | null> {
  try {
    const rows = await query<{ settingvalue: string | null }>(
      'SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1',
      [TOKEN_SETTINGS_TYPE, cacheKey]
    );
    const row = rows[0];
    if (!row?.settingvalue) return null;
    const data = JSON.parse(row.settingvalue) as { token?: string; expiresAt?: number };
    if (typeof data?.token === 'string' && typeof data?.expiresAt === 'number') return data as { token: string; expiresAt: number };
    return null;
  } catch {
    return null;
  }
}

async function setTokenInDb(cacheKey: string, token: string, expiresAt: number): Promise<void> {
  try {
    const value = JSON.stringify({ token, expiresAt });
    await execute(
      `INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)
       ON CONFLICT (type, settingname) DO UPDATE SET settingvalue = EXCLUDED.settingvalue`,
      [TOKEN_SETTINGS_TYPE, cacheKey, value]
    );
  } catch {
    // ignore DB errors; in-memory cache still works
  }
}

export async function getAccessToken(
  endpoint?: string | null,
  options?: { skipCache?: boolean }
): Promise<{ token: string; debug: TracksolidDebug }> {
  const baseUrl = getBaseUrl(endpoint);
  const cacheKey = getTokenCacheKey(baseUrl);
  const now = Date.now();
  const minExpires = now + TOKEN_REUSE_BUFFER_SEC * 1000;

  if (!options?.skipCache) {
    let cached = tokenCache.get(baseUrl);
    if (!cached) {
      const fromDb = await getTokenFromDb(cacheKey);
      cached = fromDb ?? undefined;
      if (cached) tokenCache.set(baseUrl, cached);
    }
    if (cached && cached.expiresAt > minExpires) {
      const debug: TracksolidDebug = buildDebug(
        baseUrl,
        'jimi.oauth.token.get',
        {},
        0,
        0,
        '(cached token used – no request sent)',
        { cached: true }
      );
      return { token: cached.token, debug };
    }
  }

  const config = getConfig();
  const params = {
    ...commonParams(config),
    method: 'jimi.oauth.token.get',
    user_id: config.account,
    user_pwd_md5: config.passwordMd5,
    expires_in: String(TOKEN_EXPIRES_IN_SEC),
  };
  const out = await post(params, config, { method: 'jimi.oauth.token.get' }, baseUrl);
  const result = out.result as Record<string, unknown> | undefined;
  const token =
    typeof result?.access_token === 'string'
      ? result.access_token
      : typeof result?.accessToken === 'string'
        ? result.accessToken
        : null;
  if (!token) {
    const debug: TracksolidDebug = buildDebug(
      baseUrl,
      'jimi.oauth.token.get',
      { ...params, sign: signParams(params, config.appSecret) },
      new URLSearchParams({ ...params, sign: signParams(params, config.appSecret) }).toString().length,
      out._status,
      out._raw,
      { code: out.code, message: out.message, result: out.result }
    );
    throw new TracksolidApiError('Tracksolid token: no access_token in result.', debug);
  }
  const serverExpiresIn =
    typeof result?.expiresIn === 'number'
      ? result.expiresIn
      : typeof result?.expires_in === 'number'
        ? result.expires_in
        : TOKEN_EXPIRES_IN_SEC;
  const expiresInSec = Math.min(Math.max(60, serverExpiresIn), 7200);
  const expiresAt = now + expiresInSec * 1000;
  tokenCache.set(baseUrl, { token, expiresAt });
  await setTokenInDb(cacheKey, token, expiresAt);
  return { token, debug: out._debug };
}

export interface DeviceItem {
  imei: string;
  deviceName?: string;
}

export async function listDevices(
  accessToken: string,
  endpoint?: string | null
): Promise<{ devices: DeviceItem[]; debug: TracksolidDebug }> {
  const config = getConfig();
  const baseUrl = getBaseUrl(endpoint);
  const params = {
    ...commonParams(config),
    method: 'jimi.user.device.list',
    access_token: accessToken,
    target: config.account,
  };
  const out = await post(params, config, { method: 'jimi.user.device.list' }, baseUrl);
  const raw = out.result;
  const list = Array.isArray(raw)
    ? (raw as Array<{ imei?: string; deviceName?: string }>)
    : (raw as { deviceList?: Array<{ imei?: string; deviceName?: string }> } | undefined)?.deviceList ?? [];
  return {
    devices: list.map((d) => ({ imei: d.imei ?? '', deviceName: d.deviceName })),
    debug: out._debug,
  };
}

export interface TrackPoint {
  lat: number;
  lng: number;
  gpsTime: string;
}

export async function getTrackForDevice(
  accessToken: string,
  imei: string,
  beginTime: string,
  endTime: string,
  endpoint?: string | null
): Promise<{ points: TrackPoint[]; debug: TracksolidDebug }> {
  const config = getConfig();
  const baseUrl = getBaseUrl(endpoint);
  const params: Record<string, string> = {
    ...commonParams(config),
    method: 'jimi.device.track.list',
    access_token: accessToken,
    imei,
    begin_time: beginTime,
    end_time: endTime,
  };
  const out = await post(params, config, { method: 'jimi.device.track.list' }, baseUrl);
  const list = (Array.isArray(out.result) ? out.result : []) as Array<{ lat?: number; lng?: number; gpsTime?: string }>;
  return {
    points: list.map((p) => ({
      lat: Number(p.lat) || 0,
      lng: Number(p.lng) || 0,
      gpsTime: String(p.gpsTime ?? ''),
    })),
    debug: out._debug,
  };
}

export function getYesterdayUTC(): { begin: string; end: string } {
  const y = new Date(Date.UTC(
    new Date().getUTCFullYear(),
    new Date().getUTCMonth(),
    new Date().getUTCDate() - 1
  ));
  const begin = dateToLiteralUTC(y);
  const endDate = new Date(Date.UTC(y.getUTCFullYear(), y.getUTCMonth(), y.getUTCDate(), 23, 59, 59));
  const end = dateToLiteralUTC(endDate);
  return { begin, end };
}

export function getLast24HoursUTC(): { begin: string; end: string } {
  const now = new Date();
  const end = dateToLiteralUTC(now);
  const beginDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const begin = dateToLiteralUTC(beginDate);
  return { begin, end };
}

/** Single day in UTC: 00:00:00 to 23:59:59. dateStr = YYYY-MM-DD. */
export function getDayRangeUTC(dateStr: string): { begin: string; end: string } {
  const parts = dateStr.trim().split(/[-/]/);
  const y = parseInt(parts[0], 10);
  const m = parseInt(parts[1], 10) - 1;
  const d = parseInt(parts[2], 10);
  if (Number.isNaN(y) || Number.isNaN(m) || Number.isNaN(d)) {
    throw new Error(`Invalid date: ${dateStr}. Use YYYY-MM-DD.`);
  }
  const start = new Date(Date.UTC(y, m, d, 0, 0, 0));
  const endDate = new Date(Date.UTC(y, m, d, 23, 59, 59));
  return {
    begin: dateToLiteralUTC(start),
    end: dateToLiteralUTC(endDate),
  };
}
