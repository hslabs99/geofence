'use client';

import { useEffect, useState } from 'react';
import {
  GPS_FENCE_SETTINGS_TYPE,
  GPS_STD_TIME_NAME,
  GPSPLUS_GROWTH_NAME,
  GPSPLUS_TIME_NAME,
} from '@/lib/gps-fence-settings-names';
import {
  VWORK_TT_LOAD_SIZE_DEFAULT,
  VWORK_TT_LOAD_SIZE_SETTING_NAME,
  VWORK_TT_LOAD_SIZE_SETTINGS_TYPE,
} from '@/lib/vwork-tt-load-size-setting-names';

const SETTING_TYPE = 'System';
const GPS_START_BUFFER_NAME = 'GPSstartbuffer';
const INSPECT_START_LESS_NAME = 'InspectStartLess';
const INSPECT_END_PLUS_NAME = 'InspectEndPlus';

export default function SettingsPage() {
  const [gpsStartBuffer, setGpsStartBuffer] = useState<string>('');
  const [inspectStartLess, setInspectStartLess] = useState<string>('');
  const [inspectEndPlus, setInspectEndPlus] = useState<string>('');
  const [gpsStdTime, setGpsStdTime] = useState<string>('');
  const [gpsplusGrowth, setGpsplusGrowth] = useState<string>('');
  const [gpsplusTime, setGpsplusTime] = useState<string>('');
  const [ttLoadSize, setTtLoadSize] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');

  useEffect(() => {
    const type = SETTING_TYPE;
    const fenceType = GPS_FENCE_SETTINGS_TYPE;
    Promise.all([
      fetch(`/api/settings?${new URLSearchParams({ type, name: GPS_START_BUFFER_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type, name: INSPECT_START_LESS_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type, name: INSPECT_END_PLUS_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type: fenceType, name: GPS_STD_TIME_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type: fenceType, name: GPSPLUS_GROWTH_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type: fenceType, name: GPSPLUS_TIME_NAME })}`).then((r) => r.json()),
      fetch(
        `/api/settings?${new URLSearchParams({ type: VWORK_TT_LOAD_SIZE_SETTINGS_TYPE, name: VWORK_TT_LOAD_SIZE_SETTING_NAME })}`
      ).then((r) => r.json()),
    ])
      .then(([data1, data2, data3, dataStd, data4, data5, dataTtLoad]) => {
        const v1 = data1?.settingvalue;
        setGpsStartBuffer(v1 != null && v1 !== '' ? String(v1) : '15');
        const v2 = data2?.settingvalue;
        setInspectStartLess(v2 != null && v2 !== '' ? String(v2) : '10');
        const v3 = data3?.settingvalue;
        setInspectEndPlus(v3 != null && v3 !== '' ? String(v3) : '60');
        const vStd = dataStd?.settingvalue;
        setGpsStdTime(vStd != null && String(vStd).trim() !== '' ? String(vStd).trim() : '');
        const v4 = data4?.settingvalue;
        setGpsplusGrowth(v4 != null && v4 !== '' ? String(v4) : '10');
        const v5 = data5?.settingvalue;
        setGpsplusTime(v5 != null && String(v5).trim() !== '' ? String(v5).trim() : '');
        const vTt = dataTtLoad?.settingvalue;
        setTtLoadSize(
          vTt != null && String(vTt).trim() !== ''
            ? String(vTt).trim()
            : String(VWORK_TT_LOAD_SIZE_DEFAULT)
        );
      })
      .catch(() => {
        setGpsStartBuffer('15');
        setInspectStartLess('10');
        setInspectEndPlus('60');
        setGpsStdTime('');
        setGpsplusGrowth('10');
        setGpsplusTime('');
        setTtLoadSize(String(VWORK_TT_LOAD_SIZE_DEFAULT));
      })
      .finally(() => setLoading(false));
  }, []);

  const saveGpsStartBuffer = () => {
    const minutes = Math.max(0, parseInt(gpsStartBuffer, 10));
    if (Number.isNaN(minutes)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: SETTING_TYPE,
        settingname: GPS_START_BUFFER_NAME,
        settingvalue: String(minutes),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setGpsStartBuffer(String(minutes));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveInspectStartLess = () => {
    const minutes = Math.max(0, parseInt(inspectStartLess, 10));
    if (Number.isNaN(minutes)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: SETTING_TYPE,
        settingname: INSPECT_START_LESS_NAME,
        settingvalue: String(minutes),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setInspectStartLess(String(minutes));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveInspectEndPlus = () => {
    const minutes = Math.max(0, parseInt(inspectEndPlus, 10));
    if (Number.isNaN(minutes)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: SETTING_TYPE,
        settingname: INSPECT_END_PLUS_NAME,
        settingvalue: String(minutes),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setInspectEndPlus(String(minutes));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveGpsStdTime = () => {
    const sec = Math.max(0, parseInt(gpsStdTime, 10));
    if (Number.isNaN(sec)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: GPS_FENCE_SETTINGS_TYPE,
        settingname: GPS_STD_TIME_NAME,
        settingvalue: String(sec),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setGpsStdTime(String(sec));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveGpsplusGrowth = () => {
    const meters = Math.max(0, parseInt(gpsplusGrowth, 10));
    if (Number.isNaN(meters)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: GPS_FENCE_SETTINGS_TYPE,
        settingname: GPSPLUS_GROWTH_NAME,
        settingvalue: String(meters),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setGpsplusGrowth(String(meters));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveTtLoadSize = () => {
    const n = Number.parseFloat(String(ttLoadSize).replace(',', '.'));
    if (!Number.isFinite(n) || n < 0) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: VWORK_TT_LOAD_SIZE_SETTINGS_TYPE,
        settingname: VWORK_TT_LOAD_SIZE_SETTING_NAME,
        settingvalue: String(n),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setTtLoadSize(String(n));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  const saveGpsplusTime = () => {
    const sec = Math.max(0, parseInt(gpsplusTime, 10));
    if (Number.isNaN(sec)) return;
    setSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: GPS_FENCE_SETTINGS_TYPE,
        settingname: GPSPLUS_TIME_NAME,
        settingvalue: String(sec),
      }),
    })
      .then((r) => {
        if (r.ok) {
          setSaveStatus('saved');
          setGpsplusTime(String(sec));
        } else setSaveStatus('error');
      })
      .catch(() => setSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSaveStatus('idle'), 2000);
      });
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Settings</h1>
      {loading && <p className="text-zinc-500">Loading…</p>}
      {!loading && (
        <div className="space-y-6 max-w-md">
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">GPS start buffer (minutes)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Tracking query shows data from actual_start_time minus this many minutes (e.g. 15 = 15 min before job start).
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={1440}
                  value={gpsStartBuffer}
                  onChange={(e) => setGpsStartBuffer(e.target.value)}
                  onBlur={saveGpsStartBuffer}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">minutes</span>
                <button
                  type="button"
                  onClick={saveGpsStartBuffer}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Inspect start less (minutes)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Inspect window: start_time minus this many minutes (e.g. 10 = query from 10 min before actual start).
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={1440}
                  value={inspectStartLess}
                  onChange={(e) => setInspectStartLess(e.target.value)}
                  onBlur={saveInspectStartLess}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">minutes</span>
                <button
                  type="button"
                  onClick={saveInspectStartLess}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Inspect end plus (minutes)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Inspect window: end_time plus this many minutes (e.g. 60 = query up to 60 min after actual end).
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={1440}
                  value={inspectEndPlus}
                  onChange={(e) => setInspectEndPlus(e.target.value)}
                  onBlur={saveInspectEndPlus}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">minutes</span>
                <button
                  type="button"
                  onClick={saveInspectEndPlus}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">GPS Std time (seconds)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Entry/Exit Tagging: minimum seconds a fence or null state must persist before writing ENTER/EXIT to tbl_tracking.
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={86400}
                  value={gpsStdTime}
                  onChange={(e) => setGpsStdTime(e.target.value)}
                  onBlur={saveGpsStdTime}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">seconds</span>
                <button
                  type="button"
                  onClick={saveGpsStdTime}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">GPS+ growth (meters)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Steps+ (GPS+): expand vineyard fence polygons by this distance when standard step 2/3 GPS is missing (PostGIS buffer).
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={500}
                  value={gpsplusGrowth}
                  onChange={(e) => setGpsplusGrowth(e.target.value)}
                  onBlur={saveGpsplusGrowth}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">meters</span>
                <button
                  type="button"
                  onClick={saveGpsplusGrowth}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">TT Load Size</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Data fix “Trailermode from load size” only updates rows with <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">loadsize</code> not null and greater than zero; it sets{' '}
                <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">trailermode</code> to <strong>TT</strong> when loadsize is greater than this value, and{' '}
                <strong>T</strong> when loadsize is less than or equal to it. Run “Set Trailer Type” first for other rows. Stored in tbl_settings (
                <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">{VWORK_TT_LOAD_SIZE_SETTING_NAME}</code>
                ).
              </p>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <input
                  type="number"
                  min={0}
                  step="0.1"
                  value={ttLoadSize}
                  onChange={(e) => setTtLoadSize(e.target.value)}
                  onBlur={saveTtLoadSize}
                  className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">(numeric, e.g. 25.5)</span>
                <button
                  type="button"
                  onClick={saveTtLoadSize}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <label className="block">
              <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">GPS+ time (seconds)</span>
              <p className="mt-0.5 text-xs text-zinc-500">
                Steps+ (GPS+): minimum stay duration inside the buffered fence to count as a valid segment (lower catches shorter vineyard visits).
              </p>
              <div className="mt-2 flex items-center gap-2">
                <input
                  type="number"
                  min={0}
                  max={86400}
                  value={gpsplusTime}
                  onChange={(e) => setGpsplusTime(e.target.value)}
                  onBlur={saveGpsplusTime}
                  className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
                <span className="text-sm text-zinc-500">seconds</span>
                <button
                  type="button"
                  onClick={saveGpsplusTime}
                  disabled={saveStatus === 'saving'}
                  className="rounded bg-zinc-200 px-3 py-2 text-sm font-medium hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:hover:bg-zinc-600"
                >
                  {saveStatus === 'saving' ? 'Saving…' : saveStatus === 'saved' ? 'Saved' : saveStatus === 'error' ? 'Save failed' : 'Save'}
                </button>
              </div>
            </label>
          </div>
        </div>
      )}
    </div>
  );
}
