'use client';

import { useEffect, useState } from 'react';

const SETTING_TYPE = 'System';
const GPS_START_BUFFER_NAME = 'GPSstartbuffer';
const INSPECT_START_LESS_NAME = 'InspectStartLess';
const INSPECT_END_PLUS_NAME = 'InspectEndPlus';

export default function SettingsPage() {
  const [gpsStartBuffer, setGpsStartBuffer] = useState<string>('');
  const [inspectStartLess, setInspectStartLess] = useState<string>('');
  const [inspectEndPlus, setInspectEndPlus] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');

  useEffect(() => {
    const type = SETTING_TYPE;
    Promise.all([
      fetch(`/api/settings?${new URLSearchParams({ type, name: GPS_START_BUFFER_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type, name: INSPECT_START_LESS_NAME })}`).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type, name: INSPECT_END_PLUS_NAME })}`).then((r) => r.json()),
    ])
      .then(([data1, data2, data3]) => {
        const v1 = data1?.settingvalue;
        setGpsStartBuffer(v1 != null && v1 !== '' ? String(v1) : '15');
        const v2 = data2?.settingvalue;
        setInspectStartLess(v2 != null && v2 !== '' ? String(v2) : '10');
        const v3 = data3?.settingvalue;
        setInspectEndPlus(v3 != null && v3 !== '' ? String(v3) : '60');
      })
      .catch(() => {
        setGpsStartBuffer('15');
        setInspectStartLess('10');
        setInspectEndPlus('60');
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
        </div>
      )}
    </div>
  );
}
