'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  SUMMARY_COLUMN_COLOR_DEFAULTS,
  SUMMARY_COLUMN_COLOR_SETTING_NAMES,
  SUMMARY_COLUMN_COLOR_SETTINGS_TYPE,
  type SummaryColumnColorKey,
} from '@/lib/summary-column-color-setting-names';

type SaveStatus = 'idle' | 'saving' | 'saved' | 'error';

function normalizeHexColor(input: string): string {
  const s = String(input ?? '').trim();
  if (!s) return '';
  const m = s.match(/^#?([0-9a-fA-F]{6})$/);
  if (!m) return '';
  return `#${m[1]!.toUpperCase()}`;
}

function labelForKey(k: SummaryColumnColorKey): string {
  switch (k) {
    case 'step2':
      return '2. Travel To Vineyard';
    case 'step3':
      return '3. Time In Vineyard';
    case 'step4':
      return '4. Travel To Winery';
    case 'step5':
      return '5. Unloading Winery';
    case 'travel':
      return 'Travel';
    case 'inVineyard':
      return 'In Vineyard';
    case 'inWinery':
      return 'In Winery';
    case 'total':
      return 'Total';
    default:
      return k;
  }
}

export default function ColumnColorsPage() {
  const [colors, setColors] = useState<Record<SummaryColumnColorKey, string>>({ ...SUMMARY_COLUMN_COLOR_DEFAULTS });
  const [loading, setLoading] = useState(true);
  const [saveStatus, setSaveStatus] = useState<Record<SummaryColumnColorKey, SaveStatus>>(() => {
    const out = {} as Record<SummaryColumnColorKey, SaveStatus>;
    (Object.keys(SUMMARY_COLUMN_COLOR_SETTING_NAMES) as SummaryColumnColorKey[]).forEach((k) => (out[k] = 'idle'));
    return out;
  });

  const keys = useMemo(
    () => Object.keys(SUMMARY_COLUMN_COLOR_SETTING_NAMES) as SummaryColumnColorKey[],
    [],
  );

  useEffect(() => {
    const type = SUMMARY_COLUMN_COLOR_SETTINGS_TYPE;
    Promise.all(
      keys.map((k) =>
        fetch(`/api/settings?${new URLSearchParams({ type, name: SUMMARY_COLUMN_COLOR_SETTING_NAMES[k] })}`, {
          cache: 'no-store',
        })
          .then((r) => r.json())
          .then((data) => ({ k, v: normalizeHexColor(data?.settingvalue ?? '') }))
          .catch(() => ({ k, v: '' })),
      ),
    )
      .then((pairs) => {
        setColors((prev) => {
          const next = { ...prev };
          for (const p of pairs) next[p.k] = p.v;
          return next;
        });
      })
      .finally(() => setLoading(false));
  }, [keys]);

  const persist = useCallback((k: SummaryColumnColorKey, nextValue: string) => {
    const v = normalizeHexColor(nextValue);
    setSaveStatus((prev) => ({ ...prev, [k]: 'saving' }));
    fetch('/api/settings', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: SUMMARY_COLUMN_COLOR_SETTINGS_TYPE,
        settingname: SUMMARY_COLUMN_COLOR_SETTING_NAMES[k],
        settingvalue: v || '',
      }),
    })
      .then((r) => {
        setSaveStatus((prev) => ({ ...prev, [k]: r.ok ? 'saved' : 'error' }));
        if (r.ok) setColors((prev) => ({ ...prev, [k]: v }));
      })
      .catch(() => setSaveStatus((prev) => ({ ...prev, [k]: 'error' })))
      .finally(() => {
        window.setTimeout(() => setSaveStatus((prev) => ({ ...prev, [k]: 'idle' })), 1500);
      });
  }, []);

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Column colors</h1>
      <p className="mb-4 max-w-2xl text-sm text-zinc-600 dark:text-zinc-400">
        Choose background colors for Summary columns. These apply across Season, Daily, and By Job, for both Admin and Client views.
        Leave blank to use the default UI background.
      </p>

      {loading ? (
        <p className="text-zinc-500">Loading…</p>
      ) : (
        <div className="max-w-2xl space-y-3">
          {keys.map((k) => {
            const v = colors[k] || '';
            const status = saveStatus[k];
            return (
              <div
                key={k}
                className="rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900"
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="min-w-[14rem]">
                    <div className="text-sm font-medium text-zinc-900 dark:text-zinc-100">{labelForKey(k)}</div>
                    <div className="text-xs text-zinc-500">{SUMMARY_COLUMN_COLOR_SETTING_NAMES[k]}</div>
                  </div>

                  <div className="flex items-center gap-2">
                    <input
                      type="color"
                      value={v || '#FFFFFF'}
                      onChange={(e) => {
                        const next = normalizeHexColor(e.target.value);
                        setColors((prev) => ({ ...prev, [k]: next }));
                      }}
                      onBlur={() => persist(k, colors[k])}
                      className="h-8 w-10 rounded border border-zinc-300 bg-white dark:border-zinc-600 dark:bg-zinc-800"
                      title={v || '(default)'}
                    />
                    <input
                      type="text"
                      value={v}
                      onChange={(e) => setColors((prev) => ({ ...prev, [k]: e.target.value }))}
                      onBlur={() => persist(k, colors[k])}
                      placeholder="(default)"
                      className="w-32 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                    />
                    <button
                      type="button"
                      onClick={() => persist(k, '')}
                      className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
                      title="Clear to default"
                    >
                      Clear
                    </button>
                    <span className="min-w-[4.5rem] text-right text-xs text-zinc-500 dark:text-zinc-400">
                      {status === 'saving' ? 'Saving…' : status === 'saved' ? 'Saved' : status === 'error' ? 'Error' : '\u00A0'}
                    </span>
                  </div>
                </div>
                <div className="mt-2 h-8 rounded border border-zinc-200 dark:border-zinc-700" style={{ backgroundColor: v || undefined }} />
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

