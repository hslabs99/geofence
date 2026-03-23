# VERBATIM TIMES — NEVER ADJUST TIMEZONES

**This has broken our process many times. The rules below are non-negotiable.**

## The rule

- **We NEVER adjust timestamps for timezones.** Not in the API layer, not in the DB layer, not in the UI.
- **What the API sends is what we store.** The value that arrives in `position_time` (tbl_apifeed, tbl_tracking) must be **exactly** the string we received. No parsing to `Date` and re-formatting. No appending `Z`. No conversion to UTC or local.
- **If we receive timezone information** (from TrackSolid, geofence API, or our own API), **we ignore it**. Treat every date and time as a literal string. Nothing is ever adjusted.

## The only exception

- **position_time_nz** is the only derived timestamp. It is set in exactly one place: a **SQL UPDATE** (e.g. `position_time + interval '13 hours'`). That happens in:
  - `api/admin/tracking/apply-position-time-nz-and-fences`
  - `api/admin/tracking/store-fences-for-date-range`
- No application code must ever derive `position_time` from API data using `Date`, `toISOString`, `getUTC*`, or any timezone logic. API → `position_time` is **verbatim only**.

## How we enforce it

1. **Runtime guard:** All values written to `position_time` from API data must go through `@/lib/verbatim-time`: `positionTimeForStorage(apiString)` and, for query bounds, `positionTimeBoundForQuery(apiString)`. These accept **only** a string (never a `Date`) and return it trimmed. Passing a `Date` throws.
2. **Check script:** Run `npm run check:verbatim` (or `node scripts/check-verbatim-times.mjs`) to scan critical paths for forbidden patterns. CI or pre-commit should run this.
3. **Cursor rule:** `.cursor/rules/timestamps-raw-no-timezone.mdc` is always applied. It states the same rules so AI and humans see them.

## FORBIDDEN (will break data)

- Passing a JavaScript `Date` to the DB for `position_time`.
- Using `formatDateForDebug`, `toISOString()`, or any `getUTC*()` to produce a value that is then stored as `position_time` when that value came from API (e.g. gpsTime).
- Appending `Z` or any timezone offset to an API timestamp before storage.
- Converting API timestamps “to UTC” or “to local” for storage.
- Using `moment`, `date-fns/tz`, or similar to convert API timestamps before writing to `position_time`.

## Allowed

- Storing the **exact** string from the API (after trim) in `position_time`.
- Using `SET LOCAL timezone = 'UTC'` when running queries so that string bounds (e.g. `'2026-03-10 00:00:00'`) are interpreted as that exact moment by the DB, not shifted by session TZ.
- Parsing an API string into `Date` **only** to validate that it is parseable (e.g. to skip invalid rows). The value we write must still be the **original string**, not the formatted Date.
- The single SQL UPDATE that sets `position_time_nz` from `position_time`.

## Pipeline (must stay verbatim)

1. **TrackSolid API** returns `gpsTime` (string).
2. **tracksolid.ts** `getTrackForDevice` passes it through as `String(p.gpsTime ?? '')` — no change.
3. **api/auto/gpsimport** sends that same string in the body to apifeed/import.
4. **api/apifeed/import** uses `positionTimeForStorage( raw )` and writes that string to `tbl_apifeed.position_time`. No Date, no format.
5. **api/apifeed/merge-tracking** copies `a.position_time` into `tbl_tracking.position_time` and uses `positionTimeBoundForQuery(minTime/maxTime)` for the range. No conversion.
6. **position_time_nz** is filled later by the SQL UPDATE only.

If any step converts or “fixes” the time, the data is wrong. Do not add timezone logic.
