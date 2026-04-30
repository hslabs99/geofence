# Master rules: job steps 1–5 (GPS + VWork + cleanup + overrides)

This document is the **plain-English** version of the logic implemented in `web/src/lib/derived-steps.ts` and the Steps+ path in `web/src/app/api/tracking/derived-steps/route.ts`. **Order matters:** rules below are listed in the same sequence the code applies them.

---

## 1. What problem this solves

For each VWork job we want five **timestamps** (steps 1–5) that describe the driver’s day: leave winery, arrive vineyard, leave vineyard, return to winery, job complete. Some values come from **GPS** (`tbl_tracking` ENTER/EXIT via geofences), some from **VWork** (completed-step fields on the job), and we apply **guardrails**, **cleanup rules**, and **manual overrides** in a fixed order.

**Separate topic (not repeated here):** API → database storage of raw `position_time` strings. See [VERBATIM_TIMES.md](./VERBATIM_TIMES.md). Derived steps use **`position_time_nz`** from `tbl_tracking` for ordering and display-scale comparisons inside this pipeline.

### Step 1–5 definitions (canonical — do not re-purpose or swap)

These are the **only** meanings for step numbers in this document and in code:

| Step | Role in the day (VWork / UI) | GPS layer (when derived) |
|------|------------------------------|---------------------------|
| **1** | **Start job** — job start time | Morning **winery EXIT** on the delivery winery (leave winery for the outbound leg). Often close to “start job”; it is still **step 1**, not step 2 or 3. |
| **2** | **Arrive vineyard** | **Vineyard ENTER** |
| **3** | **Depart vineyard** (leave vineyard) | **Vineyard EXIT** (optional **GPS\*** smoothing) |
| **4** | **Arrive winery** (return to winery) | **Winery ENTER** |
| **5** | **Job completed** | **Winery EXIT** in the job-end / extend window |

Implementation comments may abbreviate “GPS time for step 1” as **G1** — that is **always** step **1** (winery EXIT), never step 2 or 3.

---

## 2. End-to-end pipeline (high level)

1. **Resolve fences**  
   IF the job has a vineyard name or delivery winery name  
   THEN look up `tbl_gpsmappings` and `tbl_geofences` to get **fence id lists** for that job’s vineyard and winery (case-insensitive names).

2. **Part 1 — Fetch GPS candidates** (`fetchGpsStepCandidates`)  
   Query `tbl_tracking` inside the **data window** for ENTER/EXIT on those fences. The lower bound is **`min`(`positionAfter`, job-start anchor)** when an anchor exists, else `positionAfter` — where **job-start anchor** = `step1oride` if set, else VWork step 1 (`step_1_completed_at` / `actual_start_time`). That pulls the window earlier when the override is before the client’s `positionAfter` so morning tags are visible. **Winery step 4** uses **`max`(anchor ∨ GPS step 1, step 2, step 3)** for the ENTER lower bound **only when both GPS step 2 and GPS step 3 exist**; **if both are missing**, the floor is **`max`(anchor ∨ GPS step 1, `positionAfter`)** only (vineyard GPS does not constrain return-leg winery tags). Produce candidate times + tracking row ids for steps 1–5 (where the rules below say to look).

3. **Guardrails** (`applyGpsGuardrails`)  
   AFTER fetch, drop or clear candidates that violate ordering, VWork job end, duplicate tracking ids, or “step 5 without step 4”. **GPS step 1 vs job-start anchor:** keep **GPS step 1** (morning winery EXIT) after the anchor only when **GPS step 2** (arrive vineyard / vineyard ENTER) exists and GPS step 1 is **before** GPS step 2; if GPS step 1 is after the anchor and **GPS step 2** is missing, clear GPS step 1. Floors for steps **2, 3, 5** use **min**(anchor, GPS step 1) when both exist; step **4** uses the same **max** rule as fetch (vineyard steps in the max only when **both** GPS step 2 and 3 exist; when **both** are missing, **max**(step‑1 leg, `positionAfter`) only).

4. **Part 2 — GPS layer only** (`decideFinalSteps`)  
   Copy candidates into `stepNGps` fields. Special case for step 5: keep GPS if it is **strictly before** VWork job end, **or** on or after job end but **strictly before** job end + **Step 5 extend for winery exit** minutes (`tbl_settings` Step5ExtendWineryExit).

5. **Part 3 — Finalize** (`finalizeDerivedSteps`)  
   Run **in this exact order:**  
   a. **Merge** GPS with VWork → provisional “actual” times  
   b. **Cleanup rules** on those actuals  
   c. **Manual overrides** (orides) on top — last word

6. **Steps+ (optional, API write-back only)**  
   IF polygon step 2/3 are missing OR buffered vineyard **widens** the polygon window  
   THEN merge buffered segments, re-fetch winery steps 4–5 with the new step 2/3 times, re-run guardrails + `decideFinalSteps` + `finalizeDerivedSteps`.  
   (Details in section 10.)

---

## 3. Data window and device

- **IF** `device` (worker) and `positionAfter` are missing  
  **THEN** no GPS derivation runs (empty result).

- **Tracking device** is the job’s **worker** (same as `tbl_tracking.device_name`) when present; else the request’s device.

- **Window end** for vineyard queries may be **extended** to at least **job end + Job End Ceiling Buffer** minutes (`tbl_settings`) so a late vineyard EXIT after an early “job complete” is still visible.  
  IF `positionBefore` is null  
  THEN that extension still applies where the code computes a ceiling from job end.

---

## 4. Part 1 — Fetch rules (per step)

All lookups use **first** row in time order (ASC) unless noted. Times compared as normalized `YYYY-MM-DD HH:mm:ss` strings (lexicographic order matches chronological order for those strings).

### Step 1 — Start job (GPS: morning winery EXIT)

- **Meaning:** First **Winery EXIT** on the delivery winery’s fences, **strictly after** the effective window lower bound, **strictly before** an upper bound, **and** with **no** mapped **Winery ENTER** on those same fences strictly between that lower bound and that EXIT. (So a re-ENTER at the winery before a later EXIT is not treated as “morning depart” — that EXIT is the return leg or a later move, not job start.)

- **Upper bound**  
  - **IF** polygon **Vineyard ENTER** (step 2 GPS) exists: **min**(`positionBefore`, polygon step 2 time). VWork **step 2 completed** is **not** used — it can be early vs GPS and would wrongly exclude a valid morning winery EXIT still before the real vineyard ENTER.  
  - **ELSE** (no polygon step 2): **min**(`positionBefore`, VWork step 2 completed when present) so the search still does not extend past “arrive vineyard” when only VWork times exist (avoids picking a **later** winery EXIT on the return leg as “morning start”).

- **IF** the driver was already outside the winery fence when the window starts  
  **THEN** step 1 GPS may be **absent**.

### Step 2 — Arrive vineyard

- **IF** vineyard fences exist  
  **THEN** first **Vineyard ENTER** after the effective window start (`min`(`positionAfter`, job-start anchor)), before the (possibly extended) vineyard window end.

- **After** morning winery EXIT (step 1 GPS) is known, **IF** the first ENTER above is **strictly before** that EXIT (impossible outbound order — usually an earlier drive-by), **THEN** the code **re-fetches** step 2 as the first Vineyard ENTER **strictly after** `max`(window start, GPS step 1 EXIT), re-fetches step 3 (+ GPS\* if applicable), then **re-queries** morning winery EXIT with the new step 2 as upper bound so step 1 stays the last winery EXIT before the real arrive.

### Step 3 — Leave vineyard

- **IF** step 2 exists  
  **THEN** first **Vineyard EXIT** strictly after step 2’s time (not an earlier exit before the enter).

- **ELSE** search from `positionAfter` as the lower bound (same as step 2 missing path).

### Step 3 — GPS* (optional smoothing)

- **IF** step 2 and step 3 polygon candidates both exist  
  **THEN** the code may replace step 3 with a **later** vineyard EXIT after up to **3** “loop backs” (exit → re-enter same vineyard fence set → exit again).

- **IF** any ENTER/EXIT on a fence **outside** the job’s vineyard fence set occurs **strictly between** two anchors in the chain  
  **THEN** GPS* is **void** for that job → revert to the **first** EXIT only.

- **IF** GPS* applies  
  **THEN** step 3 is labeled **GPS\*** in downstream Via logic.

### VineSR1 (Bankhouse South)

- **IF** vineyard name is **Bankhouse South** **AND** polygon step 2 **or** step 3 is missing  
  **THEN** clear partial results and **retry** step 2/3 using fences mapped for **Bankhouse** only (tag **VineSR1**).

- **IF** guardrails later wipe step 2 or 3  
  **THEN** the VineSR1 fallback flag is cleared.

### Step 4 — Return: winery ENTER

- **IF** winery fences exist  
  **THEN** first **Winery ENTER** in the data window (strictly after the lower bound, strictly before `positionBefore` when set).  
  **Lower bound:** **IF** both **GPS step 2** and **GPS step 3** exist (after the same job-end / step‑3 ceiling pruning as guardrails — vineyard ENTER/EXIT at or after “job complete” do not count) **THEN** strictly after `max`(step‑1 leg, step 2, step 3) — step‑1 leg = `min`(anchor, GPS step 1) when both exist, else whichever exists. **IF both are missing after that pruning** **THEN** strictly after `max`(step‑1 leg, `positionAfter`) only — vineyard GPS does not narrow step 4 when there is no usable GPS vineyard leg.

### Step 5 — Winery EXIT for job end (forgot to end job **or** early “complete” tap)

- VWork **step 5** = `step_5_completed_at` (or `actual_end_time` when used as job end).

- **IF** step 4 exists **AND** VWork step 5 exists **AND** VWork step 5 is **strictly after** step 4  
  **THEN** look for the **first** **Winery EXIT** strictly after step 4, strictly before **min**(`positionBefore`, VWork step 5 **+ Step 5 extend for winery exit** minutes) — **not** the last exit. This captures “left winery before tapping complete” and “tapped complete before physically leaving”, within the extend window.

- **IF** those conditions fail  
  **THEN** step 5 GPS candidate is not set this way (often null).

---

## 5. Guardrails (after fetch, before “GPS layer” is final)

Applied in `applyGpsGuardrails` in this **order**:

1. **VWork job end vs step 1**  
   **IF** VWork job end exists **AND** GPS step 1 exists **AND** step 1 time ≥ job end  
   **THEN** clear step 1.

2. **GPS step 1 vs job-start anchor** (`step1oride` if set, else VWork step 1 — see **Step 1–5 definitions** above)  
   VWork **step 2 / step 3** times are **not** used to qualify GPS step 1.  
   **IF** GPS step 1 **>** anchor **AND** GPS step 2 (arrive vineyard) exists **AND** GPS step 1 **&lt;** GPS step 2 **THEN** **keep** GPS step 1 (driver tapped start before physically leaving the winery, but winery EXIT is still before **step 2** arrive vineyard).  
   **ELSE IF** GPS step 1 **>** anchor **AND** (GPS step 2 is missing **OR** GPS step 1 ≥ GPS step 2) **THEN** clear GPS step 1.

3. **Sequencing floor for steps 2, 3, 5**  
   Let **anchor** = `step1oride` if set, else VWork step 1. Let **GPS step 1** = morning winery EXIT candidate when present.  
   The floor is **`min`(anchor, GPS step 1)** when **both** exist, else whichever exists — so a **late VWork start** (e.g. 10:15) does **not** clear a valid vineyard ENTER (e.g. 10:09) that is still **after** GPS morning exit (e.g. 09:47).  
   **THEN** drop step 2, 3, or 5 if their time is **≤** that floor (after any GPS step 1 that is **strictly after** the override-only anchor has been cleared).  
   (Step 4 uses the same **min**(anchor, GPS step 1) for the step‑1 leg inside the step 4 floor; vineyard steps 2–3 participate in that max **only when both** GPS step 2 and 3 exist; when **both** are missing, only `positionAfter` joins the step‑1 leg in the max.)

4. **VWork job end vs steps 2–3**  
   **IF** job end exists **AND** step 2 ≥ job end  
   **THEN** clear step 2 **and** step 3.  
   **ELSE IF** step 3 exists **AND** step 3 ≥ **ceiling**  
   **THEN** clear step 3 only.  
   Here **ceiling** = job end **+ Job End Ceiling Buffer** minutes (when buffer &gt; 0), else job end.

5. **Step 4 floor**  
   **IF** the step 4 floor exists (same rule as fetch: `max`(step‑1 leg, step 2, step 3) when **both** step 2 and 3 GPS exist; when **both** are missing, `max`(step‑1 leg, `positionAfter`)) **AND** step 4 ≤ that floor  
   **THEN** clear step 4.

6. **Duplicate `tbl_tracking` id**  
   **IF** the same tracking row id would be used for more than one step  
   **THEN** clear the later assignment(s) so each id appears at most once.

7. **Step 5 depends on step 4**  
   **IF** step 4 was cleared  
   **THEN** clear step 5.

8. **GPS* metadata**  
   **IF** step 3 was cleared  
   **THEN** drop GPS* flags.

---

## 6. Part 2 — Build `stepNGps` from candidates (`decideFinalSteps`)

- Steps 1–4: **IF** candidate present **THEN** set `stepNGps` and tracking id — **except** step 1: **IF** `step1oride` is set **THEN** do **not** set `step1Gps` (final step 1 comes from the override; avoids a misleading GPS “start” tag).

- Step 5: **IF** candidate present **AND** VWork job end exists **AND** (`gpsTime < jobEnd` **OR** (`gpsTime` ≥ jobEnd **AND** `gpsTime` &lt; jobEnd + Step 5 extend minutes))  
  **THEN** set `step5Gps`.  
  **ELSE** leave `step5Gps` null (VWork will carry step 5 in merge).

- Via hints from Part 1: **GPS\***, **VineSR1** on step 2/3 when applicable.

---

## 7. Part 3a — Merge GPS with VWork (`resolveActualFromGpsAndVwork`)

For each step **N**:

- **IF** `stepNGps` is present  
  **THEN** `stepN_actual` = GPS time.  
  **ELSE** `stepN_actual` = VWork completed time for that step (with step 5 using `step_5_completed_at` / `actual_end_time` per `vworkStepTime`).

- **Step 1 (special):** **IF** `step1oride` is set **THEN** merged step 1 = **override** (anchor wins over GPS step 1 and over raw VWork step 1). **ELSE** `step1_actual` = GPS step 1 **if** present, **else** VWork step 1 (`step_1_completed_at` / `actual_start_time`).  
  **Why:** One **job-start anchor** — override or VWork — drives fetch, guardrails, merge, and cleanup so a late tap or a wrong GPS winery EXIT does not wipe valid vineyard rows or break step 4.

---

## 8. Part 3b — Cleanup rules (`applyCleanupRules`) — run on merged actuals only

These run **after** GPS∨VWork merge, **before** overrides. They only adjust **actual** fields, not raw `stepNGps`.

**Job-start anchor** in Rules A and B is **`jobStep1Anchor`**: `step1oride` when set, else `step_1_completed_at`, else `actual_start_time` (same as §7).

### Rule A — `cleanup_start`

**IF** actual step 1 is null **AND** actual step 2 exists **AND** VWork job-start time exists **AND** actual step 2 is **before** VWork job-start (strange ordering)  
**THEN** set step 1 = step 2 **minus 10 minutes**.

### Rule B — `travel` (else-if, not both A and B)

**ELSE IF** VWork job-start exists **AND** actual step 2 exists **AND** VWork job-start is **after** actual step 2 **AND** (actual step 1 is null **OR** step 1 is **not strictly before** step 2)  
**THEN** set step 1 = step 2 minus **travel minutes**, where:

- **IF** both step 3 and step 4 exist  
  **THEN** travel minutes = duration from step 3 to step 4 (capped between 1 and 120).  
- **ELSE** travel minutes = **20**.

### Rule C — `Step3windback` (before Rule D)

**IF** `step_4_gps` exists (winery ENTER from GPS) **AND** `step_3_gps` does **not** exist (leave vineyard not from GPS) **AND** steps 1–4 are all non-null **AND** merged step 4 is **before** merged step 3 (VWork step 3 after GPS winery arrive — impossible order) **THEN**:

- Set step 3 = step 4 **minus** outbound minutes (duration from step 1 to step 2), capped to at most 24 hours of minutes — **if** that time is **strictly after** step 2.
- **Else** set step 3 = **midpoint** between step 2 and step 4 (half the interval after step 2, half before step 4).

Step 4 is **not** changed. Rule **D** does not run when this applies.

With **GPS write-back**, **`Step3windback:`** is appended to **`tbl_vworkjobs.calcnotes`** (same pattern as **`VineFence+:`** / **`GPS*:`**), so Inspect CalcNotes shows that this cleanup ran.

The **`/api/tracking/derived-steps`** JSON includes **`cleanupRulesReport`** (and **`debug.cleanupRulesReport`**) with auditable fields for Part 3b: step 1 cleanup, **Step3windback**, and **step4_order**. Inspect → Steps debug → **Explanation** renders the same text.

### Rule D — `step4_order` (else-if after C)

**IF** Rule C did **not** apply **AND** steps 1–4 are all non-null **AND** step 4 is **before** step 3 (e.g. step 3 is also GPS — windback skipped)  
**THEN** set step 4 = step 3 **plus** outbound minutes (duration from step 1 to step 2), capped to at most 24 hours of minutes, only if the result is still ≥ step 3.

---

## 9. Part 3c — Manual overrides (`applyOrides`) — last

For each step **N**:

- **IF** `stepNoride` is non-empty  
  **THEN** final `stepN` = override.  
  **ELSE** final `stepN` = value after cleanup.

For **step 1 only**, if `step1oride` is set, it is **already** the VWork step-1 input to Part 3a (§7) and the “job start” for Part 3b cleanup (§8); Part 3c still stamps the final step 1 and **ORIDE** Via as the last word.

Raw `stepNGps` columns in the result object are still the **GPS layer** for labeling and persistence.

### Via labels (which source “wins” for UI)

Priority for each step:

1. **ORIDE** if that step has an override.
2. Else **RULE** for step 1 if cleanup changed step 1; **Step3windback** for step 3 if that cleanup applied; **RULE** for step 4 if `step4_order` applied.
3. Else **VineFence+** / **VineFenceV+** / **VineSR1** / **GPS\*** when set on the GPS layer from Steps+ or GPS*.
4. Else **GPS** if `stepNGps` present.
5. Else **VW** (VWork).

---

## 10. Steps+ (buffered vineyard) — when it runs

Only in **`/api/tracking/derived-steps` with `writeBack`** and a vineyard name:

1. Run **initial** `deriveGpsStepsForJob` (full pipeline above).

2. Query buffered stays (`runStepsPlusQuery` + settings). Filter stays to those inside job rules (enter before VWork end, exit before job end + ceiling buffer).

3. **IF** no qualifying stays → stop; keep first result.

4. **IF** polygon had **both** step 2 and step 3 GPS  
   **THEN** compute merged buffer enter/exit; **IF** buffered enter is **not** strictly before polygon ENTER by **more than 5 minutes** (`vineyardBufferWidensPolygonEnter` / `VINE_FENCE_V_PLUS_MIN_ENTER_DELTA_MINUTES`) → **do not** apply VineFenceV+ (avoids false “queue” when the truck is only beside the fence on the road). Polygon EXIT is never replaced by the buffer (exit stays the true fence tag).

5. **ELSE** (missing polygon 2/3 **OR** buffered enter is earlier than polygon ENTER):  
   - Merge segments (may apply **GPS\***-style merge across buffer segments — see `aggregateStepsPlusBufferedSegments`).  
   - Re-fetch winery steps **4–5** with `deriveGpsLayerAfterVineFencePlus` (re-runs guardrails + `decideFinalSteps` on new step 2/3). VineFenceV+ uses buffered **enter** + polygon **exit**; VineFence+ uses merged enter/exit when polygon steps were missing.  
   - `finalizeDerivedSteps` again with Via **VineFence+** (missing polygon) or **VineFenceV+** on step 2 only (step 3 Via **GPS** when polygon EXIT kept). If guardrails drop the buffered step 2/3 pair, the run **reverts** to the pre-buffer polygon GPS (no VineFenceV+ tag / null step 2 GPS mismatch). **Calcnotes** for VineFenceV+ may include **`(N min enter earlier)`** after the token when the buffer pulled arrive earlier than polygon ENTER.

---

## 11. Quick reference: order of operations

| Phase | What runs |
|--------|-----------|
| Fetch | Fences → step 2 → step 3 (+ GPS*) → VineSR1 retry if needed → step 1 → step 4–5 |
| Guardrails | step 1 vs end → GPS step 1 vs anchor & step 2 → floor 2,3,5 → step 2–3 vs end/ceiling → step 4 floor → dedupe ids → step 5 if no step 4 |
| GPS layer | `decideFinalSteps` |
| Finalize | merge GPS∨VWork → cleanup (A, else B, then C) → orides |

---

## 12. Code pointers

| Topic | Location |
|--------|----------|
| Job-start anchor (`step1oride` or VWork step 1) | `jobStep1Anchor` in `web/src/lib/derived-steps.ts` |
| Fetch + guardrails + finalize helpers | `web/src/lib/derived-steps.ts` |
| HTTP API, window end, Steps+, write-back | `web/src/app/api/tracking/derived-steps/route.ts` |
| Verbatim API timestamps | `web/docs/VERBATIM_TIMES.md`, `web/src/lib/verbatim-time.ts` |

---

*Regenerate or extend this doc when changing `derived-steps.ts` or the derived-steps route so plain-English rules stay aligned with code.*
