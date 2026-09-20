# Delegat overlap: wrong start clock vs final job times

**Source:** Delegat *Waiting time analysis for CEL.xlsx*, sheet **DATA Sorted**.  
**Our clocks:** `step_1_actual_time` (final start) and `step_5_actual_time` (final end). These are the same timestamps used for travel, in-vineyard, in-winery, and total.

## What went wrong

The client export from **Summary → By Job (client view)** / **Export Jobs Data** gave them `start_time` = VWork `actual_start_time` (the tap), not our final start.

Duration columns (travel, in vineyard, in winery, total) were already from final steps. They then set **end = start + total**. That mixes a VWork start with a GPS-length job. Overlap became: next VWork start vs that constructed end.

Their Summary figure **3,829 overlap minutes** (and **439 instances**) is that mixed clock. It is not double-charged time in CEL’s job-card minutes.

## If they rerun the same overlap test on the correct start

- **Previous final end vs next final start** (same truck, consecutive jobs, included jobs only): **43 pairs, 129 minutes**.
- That is the overlap that exists on the times we actually bill.
- If they only swap in final start and still compute **end = start + total** (whole minutes), they will **not** get 129. Rounding of phase minutes reconstructs an end that is often a fraction of a minute away from GPS step 5, and that inflates the count. The fair test is **final start and final end**, not start + rounded total.

## Season picture (1,230 included jobs)

| Test | Result |
|---|---|
| Their sheet (VWork start + start+total) | 439 instances, 3,829 minutes (Summary) |
| Same overlap rule, **final** start and **final** end | **43 pairs, 129 minutes** |

We do still have **129 minutes** of same-truck overlap on job cards (manual overrides, excluded-then-wound starts, and GPS job-complete after the next final start). That is real and should be owned. It is not 3,800 minutes.

---

## Their first three overlap examples

All three are 01-HBD, Crownthorpe. On **DATA Sorted** the minutes sit on the previous job.

### 1. Jobs `49497514` → `49497513` — they have 4 minutes

| | Their sheet | Our final times |
|---|---|---|
| Previous start | 11 Feb 22:50 (VWork) | 11 Feb **22:51:11** |
| Previous end | 12 Feb 02:09 (start + 199 total) | 12 Feb **02:09:55** (GPS) |
| Next start | 12 Feb **02:05** (VWork tap) | 12 Feb **02:09:55** (GPS) |
| Overlap | **4 min** | **0** |

They tapped complete / start at 02:05. GPS left the winery at 02:09:55. We use that EXIT as previous end **and** next start. The windows meet; they do not overlap.

### 2. Jobs `49531938` → `49542443` — they have 6 minutes

| | Their sheet | Our final times |
|---|---|---|
| Previous start | 14 Feb 18:58 (VWork) | 14 Feb **19:00:17** |
| Previous end | 14 Feb 21:57 (start + 179) | 14 Feb **21:59:11** (GPS) |
| Next start | 14 Feb **21:51** (VWork) | 14 Feb **21:59:11** (GPS) |
| Overlap | **6 min** | **0** |

Same pattern: VWork next-start is before GPS leave; final start of the next job **is** that GPS leave.

### 3. Jobs `49542443` → `49542445` — they have 8 minutes

| | Their sheet | Our final times |
|---|---|---|
| Previous start | 14 Feb 21:51 (VWork) | 14 Feb **21:59:11** |
| Previous end | 15 Feb 01:48 (start + 237) | 15 Feb **01:55:32** (GPS) |
| Next start | 15 Feb **01:40** (VWork) | 15 Feb **01:55:32** (GPS) |
| Overlap | **8 min** | **0** |

Again 0 on final times. Their 8 minutes is VWork tap vs constructed end.

---

## Their three largest overlap examples

These look severe on VWork start + start+total. On final start/end they collapse to **0**.

### 1. Job `50180507` (20-WLS, Birch Hill) — they have 307 minutes

Next job on the sheet: `50180506`.

| | Their sheet | Our final times |
|---|---|---|
| This start | 2 Apr **20:31** (VWork) | 2 Apr **15:31:00** (final) |
| This end | 3 Apr **03:20** (20:31 + 409) | 2 Apr **22:20:16** (GPS) |
| Next start | 2 Apr **22:13** (VWork) | 2 Apr **22:20:16** (GPS) |
| Overlap | **307 min** | **0** |

The 409-minute total is the GPS-length job. Adding it to the **late** VWork tap (20:31) invents an end at 03:20. Final start was already 15:31; GPS end and next GPS start are the same instant (22:20:16).

### 2. Job `50162862` (19-TEM, Webb’s) — they have 243 minutes

Next job on the sheet: `50162870`.

| | Their sheet | Our final times |
|---|---|---|
| This start | 31 Mar **22:35** (VWork) | 31 Mar **18:35:00** (final) |
| This end | 1 Apr **05:29** (22:35 + 414) | 1 Apr **01:28:52** (GPS) |
| Next start | 1 Apr **01:26** (VWork) | 1 Apr **01:28:52** (GPS) |
| Overlap | **243 min** | **0** |

Same construction: VWork start + GPS duration overruns the next VWork tap. Final windows meet at 01:28:52.

### 3. Job `50180508` (16-NC, Birch Hill) — they have 146 minutes

Next job on the sheet: `50180522`.

| | Their sheet | Our final times |
|---|---|---|
| This start | 2 Apr **16:34** (VWork) | 2 Apr **13:38:00** (final) |
| This end | 2 Apr **23:27** (16:34 + 413) | 2 Apr **20:30:37** (GPS) |
| Next start | 2 Apr **21:01** (VWork) | 2 Apr **20:30:37** (GPS) |
| Overlap | **146 min** | **0** |

Final next start equals this GPS end. Their 146 minutes is the mixed clock.

---

## What we still own

Using **final start vs final end**, same-truck overlap on included Delegat jobs is **129 minutes** across **43** consecutive pairs (**78** jobs of 1,230). That is job-card overlap (overrides, GPS complete after the next final start, and similar). It should be reduced. It is not the 3,829 minutes on their Summary.

**Export fix:** client view By Job / Export Jobs Data must use `step_1_actual_time` as start (and `step_5_actual_time` if an end is exported), never VWork `actual_start_time`.
