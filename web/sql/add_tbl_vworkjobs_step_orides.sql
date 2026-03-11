-- Manual step overrides (orides) and comment. Run in Heidi or any SQL client.
-- Part 3 of steps calc: if stepNoride is set, it trumps GPS/VWork for that step; step_N_via = 'ORIDE'.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step1oride timestamp,
  ADD COLUMN IF NOT EXISTS step2oride timestamp,
  ADD COLUMN IF NOT EXISTS step3oride timestamp,
  ADD COLUMN IF NOT EXISTS step4oride timestamp,
  ADD COLUMN IF NOT EXISTS step5oride timestamp,
  ADD COLUMN IF NOT EXISTS steporidecomment text;
