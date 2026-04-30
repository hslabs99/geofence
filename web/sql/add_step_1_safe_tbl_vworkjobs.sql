-- One-time stash of imported step_1_completed_at before Step1(lastJobEnd) override. Write-once (never overwritten).
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step_1_safe TIMESTAMP WITHOUT TIME ZONE;

COMMENT ON COLUMN tbl_vworkjobs.step_1_safe IS 'Original imported step_1_completed_at preserved once when Step1(lastJobEnd) applies; never updated after.';
