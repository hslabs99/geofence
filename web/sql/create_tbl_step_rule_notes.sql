-- Step / vineyard special rules documentation (Admin → Rules notes).
-- Optional seed for VineSR1 (comment out if duplicate).

CREATE TABLE IF NOT EXISTS tbl_step_rule_notes (
  id          serial PRIMARY KEY,
  ruled       text NOT NULL,
  ruledesc    text,
  level       text NOT NULL CHECK (level IN ('Winery', 'Vineyard')),
  rulenotes   text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tbl_step_rule_notes_ruled_level
  ON tbl_step_rule_notes (ruled, level);

CREATE INDEX IF NOT EXISTS idx_tbl_step_rule_notes_level
  ON tbl_step_rule_notes (level);

COMMENT ON TABLE tbl_step_rule_notes IS 'Human-readable notes for GPS step special rules (e.g. VineSR1).';
COMMENT ON COLUMN tbl_step_rule_notes.ruled IS 'Short rule id (e.g. VineSR1).';
COMMENT ON COLUMN tbl_step_rule_notes.level IS 'Winery or Vineyard scope.';

-- INSERT INTO tbl_step_rule_notes (ruled, ruledesc, level, rulenotes) VALUES
-- ('VineSR1', 'Bankhouse South fallback to Bankhouse for steps 2–3', 'Vineyard',
--  'When vineyard_name is Bankhouse South and polygon step 2/3 cannot be found for South fences, use Bankhouse vineyard fences; tags VineSR1 on job and calcnotes.')
-- ON CONFLICT (ruled, level) DO NOTHING;
