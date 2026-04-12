-- Exclude job from Summary rollups (Inspect manual overrides). 1 = excluded; 0 or NULL = included.
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS excluded integer,
  ADD COLUMN IF NOT EXISTS excludednotes varchar(250);
