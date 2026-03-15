-- Update TT check constraint to allow T, TT, TTT (no longer T_T).
-- Run after add_template_tbl_wineryminutes.sql if the table already had the old TT constraint.
ALTER TABLE tbl_wineryminutes DROP CONSTRAINT IF EXISTS tbl_wineryminutes_tt_check;
ALTER TABLE tbl_wineryminutes ADD CONSTRAINT tbl_wineryminutes_tt_check CHECK ("TT" IN ('T', 'TT', 'TTT'));
