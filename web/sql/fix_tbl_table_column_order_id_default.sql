-- tbl_table_column_order.id is NOT NULL with no default and is a TEXT column.
-- The /api/column-order PUT now supplies id = table_name explicitly, so this
-- migration is OPTIONAL — it only makes the column self-defaulting so future
-- callers do not need to know about the id column.
--
-- Idempotent. Safe to re-run.

ALTER TABLE tbl_table_column_order
  ALTER COLUMN id SET DEFAULT '';

-- Backfill any existing NULLs with the table_name (defensive).
UPDATE tbl_table_column_order
SET id = table_name
WHERE id IS NULL OR id = '';
