-- Example schema for import_file. Adapt to your existing schema.
-- tbl_vworkjobs and seq_vwork_batch should already exist.

CREATE TABLE IF NOT EXISTS import_file (
  drive_file_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'processing',
  row_count INTEGER,
  batchnumber INTEGER,
  error_message TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
