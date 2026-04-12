-- Load size from vWork exports → tbl_vworkjobs.loadsize (numeric).
-- Also align tbl_mappings: "load size" → loadsize; add "customer" → customer for exports that use "Customer" instead of "Client name".

ALTER TABLE tbl_vworkjobs ADD COLUMN IF NOT EXISTS loadsize NUMERIC;

UPDATE tbl_mappings SET dbcolumnname = 'loadsize'
WHERE type = 'VW' AND lower(trim(filefieldname)) = 'load size';

INSERT INTO tbl_mappings (type, filefieldname, dbcolumnname, dbmaxlength) VALUES
('VW', 'load size', 'loadsize', NULL),
('VW', 'customer', 'customer', 100)
ON CONFLICT (type, filefieldname) DO UPDATE SET
  dbcolumnname = EXCLUDED.dbcolumnname,
  dbmaxlength = COALESCE(EXCLUDED.dbmaxlength, tbl_mappings.dbmaxlength);
