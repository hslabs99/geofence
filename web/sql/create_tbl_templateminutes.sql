-- tbl_templateminutes: template minute allocations by customer
CREATE TABLE IF NOT EXISTS tbl_templateminutes (
  id          SERIAL PRIMARY KEY,
  "Customer"  VARCHAR(255),
  "Template"  VARCHAR(255),
  "ToVineMins"   INTEGER,
  "InVineMins"   INTEGER,
  "ToWineMins"   INTEGER,
  "InWineMins"   INTEGER,
  "TotalMins"    INTEGER
);
