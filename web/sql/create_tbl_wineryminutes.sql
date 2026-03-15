-- tbl_wineryminutes: winery minute allocations by customer, template, winery
CREATE TABLE IF NOT EXISTS tbl_wineryminutes (
  id          SERIAL PRIMARY KEY,
  "Customer"  VARCHAR(255),
  "Template"  VARCHAR(255),
  "Winery"    VARCHAR(255),
  "TT"        VARCHAR(10) CHECK ("TT" IN ('T', 'TT', 'TTT')),
  "ToVineMins"   NUMERIC(10,2),
  "InVineMins"   NUMERIC(10,2),
  "ToWineMins"   NUMERIC(10,2),
  "InWineMins"   NUMERIC(10,2),
  "TotalMins"    NUMERIC(10,2)
);
