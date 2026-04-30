-- step_N_via: allow Step3windback, VineFenceV+, etc. (varchar(10) truncates longer tokens).
ALTER TABLE tbl_vworkjobs
  ALTER COLUMN step_1_via TYPE varchar(32),
  ALTER COLUMN step_2_via TYPE varchar(32),
  ALTER COLUMN step_3_via TYPE varchar(32),
  ALTER COLUMN step_4_via TYPE varchar(32),
  ALTER COLUMN step_5_via TYPE varchar(32);
