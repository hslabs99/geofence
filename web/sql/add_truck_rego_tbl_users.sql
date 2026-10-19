-- Truck registration for Driver users. Shown in User Admin only when user type is Driver.
-- The app also adds this column and seeds driver Mike (password 123456) on first driver login or user list load.

ALTER TABLE tbl_users ADD COLUMN IF NOT EXISTS truck_rego text;

COMMENT ON COLUMN tbl_users.truck_rego IS 'Truck registration for user_type Driver. Null for other user types.';
