-- Add user_type and customer to tbl_users for User CRUD (Super Admin, Admin, Client; Client can have assigned customer)
ALTER TABLE tbl_users ADD COLUMN IF NOT EXISTS user_type VARCHAR(50) DEFAULT 'Admin';
ALTER TABLE tbl_users ADD COLUMN IF NOT EXISTS customer VARCHAR(255) NULL;
