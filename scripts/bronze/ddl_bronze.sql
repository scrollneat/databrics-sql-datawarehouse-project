
-- =====================================================================
-- 🟤 Databricks SQL | Bronze Layer (Unity Catalog)
-- -------------------------------------------------
-- 📦 Creates Bronze Tables with explicit columns/types
-- ✅ Validates row counts
-- 💡 Paste as a single query or one %sql cell
-- =====================================================================

-- ----------------------------
-- 0️⃣ SAFETY: Set Desired Catalog Name
-- ----------------------------
-- 🔄 Change this if you want a different top-level name
USE CATALOG datawarehouse;
USE SCHEMA bronze;


-- ----------------------------
-- 1️⃣ Bronze Tables (Explicit Schema)
--    📂 Paths assume upload via Databricks UI → /Catalogue/Volume/
--    🛠️ If your files are elsewhere, update the path accordingly.
-- ----------------------------

-- 1a) 🛒 PRODUCTS (prd_info.csv)
-- -------------------------------------------------
-- 📄 File columns: prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
-- 🟤 Bronze principle: keep raw fidelity; define reasonable types where safe.
-- ⚠️ prd_cost and prd_end_dt have blanks in file → use PERMISSIVE + nullValue ''
-- 📝 Table contains product information for CRM:
--    - Product ID, name, cost, and lifecycle dates
--    - Used for inventory, pricing analysis, and lifecycle insights

-- Drop the table if it exists
DROP TABLE IF EXISTS datawarehouse.bronze.crm_prd_info;

-- Create the table

CREATE TABLE datawarehouse.bronze.crm_prd_info (
  prd_id        INT,                -- 🆔 Unique product identifier
  prd_key       STRING,             -- 🔑 Product reference key
  prd_nm        STRING,             -- 🏷️ Product name
  prd_cost      INT,                -- 💲 Product cost
  prd_line      STRING,             -- 🗂️ Product line/category
  prd_start_dt  DATE,               -- 📅 Start date (available)
  prd_end_dt    DATE                -- 📅 End date (unavailable)
);


-- 1b) 👤 CUSTOMERS (cust_info.csv)
-- -------------------------------------------------
-- 📄 File columns: cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
-- 🗓️ cst_create_date: YYYY-MM-DD in file
-- 📝 Table contains customer info for CRM:
--    - Customer ID, name, marital status, gender, creation date
--    - Used for segmentation, marketing, and demographic analysis

-- Drop the table if it exists
DROP TABLE IF EXISTS datawarehouse.bronze.crm_cust_info;

-- Create the table
CREATE TABLE datawarehouse.bronze.crm_cust_info (
  cst_id             INT,           -- 🆔 Unique customer identifier
  cst_key            STRING,        -- 🔑 Customer reference key
  cst_firstname      STRING,        -- 🧑 First name
  cst_lastname       STRING,        -- 🧑 Last name
  cst_marital_status STRING,        -- 💍 Marital status
  cst_gndr           STRING,        -- 🚻 Gender
  cst_create_date    DATE           -- 📅 Creation date
);


-- 1c) 💸 SALES (sales_details.csv)
-- -------------------------------------------------
-- 📄 File columns: sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
-- 📆 Dates: numeric strings YYYYMMDD in file; keep as INT in bronze.
--    (Convert to DATE in Silver via to_date(string, "yyyyMMdd").)
-- 📝 Table contains detailed sales info:
--    - Order numbers, product keys, customer IDs, dates, sales, quantities, prices
--    - Used for sales analysis, order tracking, and purchasing patterns

-- Drop the table if it exists
DROP TABLE IF EXISTS datawarehouse.bronze.crm_sales_details;

-- Create the table
CREATE TABLE datawarehouse.bronze.crm_sales_details (
  sls_ord_num   STRING,             -- 🆔 Unique order identifier
  sls_prd_key   STRING,             -- 🔑 Product key
  sls_cust_id   INT,                -- 🆔 Customer ID
  sls_order_dt  INT,                -- 📅 Order date (YYYYMMDD as int)
  sls_ship_dt   INT,                -- 📅 Ship date (YYYYMMDD as int)
  sls_due_dt    INT,                -- 📅 Due date (YYYYMMDD as int)
  sls_sales     INT,                -- 💰 Sales amount
  sls_quantity  INT,                -- 🔢 Quantity sold
  sls_price     INT                 -- 💲 Price per unit
);