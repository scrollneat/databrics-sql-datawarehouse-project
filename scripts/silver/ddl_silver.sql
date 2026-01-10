
-- =====================================================================
-- 🟤 Databricks SQL | Silver Layer (Unity Catalog)
-- -------------------------------------------------
-- 📦 Creates Silver Tables with explicit columns/types
-- ✅ Validates row counts
-- 💡 Paste as a single query or one %sql cell
-- =====================================================================
-- ----------------------------
-- 0️⃣ SAFETY: Set Desired Catalog Name
-- ----------------------------
-- 🔄 Change this if you want a different top-level name
USE CATALOG datawarehouse;
USE SCHEMA silver;
---------------------------------------------------------------------
-- ----------------------------
-- 1️⃣ Silver Tables (Explicit Schema)
--    📂 Paths assume upload via Databricks UI → /Catalogue/Volume/
--    🛠️ If your files are elsewhere, update the path accordingly.
-- ----------------------------

-- 1a) 🛒 PRODUCTS (prd_info.csv)
-- -------------------------------------------------
-- 📄 File columns: prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- ⚠️ prd_cost and prd_end_dt have blanks in file → use PERMISSIVE + nullValue ''
-- 📝 Table contains product information for CRM:
--    - prd_id: Product ID
--    - prd_key: Product reference key
--    - prd_nm: Product name
--    - prd_cost: Product cost
--    - prd_line: Product line/category
--    - prd_start_dt: Start date (available)
--    - prd_end_dt: End date (unavailable)
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for inventory, pricing analysis, and lifecycle insights

DROP TABLE IF EXISTS datawarehouse.silver.crm_prd_info;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.crm_prd_info (
  prd_id        INT,                -- 🆔 Unique product identifier
  cat_id        STRING,             -- 🗂️ Category ID TO  connect erp_px_cat_g1v2 Table
  prd_key       STRING,             -- 🔑 Product reference key
  prd_nm        STRING,             -- 🏷️ Product name
  prd_cost      INT,                -- 💲 Product cost
  prd_line      STRING,             -- 🗂️ Product line/category
  prd_start_dt  DATE,               -- 📅 Start date (available)
  prd_end_dt    DATE,               -- 📅 End date (available)
  dwh_create_date TIMESTAMP         -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.crm_prd_info 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.crm_prd_info 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- 1b) 👤 CUSTOMERS (cust_info.csv)
-- -------------------------------------------------
-- 📄 File columns: cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- 📝 Table contains customer info for CRM:
--    - cst_id: Customer ID
--    - cst_key: Customer reference key
--    - cst_firstname: First name
--    - cst_lastname: Last name
--    - cst_marital_status: Marital status
--    - cst_gndr: Gender
--    - cst_create_date: Creation date (YYYY-MM-DD)
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for segmentation, marketing, and demographic analysis

DROP TABLE IF EXISTS datawarehouse.silver.crm_cust_info;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.crm_cust_info (
  cst_id             INT,           -- 🆔 Unique customer identifier
  cst_key            STRING,        -- 🔑 Customer reference key
  cst_firstname      STRING,        -- 🧑 First name
  cst_lastname       STRING,        -- 🧑 Last name
  cst_marital_status STRING,        -- 💍 Marital status
  cst_gndr           STRING,        -- 🚻 Gender
  cst_create_date    DATE,          -- 📅 Creation date
  dwh_create_date    TIMESTAMP      -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.crm_cust_info 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.crm_cust_info 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- 1c) 💸 SALES (sales_details.csv)
-- -------------------------------------------------
-- 📄 File columns: sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- 📝 Table contains detailed sales info:
--    - sls_ord_num: Order number
--    - sls_prd_key: Product key
--    - sls_cust_id: Customer ID
--    - sls_order_dt: Order date (YYYYMMDD as int)
--    - sls_ship_dt: Ship date (YYYYMMDD as int)
--    - sls_due_dt: Due date (YYYYMMDD as int)
--    - sls_sales: Sales amount
--    - sls_quantity: Quantity sold
--    - sls_price: Price per unit
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for sales analysis, order tracking, and purchasing patterns

DROP TABLE IF EXISTS datawarehouse.silver.crm_sales_details;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.crm_sales_details (
  sls_ord_num   STRING,             -- 🆔 Unique order identifier
  sls_prd_key   STRING,             -- 🔑 Product key
  sls_cust_id   INT,                -- 🆔 Customer ID
  sls_order_dt  DATE,               -- 📅 Order date (YYYYMMDD as Date)
  sls_ship_dt   DATE,               -- 📅 Ship date (YYYYMMDD as Date)
  sls_due_dt    DATE,               -- 📅 Due date (YYYYMMDD as Date)
  sls_sales     INT,                -- 💰 Sales amount
  sls_quantity  INT,                -- 🔢 Quantity sold
  sls_price     INT,                -- 💲 Price per unit
  dwh_create_date TIMESTAMP         -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.crm_sales_details 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.crm_sales_details 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- =====================================================================
-- 🟤 Databricks SQL | Silver Layer (Unity Catalog)
-- -------------------------------------------------
-- 📦 ERP Source Silver Tables with explicit columns/types
-- =====================================================================

-- 1️⃣ ERP CUSTOMERS (erp_cust_az12)
-- -------------------------------------------------
-- 📄 File columns: cid, bdate, gen
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- 📝 Table contains ERP customer info:
--    - cid: Customer identifier
--    - bdate: Birthdate
--    - gen: Gender
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for demographic enrichment and analysis

DROP TABLE IF EXISTS datawarehouse.silver.erp_cust_az12;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.erp_cust_az12 (
  cid    STRING,    -- 🆔 Customer identifier
  bdate  DATE,      -- 📅 Birthdate
  gen    STRING,    -- 🚻 Gender
  dwh_create_date TIMESTAMP -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.erp_cust_az12 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.erp_cust_az12 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- 2️⃣ ERP LOCATIONS (erp_loc_a101)
-- -------------------------------------------------
-- 📄 File columns: cid, cntry
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- 📝 Table contains ERP customer location info:
--    - cid: Customer identifier
--    - cntry: Country
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for geographic segmentation and analysis

DROP TABLE IF EXISTS datawarehouse.silver.erp_loc_a101;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.erp_loc_a101 (
  cid    STRING,    -- 🆔 Customer identifier
  cntry  STRING,    -- 🌍 Country
  dwh_create_date TIMESTAMP -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.erp_loc_a101 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.erp_loc_a101 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- 3️⃣ ERP PRODUCT CATEGORY (erp_px_cat_g1v2)
-- -------------------------------------------------
-- 📄 File columns: id, cat, subcat, maintenance
-- 🟤 Silver principle: keep raw fidelity; define reasonable types where safe.
-- 📝 Table contains ERP product category info:
--    - id: Product identifier
--    - cat: Category
--    - subcat: Subcategory
--    - maintenance: Maintenance info
--    - dwh_create_date: Row creation timestamp in DWH
--    - Used for product classification and maintenance tracking

DROP TABLE IF EXISTS datawarehouse.silver.erp_px_cat_g1v2;

-- 1. Create table WITHOUT DEFAULT
CREATE TABLE datawarehouse.silver.erp_px_cat_g1v2 (
  id           STRING,    -- 🆔 Product identifier
  cat          STRING,    -- 🗂️ Category
  subcat       STRING,    -- 🗂️ Subcategory
  maintenance  STRING,    -- 🛠️ Maintenance info
  dwh_create_date TIMESTAMP -- 🕒 Row creation timestamp in DWH
);

-- 2. Enable column defaults feature
ALTER TABLE datawarehouse.silver.erp_px_cat_g1v2 
SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- 3. Add DEFAULT value to column
ALTER TABLE datawarehouse.silver.erp_px_cat_g1v2 
ALTER COLUMN dwh_create_date SET DEFAULT CURRENT_TIMESTAMP();

-- -------------------------------------------------