
-- =====================================================================
-- Databricks SQL - Single Script: Bronze Layer (Unity Catalog)
-- Creates Bronze Tables with explicit columns/types
-- and validates row counts. Paste this as a single query or one %sql cell.
-- =====================================================================

-- ----------------------------
-- 0) SAFETY: Set desired catalog name
-- ----------------------------
-- Change this if you want a different top-level name
USE CATALOG datawarehouse;
USE SCHEMA bronze;


-- ----------------------------
-- 1) Bronze Tables (explicit schema)
--    Paths assume you uploaded via Databricks UI → /Catalogue/Volume/
--    If your files are elsewhere, update the path accordingly.
-- ----------------------------

-- 1a) PRODUCTS (prd_info.csv)
-- File columns: prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
-- Bronze principle: keep raw fidelity; define reasonable types where safe.
-- prd_cost and prd_end_dt have blanks in file → use PERMISSIVE + nullValue ''
-- Table contains product information relevant to customer relationship management.
-- Includes details such as product ID, name, cost, and the time frame during which the product is available.
-- Used for inventory management, pricing analysis, and understanding product lifecycle.

-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_prd_info;

-- Create the table

CREATE TABLE bronze.crm_prd_info (
  prd_id        INT,                -- Unique identifier for each product
  prd_key       STRING,             -- Reference key for the product
  prd_nm        STRING,             -- Name of the product
  prd_cost      DECIMAL(18,2),      -- Cost associated with the product
  prd_line      STRING,             -- Product line/category
  prd_start_dt  DATE,               -- Date when product becomes available
  prd_end_dt    DATE                -- Date when product is no longer available
)


-- 1b) CUSTOMERS (cust_info.csv)
-- File columns: cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
-- cst_create_date: YYYY-MM-DD in file
-- Table contains customer information relevant to CRM activities.
-- Includes details such as customer ID, name, marital status, gender, and the date the customer was created.
-- Used for customer segmentation, personalized marketing, and demographic analysis.

-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_cust_info;

-- Create the table
CREATE TABLE bronze.crm_cust_info (
  cst_id             INT,           -- Unique identifier for each customer
  cst_key            STRING,        -- Reference key for the customer
  cst_firstname      STRING,        -- First name of the customer
  cst_lastname       STRING,        -- Last name of the customer
  cst_marital_status STRING,        -- Marital status of the customer
  cst_gndr           STRING,        -- Gender of the customer
  cst_create_date    DATE           -- Date when customer was created in the system
)


-- 1c) SALES (sales_details.csv)
-- File columns: sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
-- Dates are numeric strings YYYYMMDD in file; keep them as STRING in bronze.
-- (You will convert to DATE in Silver via to_date(string, "yyyyMMdd").)
-- Table contains detailed sales information related to customer orders.
-- Includes order numbers, product keys, customer IDs, order dates, shipping dates, due dates, sales amounts, quantities sold, and prices.
-- Used for sales analysis, order tracking, and understanding customer purchasing patterns.

-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_sales_details;

-- Create the table
CREATE TABLE bronze.crm_sales_details (
  sls_ord_num   STRING,             -- Unique identifier for each customer order
  sls_prd_key   STRING,             -- Product key associated with the order
  sls_cust_id   INT,                -- Customer ID who placed the order
  sls_order_dt  STRING,             -- Order date (YYYYMMDD as string)
  sls_ship_dt   STRING,             -- Ship date (YYYYMMDD as string)
  sls_due_dt    STRING,             -- Due date (YYYYMMDD as string)
  sls_sales     DECIMAL(18,2),      -- Total sales amount for the order
  sls_quantity  INT,                -- Number of units sold
  sls_price     DECIMAL(18,2)       -- Price per unit sold
)


-- ----------------------------
-- 2) Validation checks
-- ----------------------------
-- Row counts
SELECT 'bronze.crm_prd_info'       AS table_name, COUNT(*) AS row_count FROM bronze.crm_prd_info
UNION ALL
SELECT 'bronze.crm_cust_info'      AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'bronze.crm_sales_details'  AS table_name, COUNT(*) AS row_count FROM bronze.crm_sales_details;

-- Sample rows
SELECT * FROM bronze.crm_prd_info       LIMIT 10;
SELECT * FROM bronze.crm_cust_info      LIMIT 10;
SELECT * FROM bronze.crm_sales_details  LIMIT 10;

-- Schema descriptions
DESC TABLE bronze.crm_prd_info;
DESC TABLE bronze.crm_cust_info;
DESC TABLE bronze.crm_sales_details;