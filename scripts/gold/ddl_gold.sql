/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

CREATE OR REPLACE VIEW datawarehouse.gold.dim_customers as
SELECT 
  row_number() over (order by cc.cst_key) AS CUSTOMER_KEY,
  cc.cst_id AS CUSTOMER_ID,
  cc.cst_key AS CUSTOMER_NUMBER,
  cc.cst_firstname AS FIRSTNAME,
  cc.cst_lastname AS LAST_NAME,
  el.cntry AS COUNTRY,
  cc.cst_marital_status AS MARITAL_STATUS, 
  CASE 
    WHEN cc.cst_gndr != 'NA' THEN cc.cst_gndr -- CRM is the primary source for gender
    ELSE COALESCE(ec.gen, 'NA') -- Fallback to ERP data
  END AS GENDER,
  ec.bdate AS BIRTH_DATE,
  cc.cst_create_date AS CREATE_DATE
FROM datawarehouse.silver.crm_cust_info cc
LEFT JOIN datawarehouse.silver.erp_cust_az12 ec ON cc.cst_key = ec.cid
LEFT JOIN datawarehouse.silver.erp_loc_a101 el ON el.cid = cc.cst_key;


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
CREATE OR REPLACE VIEW datawarehouse.gold.dim_products as
SELECT 
row_number() over(order by cp.prd_key, cp.prd_start_dt) as PRODUCT_KEY,
cp.prd_id AS PRODUCT_ID,
cp.prd_key AS PRODUCT_NUMBER,
cp.prd_nm AS PRODUCT_NAME,
cp.cat_id AS CATEGORY_ID,
ec.cat AS CATEGORY,
ec.subcat AS SUBCATEGORY,
ec.maintenance,
cp.prd_cost AS COST,
cp.prd_line AS PRODUCT_LINE,
cp.prd_start_dt AS START_DATE
FROM
datawarehouse.silver.crm_prd_info cp
LEFT JOIN datawarehouse.silver.erp_px_cat_g1v2 ec ON ec.id = cp.cat_id
WHERE cp.prd_end_dt IS NULL; -- Filter out all historical data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
CREATE OR REPLACE VIEW datawarehouse.gold.fact_sales as
SELECT 
cs.sls_ord_num AS ORDER_NUMBER,
cs.sls_prd_key AS PRODUCT_KEY,
cs.sls_cust_id AS CUSTOMER_ID,
cs.sls_order_dt AS ORDER_DATE,
cs.sls_ship_dt AS SHIP_DATE, 
cs.sls_due_dt AS DUE_DATE,
cs.sls_price AS PRICE,
cs.sls_quantity AS QUANTITY,
cs.sls_sales AS SALES_AMOUNT
FROM
datawarehouse.silver.crm_sales_details cs
LEFT JOIN datawarehouse.gold.dim_products pr
    ON cs.sls_prd_key = pr.product_number
LEFT JOIN datawarehouse.gold.dim_customers cu
    ON cs.sls_cust_id = cu.customer_id

