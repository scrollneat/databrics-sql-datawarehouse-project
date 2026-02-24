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
