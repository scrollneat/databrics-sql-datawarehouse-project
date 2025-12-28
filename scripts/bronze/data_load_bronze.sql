-- ╔════════════════════════════════════════════════════════════════════════════════════════════╗
-- ║                               🚀 Bronze Table Data Load 🚀                                ║
-- ║   Insert data from staged CSV files into bronze tables, casting columns to correct types   ║
-- ║   and skipping header rows. Each statement loads a specific entity from the volume.        ║
-- ╚════════════════════════════════════════════════════════════════════════════════════════════╝
------------------------------------------------------------------------------------------------------------------
INSERT INTO datawarehouse.bronze.crm_prd_info
SELECT 
  CAST(_c0 AS INT) AS prd_id,
  _c1 AS prd_key,
  _c2 AS prd_nm,
  CAST(_c3 AS DECIMAL(18,2)) AS prd_cost,
  _c4 AS prd_line,
  CAST(_c5 AS DATE) AS prd_start_dt,
  CAST(_c6 AS DATE) AS prd_end_dt
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/prd_info.csv`
WHERE _c0 != 'prd_id';

------------------------------------------------------------------------------------------------------------------

INSERT INTO datawarehouse.bronze.crm_cust_info
SELECT 
  CAST(_c0 AS INT) AS cst_id,
  _c1 AS cst_key,
  _c2 AS cst_firstname,
  _c3 AS cst_lastname,
  _c4 AS cst_marital_status,
  _c5 AS cst_gndr,
  CAST(_c6 AS DATE) AS cst_create_date
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/cust_info.csv`
WHERE _c1 != 'cst_key';

------------------------------------------------------------------------------------------------------------------

INSERT INTO datawarehouse.bronze.crm_sales_details
SELECT 
  _c0 AS sls_ord_num,
  _c1 AS sls_prd_key,
  CAST(_c2 AS INT) AS sls_cust_id,
  CAST(_c3 AS INT) AS sls_order_dt,
  CAST(_c4 AS INT) AS sls_ship_dt,
  CAST(_c5 AS INT) AS sls_due_dt,
  CAST(_c6 AS INT) AS sls_sales,
  CAST(_c7 AS INT) AS sls_quantity,
  CAST(_c8 AS INT) AS sls_price
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/sales_details.csv`
WHERE _c0 != 'sls_ord_num';

------------------------------------------------------------------------------------------------------------------

INSERT INTO datawarehouse.bronze.erp_cust_az12
SELECT 
  _c0 AS cid,
  CAST(_c1 AS DATE) as bdate,
  _c2 AS gen
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/CUST_AZ12.csv`
WHERE _c0 != 'CID';

------------------------------------------------------------------------------------------------------------------

INSERT INTO datawarehouse.bronze.erp_loc_a101
SELECT 
  _c0 AS cid,
  _c1 AS cntry
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/LOC_A101.csv`
WHERE _c0 != 'CID';

------------------------------------------------------------------------------------------------------------------

INSERT INTO datawarehouse.bronze.erp_px_cat_g1v2
SELECT 
  _c0 AS id,
  _c1 AS cat,
  _c2 AS subcat,
  _c3 AS maintenance
FROM csv.`/Volumes/datawarehouse/bronze/bronze_stage/PX_CAT_G1V2.csv`
WHERE _c0 != 'ID';

------------------------------------------------------------------------------------------------------------------