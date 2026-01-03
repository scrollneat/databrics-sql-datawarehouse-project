SELECT cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS  cst_lastname,
CASE WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
     WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
     ELSE TRIM(UPPER(cst_marital_status)) END AS cst_marital_status,
CASE WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
     WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
     ELSE 'NA' END AS cst_gndr,
cst_create_date
FROM (
SELECT 
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date,
RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS LATEST_FLAG
FROM datawarehouse.bronze.crm_cust_info) WHERE LATEST_FLAG = 1 ;

SELECT 
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as prd_end_dt
FROM 
(SELECT 
prd_id,
REPLACE(substring(prd_key, 1, 5) , '-', '_') AS cat_id,
substring(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE 
    WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
    WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
    WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other'
    WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
    ELSE 'NA'
END AS prd_line,
prd_start_dt,
prd_end_dt 
FROM datawarehouse.bronze.crm_prd_info);

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
     WHEN length(sls_order_dt) != 8 THEN NULL
     ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
END AS 
sls_order_dt,
CASE 
     WHEN length(sls_ship_dt) != 8 THEN NULL
     ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
END AS 
sls_ship_dt,
CASE 
     WHEN length(sls_due_dt) != 8 THEN NULL
     ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
END AS
sls_due_dt ,
CASE 
     WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price) 
     THEN sls_quantity * abs(sls_price)
     ELSE sls_sales
END AS
sls_sales ,
sls_quantity,
CASE 
     WHEN sls_price IS NULL OR sls_price <=0 
     THEN abs(sls_sales) / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS
sls_price  
from datawarehouse.bronze.crm_sales_details ;
