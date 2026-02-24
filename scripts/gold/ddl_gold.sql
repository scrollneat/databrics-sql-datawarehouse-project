SELECT 
cc.cst_id AS CUSTOMER_ID,
cc.cst_key AS CUSTOMER_NUMBER,
cc.cst_firstname AS FIRSTNAME,
cc.cst_lastname AS LAST_NAME,
cc.cst_marital_status AS MARITAL_STATUS, 
cc.cst_gndr AS GENDER,
cc.cst_create_date AS CREATE_DATE,
ec.cid ,
ec.bdate AS BIRTH_DATE,
ec.gen,
el.cid,
el.cntry AS COUNTRY
FROM datawarehouse.silver.crm_cust_info cc
LEFT JOIN datawarehouse.silver.erp_cust_az12 ec ON cc.cst_id = CAST(ec.cid AS STRING)
LEFT JOIN datawarehouse.silver.erp_loc_a101 el ON el.cid = cc.cst_id;
