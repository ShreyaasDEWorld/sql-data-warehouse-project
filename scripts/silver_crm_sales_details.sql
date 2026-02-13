insert into silver.crm_sales_details (
sls_ord_num, 
sls_prd_key, 
sls_cust_id, 
sls_order_dt, 
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
)
SELECT 
sls_ord_num, 
sls_prd_key, 
sls_cust_id, 
--sls_order_dt, 
case when sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
	 ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	 END sls_order_dtm,
--sls_ship_dt,
case when sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
	 ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	 END sls_ship_dt,
--sls_due_dt, 
case when sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
	 ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	 END sls_due_dt,
case  when sls_sales <= 0 OR sls_sales is null OR sls_sales != sls_quantity * ABS(sls_price)
	  THEN sls_quantity * ABS(sls_price)
	  Else sls_sales
End as sls_sales,
sls_quantity, 
CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_price
 END AS sls_price
FROM bronze.crm_sales_details;


---Below are data quailty check on date column and sls_sales,sls_price,sls_quantity.
select * from 
bronze.crm_sales_details
where sls_order_dt <= 0 oR sls_order_dt is null;

SELECT 
    sls_ord_num,
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt = 0 OR sls_order_dt is null

select * ,
LENGTH(CAST(sls_order_dt AS TEXT)) as sls_order_dt
from bronze.crm_sales_details

SELECT sls_ord_num, 
sls_prd_key, 
sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price, dwh_create_date
	FROM silver.crm_sales_details;


SELECT 
    sls_order_dt,
    TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') AS converted_date
FROM bronze.crm_sales_details
where sls_ord_num='SO43697'
where sls_prd_key in (
select prd_key from silver.crm_prd_info);

where sls_ord_num != TRIM(sls_ord_num);


select 
TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') AS sls_order_dt,
TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') AS sls_ship_dt,
TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') AS sls_due_dt
from 
bronze.crm_sales_details
--where sls_sales <= 0
--where sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT 
sls_ord_num, 
sls_prd_key, 
sls_cust_id, 
case when sls_order_dt <= 0  or LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN null
	 else TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
End as sls_order_dt,
case when sls_ship_dt <= 0  or LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN null
	 else TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
End as sls_ship_dt,
case when sls_due_dt <= 0  or LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN null
	 else TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
End as sls_due_dt,
sls_sales, 
sls_quantity, 
sls_price
FROM bronze.crm_sales_details;


---Quality check for date
SELECT 
    ---LENGTH(CAST(sls_order_dt AS TEXT))
	--sls_order_dt
	nullif(sls_order_dt,0) as sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt <= 0  or LENGTH(CAST(sls_order_dt AS TEXT)) != 8 ;

select
case when sls_order_dt <= 0  or LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN null
	 else TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
End as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0  or LENGTH(CAST(sls_order_dt AS TEXT)) != 8 ; 

---Business Rule sales= qty* price
--Should not be 0,null,negative or not allowed
select 
sls_sales as old_sls_sales,
sls_quantity as old_sls_quantity,
sls_price as old_sls_price,
case  when sls_sales <= 0 OR sls_sales is null OR sls_sales != sls_quantity * ABS(sls_price)
	  THEN sls_quantity * ABS(sls_price)
	  Else sls_sales
End as sls_sales,
 CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_price
 END AS sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0;

SELECT 
    CASE 
        WHEN sls_price <= 0 OR sls_price IS NULL
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

--Post data load  quality check
select *
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0;


select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt


