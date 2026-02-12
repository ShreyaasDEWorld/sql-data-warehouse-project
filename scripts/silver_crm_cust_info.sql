
---Check for null or duplicate in Primary key
---Expecetation: No result 
select 
cst_id,
count(*)
from 
bronze.crm_cust_info
group by cst_id
having count(*) >1

with data_check_cte as (
select *,
row_number() over (partition by cst_id order by cst_create_date desc ) as flag_last
from bronze.crm_cust_info 
)
select 
cst_id,
cst_key,
TRIM(cst_firstname),
TRIM(cst_firstname),
case when Upper(TRIM(cst_material_status))='S' Then 'Single'
	 when Upper(TRIM(cst_material_status))='M' Then 'Married'
	 else 'n/a'
	 End cst_material_status,
case when Upper(TRIM(cst_gender))='F' Then 'Female'
	 when Upper(TRIM(cst_gender))='M' Then 'Male'
	 else 'n/a'
	 End cst_gender,
cst_create_date
from data_check_cte where flag_last =1;


----Clean insert into data silver crm_cust_info

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
WITH data_check_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
)

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    
    CASE 
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,

    CASE 
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,

    cst_create_date

FROM data_check_cte
WHERE flag_last = 1;


--Check Unwanted Space
--Expectation : No result
select 
cst_firstname 
from 
bronze.crm_cust_info 
where cst_firstname != TRIM(cst_firstname);
--Similarly check for cst_lastname




--Data standardization and Consistency.
select distinct cst_gender
from bronze.crm_cust_info

select * from 
silver.crm_cust_info

select * from 
bronze.crm_cust_info 
