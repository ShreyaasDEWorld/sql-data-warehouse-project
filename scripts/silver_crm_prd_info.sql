insert into silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT prd_id, 
REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm, 
COALESCE(prd_cost, 0) AS prd_cost, 
case when Upper(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 when Upper(TRIM(prd_line)) = 'R' THEN 'Road'
	 when Upper(TRIM(prd_line)) = 'S' THEN 'Other sales'
	 when Upper(TRIM(prd_line)) = 'T' THEN 'Trouring'
	 else 'n/a'
End as prd_line,
(prd_start_dt) :: DATE as prd_start_dt, 
(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-INTERVAL '1 day') :: DATE AS prd_end_dt
FROM bronze.crm_prd_info
