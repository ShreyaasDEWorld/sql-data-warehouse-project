select * from bronze.erp_loc_a101
---check1 on cid
select 
replace(cid,'-','') as cid,
cntry
from bronze.erp_loc_a101
where replace(cid,'-','') not in(
select cst_key from silver.crm_cust_info
)

--check 2 on cntry
select distinct 
case when TRIM(cntry) in ('US','USA') then 'United States'
	 when TRIM(cntry) in ('DE') then 'Germany'
	 when TRIM(cntry) iS null or TRIM(cntry) ='' then 'n/a'
	 else TRIM(cntry)
end as  cntry
from bronze.erp_loc_a101


INSERT INTO SILVER.erp_loc_a101(
cid,
cntry
)
SELECT 
replace(cid,'-','') as cid,
case when TRIM(cntry) in ('US','USA') then 'United States'
	 when TRIM(cntry) in ('DE') then 'Germany'
	 when TRIM(cntry) iS null or TRIM(cntry) ='' then 'n/a'
	 else TRIM(cntry)
end as  cntry
FROM bronze.erp_loc_a101

select * from silver.erp_loc_a101
