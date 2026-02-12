
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
BEGIN
	-- Capture start time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Starting Bronze Layer Load...%',v_start_time;
			
    RAISE NOTICE '--------------------------------------------------------------';
	RAISE NOTICE 'Starting ERP Customer Table erp_cust_az12 Bronze Layer Load...';
		
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 (cid, bdate, gen)
    FROM 'D:/WareHouseProject/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;

	RAISE NOTICE 'Ending ERP Customer Table erp_cust_az12 Bronze Layer Load.....';
	RAISE NOTICE '--------------------------------------------------------------';
	
    
	RAISE NOTICE 'Starting ERP Customer Table erp_loc_a101 Bronze Layer Load...';
	
	
	TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 (cid, cntry)
    FROM 'D:/WareHouseProject/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;
	
	RAISE NOTICE 'Ending ERP Customer Table erp_loc_a101 Bronze Layer Load...';
	RAISE NOTICE '--------------------------------------------------------------';

    RAISE NOTICE 'Starting ERP Customer Table erp_px_cat_g1v2 Bronze Layer Load...';
	
	
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    FROM 'D:/WareHouseProject/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;

	RAISE NOTICE 'Ending ERP Customer Table erp_px_cat_g1v2 Bronze Layer Load...';
	RAISE NOTICE '--------------------------------------------------------------';
	
   	RAISE NOTICE 'Bronze Layer Load Completed Successfully...%',v_start_time;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during Bronze Load: %', SQLERRM;
        RAISE;
END;
$$;

call bronze.load_bronze();
