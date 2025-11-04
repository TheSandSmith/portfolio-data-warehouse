/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
WHY?
Load and transform data from the 'bronze' schema into the 'silver' schema.

HOW?
- truncate all tables in the schema before loading the cleansed data
- perform necessary data transformations and cleansing during the insert process
- collect and print duration for each table load
- handle errors gracefully and print relevant error messages if any issues occur

SAMPLE EXECUTION:

`EXEC silver.load_silver;`

================================================
Loading Silver Layer
================================================

Loading CRM Tables
------------------------------------------------
>> Truncating Table: silver.crm_cust_info
>> Inserting Data Into: silver.crm_cust_info

(18493 rows affected)
(Load Duration: 85 seconds)

... all other tables ...

==========================================
Loading Silver Layer is Completed
Total Load Duration: 412 seconds
==========================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

        /*
        -------------------------------------------------------------------------------------------------
        Table: silver.crm_cust_info
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        - Remove unwanted spaces from all relevant string fields
        - Normalize marital status values ('S' -> 'Single', 'M' -> 'Married', others -> 'n/a')
        - Normalize gender values ('F' -> 'Female', 'M' -> 'Male', others -> 'n/a')
        - Handle null values appropriately
        - Remove duplicate records based on 'cst_id', keeping the most recent record by 'cst_create_date'
        -------------------------------------------------------------------------------------------------
        */
        SET @start_time = GETDATE();
		
        PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		
        INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
        SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_updated_rank
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) sub
		WHERE last_updated_rank = 1;

        SET @end_time = GETDATE();

        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';

        /*
        -------------------------------------------------------------------------------------------------
        Table: silver.crm_prd_info
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        - Extract 'cat_id' from 'prd_key' by taking the first 5 characters and replacing '-' with '_'
        - Extract 'prd_key' from 'prd_key' by taking the substring after the 6th character
        - Handle null values appropriately
        - Normalize product line values ('M' -> 'Mountain', 'R' -> 'Road', 'S' -> 'Other Sales', 'T' -> 'Touring', others -> 'n/a')
        - Drop the unnecessary seconds from DATETIME fields by casting to DATE (the time part was always 00:00:00 so it is not informative)
        - Re-calculate 'prd_end_dt' to be one day before the next 'prd_start_dt' for the same 'prd_key' using the LEAD() function
        -- 'prd_end_dt' could still be NULL for the latest record of each 'prd_key' which is considered acceptable in this context
        -------------------------------------------------------------------------------------------------
        */
        SET @start_time = GETDATE();
		
        PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		
        INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt
		FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();

        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';

        /*
        -------------------------------------------------------------------------------------------------
        Table: silver.crm_sales_details
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        - Convert and cast date fields from INT (YYYYMMDD) to DATE, setting invalid dates (0 or incorrect length) to NULL
        - Recalculate 'sls_sales' if original 'sls_sales' is NULL, negative, or does not match the otherwise logically-calculated value
        - Derive 'sls_price' as 'sls_sales' / 'sls_quantity' if original 'sls_price' is NULL or negative
        - Handle division by zero when calculating 'sls_price' by using NULLIF
        -------------------------------------------------------------------------------------------------
        */
        SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		
        INSERT INTO silver.crm_sales_details (
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
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        
        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';

        PRINT '';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        /*
        -------------------------------------------------------------------------------------------------
        Table: silver.erp_cust_az12
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        - Remove 'NAS' prefix from 'cid' if present
        - Set future birthdates to NULL
        - Normalize gender values ('F'/'FEMALE' -> 'Female', 'M'/'MALE' -> 'Male', others -> 'n/a')
        -------------------------------------------------------------------------------------------------
        */
        SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		
        INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12;

	    SET @end_time = GETDATE();

        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';

        /*
        -------------------------------------------------------------------------------------------------
        Table: silver.erp_loc_a101
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        - Remove dashes from 'cid'
        - Normalize country codes to trimmed, full country names
        -------------------------------------------------------------------------------------------------
        */
        SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		
        INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;

	    SET @end_time = GETDATE();

        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';
		
		/*
        -------------------------------------------------------------------------------------------------
        Table: silver.erp_px_cat_g1v2
        -------------------------------------------------------------------------------------------------
        Data Cleansing and Transformation Logic:
        No specific transformations were required; direct copy from bronze to silver
        -------------------------------------------------------------------------------------------------
        */
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		
        INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		
        SET @end_time = GETDATE();

        PRINT '(Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
        PRINT '';

		SET @batch_end_time = GETDATE();

		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
