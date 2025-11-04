/*
===============================================================================
Stored Procedure: Load Bronze Layer (Sources -> Bronze)
===============================================================================
WHY?
Load all source data into the 'bronze' schema

HOW?
- truncate all tables in the schema before loading the source data
- collect and print duration for each table load
- handle errors gracefully and print relevant error messages if any issues occur

SAMPLE EXECUTION:

`EXEC bronze.load_bronze;`

================================================
Loading Bronze Layer
================================================
 
Loading CRM Tables
------------------------------------------------
>> Truncating Table: bronze.crm_cust_info
>> Inserting Data Into: bronze.crm_cust_info

(18493 rows affected)
(Load Duration: 70 milliseconds)
 
... all other tables ...
 
==========================================
Loading Bronze Layer is Completed
Total Load Duration: 324 milliseconds
==========================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE
		@start_time DATETIME,
		@end_time DATETIME,
		@batch_start_time DATETIME,
		@batch_end_time DATETIME,
		@path_to_source NVARCHAR(500),
		@bulk_insert_sql NVARCHAR(MAX);
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';

		SET @path_to_source = dbo.GetSourceFilePath('source_crm', 'cust_info.csv');

		SET @bulk_insert_sql = N'BULK INSERT bronze.crm_cust_info FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;

		SET @end_time = GETDATE();

		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';

		PRINT '';

        SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		
		SET @path_to_source = dbo.GetSourceFilePath('source_crm', 'prd_info.csv');

		SET @bulk_insert_sql = N'BULK INSERT bronze.crm_prd_info FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;

		SET @end_time = GETDATE();

		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
		PRINT '';

        SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';

		SET @path_to_source = dbo.GetSourceFilePath('source_crm', 'sales_details.csv');
		
		SET @bulk_insert_sql = N'BULK INSERT bronze.crm_sales_details FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;
		
		SET @end_time = GETDATE();

		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
		PRINT '';

		PRINT '';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

		SET @path_to_source = dbo.GetSourceFilePath('source_erp', 'LOC_A101.csv');
		
		SET @bulk_insert_sql = N'BULK INSERT bronze.erp_loc_a101 FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;
		
		SET @end_time = GETDATE();
		
		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
		PRINT '';

		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

		SET @path_to_source = dbo.GetSourceFilePath('source_erp', 'CUST_AZ12.csv');

		SET @bulk_insert_sql = N'BULK INSERT bronze.erp_cust_az12 FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;

		SET @end_time = GETDATE();

		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
		PRINT '';

		SET @start_time = GETDATE();
		
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

		SET @path_to_source = dbo.GetSourceFilePath('source_erp', 'PX_CAT_G1V2.csv');
		
		SET @bulk_insert_sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @path_to_source + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)';
		EXEC sp_executesql @bulk_insert_sql;
		
		SET @end_time = GETDATE();

		PRINT '(Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds)';
		PRINT '';

		SET @batch_end_time = GETDATE();
		
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
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
END;
