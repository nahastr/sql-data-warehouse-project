/*
=====================================================================
STORED PROCEDURE : LOAD BRONZE LAYER (Source -> Bronze)
=====================================================================
Script Purpose:
  This storedprocedure loads data into the 'bronze schema from external CSV files.
  It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Usage Example:
    EXEC bronze. load bronze;

=====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_cust_info loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL2022\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_cust_info loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'


		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_prd_info loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL2022\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_prd_info loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'

		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_sales_details  loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.crm_sales_details 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL2022\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'crm_sales_details  loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'
	
		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_cust_az12 loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL2022\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_cust_az12 loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'

		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_loc_a101 loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL2022\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_loc_a101 loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'

		SET @start_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_px_cat_g1v2 loading started'
		PRINT '============================================'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL2022\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '============================================'
		PRINT 'erp_px_cat_g1v2 loading completed'
		PRINT 'Duration : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================'
		
		SET @batch_end_time = GETDATE();
		PRINT 'Total duration : ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';

	END TRY
	BEGIN CATCH
		PRINT '--------------------------------------';
		PRINT 'Error occured while loading bronze layer';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------';
	END CATCH
	
END;
