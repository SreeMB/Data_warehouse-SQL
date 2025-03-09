/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
-- Command to connect to the DB: DataWareHoues
-- use DataWarehouse

-- Command to Execute the Stroed Procedure to load the data into Bronze Layer
-- EXEC bronze.load_bronze

-- Create a Stored Procedure

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
				-------------------------------------------------------------------------------------------------------------
				-- CUST INFO TABLE

				-- We truncate and then load the data as this is our choosen method of LOAD.
				SET @batch_start_time = GETDATE();
				PRINT '=============================================';
				PRINT 'Loading the Bronze layer';
				PRINT '=============================================';

				PRINT '---------------------------------------------';
				PRINT 'Loading the CRM Tables';
				PRINT '---------------------------------------------';

				SET @start_time =GETDATE();
				PRINT '>> Truncating Table: bronze.crm_cust_info ';
				TRUNCATE TABLE bronze.crm_cust_info;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.crm_cust_info ';
				BULK INSERT bronze.crm_cust_info
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				SET @end_time = GETDATE();
				PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';

				----------------------------------------------------------------------------------------------------------------
				-- PRODUCT INFO TABLE

				-- We truncate and then load the data as this is our choosen method of LOAD.
				set @start_time = GETDATE();
				PRINT '>> Truncating Table: bronze.crm_prd_info ';
				TRUNCATE TABLE bronze.crm_prd_info;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.crm_prd_info ';
				BULK INSERT bronze.crm_prd_info
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				set @end_time = GETDATE();
				PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';

				----------------------------------------------------------------------------------------------------------------------
				-- SALES DETAILS TABLE

				-- We truncate and then load the data as this is our choosen method of LOAD.
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: bronze.crm_sales_details ';
				TRUNCATE TABLE bronze.crm_sales_details;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.crm_sales_details ';
				BULK INSERT bronze.crm_sales_details
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				SET @end_time = GETDATE();
				PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';
				---------------------------------------------------------------------------------------------------------------------
				PRINT '---------------------------------------------';
				PRINT 'Loading the ERP Tables';
				PRINT '---------------------------------------------';
				-- CUSTOMER ATOZ TABLE

				-- We truncate and then load the data as this is our choosen method of LOAD.
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: bronze.erp_cust_az12 ';
				TRUNCATE TABLE bronze.erp_cust_az12;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.erp_cust_az12 ';
				BULK INSERT bronze.erp_cust_az12
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				SET @end_time = GETDATE();
				PRINT 'LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
				PRINT '---------------------------------------------';	

				---------------------------------------------------------------------------------------------------------------------
				-- LOCATION TABLE


				-- We truncate and then load the data as this is our choosen method of LOAD.
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: bronze.erp_loc_a101 ';
				TRUNCATE TABLE bronze.erp_loc_a101;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.erp_loc_a101 ';
				BULK INSERT bronze.erp_loc_a101
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				SET @end_time = GETDATE();
				PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';	
	
				------------------------------------------------------------------------------------------------------------------------
				-- PRODUCT CATEGORY TABLE

				-- We truncate and then load the data as this is our choosen method of LOAD.
				SET @start_time= GETDATE();
				PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2 ';
				TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
				-- BULK INSERTION of the data into DB
				PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2 ';
				BULK INSERT bronze.erp_px_cat_g1v2
				FROM 'C:\Git_Projects\Data_warehouse\data\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				WITH (

					FIRSTROW =2,  -- Skipping the header and starting from the data directly
					FIELDTERMINATOR = ',',
					TABLOCK            -- lOCKING THE ENTIRE TABLE DURING LOADING
	
					);
				SET @end_time=GETDATE();
				PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';	
				SET @batch_end_time = GETDATE();
				PRINT '>>LOAD DURATION OF BRONZE LAYER: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		
		END TRY
		
		BEGIN CATCH
				
				PRINT '=============================================';
				PRINT 'Error occured during loading the bronze layer';
				PRINT 'Error Message'+ERROR_MESSAGE();
				PRINT 'Error Number'+CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error State'+CAST(ERROR_STATE() AS NVARCHAR);
				PRINT '=============================================';

		END CATCH
		

END
