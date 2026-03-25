/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.LOAD_Bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY

        SET @batch_start_time = GETDATE();

        PRINT '=========================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '=========================';

        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.crm_cus_info TABLE';
        TRUNCATE TABLE bronze.crm_cus_info;
        PRINT '>>Inserting Data Into bronze.crm_cus_info TABLE';
        BULK INSERT bronze.crm_cus_info 
        FROM '/var/opt/mssql/cust_info.csv'
        WITH ( 
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT concat('>> Load duration : ',datediff(second,@start_time,@end_time),' second');
        PRINT '=========================';

        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.crm_prd_info TABLE';
        TRUNCATE Table bronze.crm_prd_info;
        PRINT '>>Inserting Data Into bronze.crm_prd_info TABLE';
        BULK INSERT bronze.crm_prd_info 
        FROM '/var/opt/mssql/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );

        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT ('>> Load duration :',DATEDIFF(second,@start_time,@end_time),' second');
        PRINT '=========================';

        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.crm_sales_details TABLE';
        TRUNCATE TABLE bronze.crm_sales_details
        PRINT '>>Inserting Data Into bronze.crm_sales_details TABLE';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );

        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT ('>> Load duration :',DATEDIFF(second,@start_time,@end_time),' second');
        PRINT '=========================';
        
        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.erp_CUST_AZ12 TABLE';
        TRUNCATE TABLE bronze.erp_CUST_AZ12;
        PRINT '>>Inserting Data Into bronze.erp_CUST_AZ12 TABLE';
        BULK INSERT bronze.erp_CUST_AZ12
        FROM '/var/opt/mssql/CUST_AZ12.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT('>> Load duration',datediff(second,@start_time,@end_time),'Second')
        PRINT '=========================';
        
        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.erp_LOC_A101 TABLE';
        TRUNCATE TABLE bronze.erp_LOC_A101;
        PRINT '>>Inserting Data Into bronze.erp_LOC_A101 TABLE';
        BULK INSERT bronze.erp_LOC_A101
        FROM '/var/opt/mssql/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT('>> Load duration',datediff(second,@start_time,@end_time),'Second')
        PRINT '=========================';

        SET @start_time = GETDATE();
        PRINT '>>Truncate bronze.erp_PX_CAT_G1V2 TABLE';
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
        PRINT '>>Truncate bronze.erp_PX_CAT_G1V2';
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT('>> Load duration',datediff(second,@start_time,@end_time),'Second')
        PRINT '=========================';

        SET @batch_end_time = GETDATE();
        PRINT '=========================';
        PRINT CONCAT('>> Total Load duration <<', DATEDIFF(Second,@batch_start_time,@batch_end_time),'Second');
        PRINT '=========================';
        
    END TRY
    BEGIN CATCH
        PRINT 'NOTICE!!';
        PRINT 'Error message' + Error_message();
        PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state' + CAST(Error_state()AS NVARCHAR);
        PRINT 'Error line' + Error_line();
        PRINT 'Error severity' + Error_severity();
    END CATCH
END;
