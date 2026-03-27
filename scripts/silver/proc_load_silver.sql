CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN    
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
    SET @batch_start_time = GETDATE();
        PRINT '=========================';
        PRINT 'LOADING SIVER LAYER';
        PRINT '=========================';

    SET @start_time = GETDATE();
    PRINT '>> TRUNCATE DATA'
    TRUNCATE TABLE silver.crm_cus_info;
    PRINT '>> inserting DATA'
    INSERT INTO silver.crm_cus_info (
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
        trim(cst_firstname) AS cst_firstname,
        trim(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(trim(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(trim(cst_marital_status)) = 'M' THEN 'Maried'
            ELSE 'N/A'
        END AS cst_marital_status,
        case when UPPER(trim(cst_gndr)) = 'F' then 'Female'
            when UPPER(trim(cst_gndr)) = 'M' then 'Male'
            else 'N/A'
        END as cst_gndr,
        cst_create_date
        FROM (
                SELECT 
                *,
                ROW_NUMBER() OVER(partition BY cst_id order BY cst_create_date desc) as flag
                FROM bronze.crm_cus_info
                WHERE cst_id IS NOT NULL
            ) as T
        WHERE flag = 1;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @start_time = GETDATE();
    PRINT '>> TRUNCATE DATA'
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> inserting DATA'
    INSERT INTO silver.crm_prd_info (
        prd_id,
        category_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )

    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS category_id,
        SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'MOUNTAIN'
            WHEN 'R' THEN 'ROAD'
            WHEN '0' THEN 'OTHER SALES'
            WHEN 'T' THEN 'TOURING'
            ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(PRD_START_DT) OVER(partition BY PRD_KEY order BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info ;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @start_time = GETDATE();
    PRINT '>>TRUNCATE the DATA'
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting the DATA'
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
        SLS_ORD_NUM,
        SLS_PRD_KEY,
        SLS_CUST_ID,
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
        END AS sls_order_dt,

        CASE
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
        END AS sls_ship_dt,

        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,

        CASE WHEN
            sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN
            sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,

        sls_quantity,

        CASE WHEN 
            sls_price IS NULL OR sls_price <= 0 THEN
            sls_sales / NULLIF(sls_quantity,0) 
            ELSE SLS_PRICE
        END AS SLS_PRICE
    FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @start_time = GETDATE();
    PRINT '>> TRUNCATE the DATA'
    TRUNCATE TABLE silver.erp_CUST_AZ12 ;
    PRINT '>> inserting the DATA'
    INSERT INTO silver.erp_CUST_AZ12 (
        CID,
        BDATE,
        GEN
    )

    SELECT 
        CASE WHEN
            CID LIKE 'NAS%' THEN
            SUBSTRING(CID,4,LEN(CID))
            ELSE CID
        END AS CID,

        CASE WHEN
            BDATE > GETDATE() THEN NULL
            ELSE BDATE
        END AS BDATE,

        CASE 
            WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female'
            WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male'
            ELSE 'N/A'
        END AS GEN
    FROM bronze.erp_CUST_AZ12;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @start_time = GETDATE();
    PRINT '>>TRUNCATE the DATA'
    TRUNCATE TABLE silver.erp_LOC_A101;
    PRINT '>>Inserting the DATA'
    INSERT INTO silver.erp_LOC_A101 (
        CID,
        CNTRY
    )

    SELECT DISTINCT
        REPLACE(CID, '-', '') AS CID,
        CASE 
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'DE' THEN 'Germany'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') IN ('USA','US') THEN 'United states'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = '' OR CNTRY IS NULL THEN 'N/A'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'France' THEN 'France'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'united Kingdom' THEN 'United Kingdom'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'United states' THEN 'United States'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'Canada' THEN 'Canada'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'Germany' THEN 'Germany'
            WHEN REPLACE(REPLACE(TRIM(CNTRY), CHAR(13),''),CHAR(10),'') = 'Australia' THEN 'Australia'
            ELSE TRIM(CNTRY)
        END AS CNTRY
    FROM bronze.erp_LOC_A101
    ORDER BY CNTRY;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE Table'
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
    PRINT '>> Inserting Data'
    INSERT INTO silver.erp_PX_CAT_G1V2 (
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
    )
    SELECT 
        ID,
        CAT,
        SUBCAT,
        CASE
            WHEN REPLACE(REPLACE(TRIM(MAINTENANCE),CHAR(13),''),CHAR(10),'') = 'Yes' THEN 'Yes'
            ELSE 'No'
        END AS MAINTENANCE
    FROM bronze.erp_PX_CAT_G1V2;

        SET @end_time = GETDATE();
        PRINT '============================================================================';
        PRINT Concat('Load duration ... ', DATEDIFF(Second,@start_time,@end_time),' sec.')
        PRINT '============================================================================';

    SET @batch_end_time = GETDATE();
    PRINT '============================================================================';
    PRINT CONCAT('Total Load Duration ..',DATEDIFF(second,@batch_start_time,@batch_end_time),'second');
    PRINT '============================================================================';

    END TRY
        BEGIN CATCH
        PRINT 'NOTICE ERROR Occured!!';
        PRINT Concat('Error message', Error_message());
        PRINT Concat('Error Number', ERROR_NUMBER());
        PRINT Concat('Error state',Error_state());
        PRINT Concat('Error line', Error_line());
        PRINT Concat('Error severity', Error_severity());
        THROW;
        END CATCH
END;


EXEC silver.load_silver










