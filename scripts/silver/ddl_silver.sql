IF OBJECT_ID('silver.crm_cus_info', 'U') is NOT NULL
DROP Table silver.crm_cus_info;

GO

CREATE TABLE silver.crm_cus_info (
    cst_id INT,
    cst_key NVARCHAR (50),
    cst_firstname NVARCHAR (50),
    cst_lastname NVARCHAR (50),
    cst_marital_status NVARCHAR (50),
    cst_gndr NVARCHAR (50),
    cst_create_date DATE,
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID('silver.crm_prd_info', 'U') is NOT NULL
DROP TABLE silver.crm_prd_info;

GO

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    category_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID('silver.crm_sales_details', 'U') is NOT NULL
DROP Table silver.crm_sales_details;

GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID('silver.erp_CUST_AZ12', 'U') is NOT NULL
DROP Table silver.erp_CUST_AZ12;

GO

CREATE TABLE silver.erp_CUST_AZ12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR (50),
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID('silver.erp_LOC_A101', 'U') is NOT NULL
DROP TABLE silver.erp_LOC_A101;

GO

CREATE TABLE silver.erp_LOC_A101 (
    CID NVARCHAR(50),
    CNTRY NVARCHAR (50),
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2', 'U') is NOT NULL
DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR (50),
    dwh_create_time DATETIME2 DEFAULT GETDATE()
);

GO



