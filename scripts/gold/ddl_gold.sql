/*
=============================================================================
DDL Script: Create Gold Views
=============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
=============================================================================
*/

--========================================--
-- Create dimension: gold.dim_customers
--========================================--
IF OBJECT_ID('gold.dim_customers') IS NOT NULL
DROP gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() over(order by cst_id) as customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ci.cst_marital_status AS martial_status,
    la.CNTRY AS country,
    CASE WHEN cst_gndr != 'N/A' THEN CI.cst_gndr
        ELSE COALESCE(CA.GEN,'N/A') 
    END AS gender,
    ca.BDATE AS birth_date,
    ci.cst_create_date AS create_date
    FROM silver.crm_cus_info ci
    LEFT JOIN silver.erp_CUST_AZ12 ca 
    ON        ci.cst_key = ca.CID
    LEFT JOIN silver.erp_LOC_A101 la
    ON        ci.cst_key = la.CID;

GO
--========================================--
-- Create dimension: gold.dim_products
--========================================--

IF OBJECT_ID('gold.dim_products') IS NOT NULL
DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() over(ORDER BY pi.prd_start_dt,pi.prd_key) as Product_key,
    pi.prd_id as product_id,
    pi.prd_key as product_number,
    pi.prd_nm as product_name,
    pi.category_id as category_id,
    pc.CAT as category,
    pc.SUBCAT as subcategory,
    pc.MAINTENANCE as miantenance,
    pi.prd_cost as cost,
    pi.prd_line as product_line,
    pi.prd_start_dt as start_date
FROM silver.crm_prd_info pi 
LEFT JOIN silver.erp_PX_CAT_G1V2 pc 
ON pi.category_id = pc.ID
WHERE pi.prd_end_dt IS NULL;

GO
--========================================--
-- Create dimension: gold.dim_products
--========================================--

IF OBJECT_ID('gold.dim_products') IS NOT NULL
DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() over(ORDER BY pi.prd_start_dt,pi.prd_key) as Product_key,
    pi.prd_id as product_id,
    pi.prd_key as product_number,
    pi.prd_nm as product_name,
    pi.category_id as category_id,
    pc.CAT as category,
    pc.SUBCAT as subcategory,
    pc.MAINTENANCE as miantenance,
    pi.prd_cost as cost,
    pi.prd_line as product_line,
    pi.prd_start_dt as start_date
FROM silver.crm_prd_info pi 
LEFT JOIN silver.erp_PX_CAT_G1V2 pc 
ON pi.category_id = pc.ID
WHERE pi.prd_end_dt IS NULL;

