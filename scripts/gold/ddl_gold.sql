-- Gold Layer DDL: Creating dimension and fact views for analytics
-- This file defines the gold layer views that integrate and present cleaned data from silver layer for business intelligence.

-- CUSTOMERS DIMENSION
-- Integrates customer data from CRM and ERP sources, prioritizing CRM as master for gender and using surrogate keys

-- Drop existing view if present
DROP VIEW IF EXISTS gold.dim_customers ;

-- Create customer dimension view with integrated data from multiple sources
CREATE VIEW gold.dim_customers AS
SELECT
	row_number() over(ORDER BY cci.cst_id) AS customer_key,
	cci.cst_id AS customer_id,
	cci.cst_key AS customer_number,
	cci.cst_firstname AS first_name,
	cci.cst_lastname last_name,
	ela.cntry AS country,
	cci.cst_marital_status AS marital_status,
	CASE WHEN cci.cst_gndr !='n/a' THEN cci.cst_gndr -- CRM is the master for cust info
		ELSE coalesce(eca.gen, 'n/a')
	END AS gender,
	eca.bdate AS birthdate,
	cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON cci.cst_key = ela.cid ;

-- Validate data integration logic for gender field across sources
-- Assumption CRM has better data, if n/a take from other table
SELECT DISTINCT
cci.cst_gndr ,
eca.gen ,
CASE WHEN cci.cst_gndr !='n/a' THEN cci.cst_gndr -- CRM is the master for cust info
	ELSE coalesce(eca.gen, 'n/a')
END AS new_gen
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON cci.cst_key = ela.cid
ORDER BY 1,2;

-- PRODUCTS DIMENSION
-- Focuses on current products (no end date) with category information from ERP

-- prd_key is primary key here.
-- We have both current data and historical data, but for this we will only work on current data.
-- to get current data we will take data which doesn't have end_date
DROP VIEW IF EXISTS gold.dim_products;

-- Create product dimension view for active products with category details
CREATE VIEW gold.dim_products AS
SELECT
	row_number() over(ORDER BY cpi.prd_start_dt) AS product_key,
	cpi.prd_id AS product_id,
	cpi.prd_key AS product_number,
	cpi.prd_nm AS product_name,
	cpi.cat_id AS category_id,
	epcgv.cat AS category,
	epcgv.subcat AS subcategory,
	epcgv.maintenance ,
	cpi.prd_cost AS cost,
	cpi.prd_line AS product_line,
	cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcgv
ON cpi.cat_id = epcgv.id
WHERE cpi.prd_end_dt IS NULL ;

-- SALES FACT
-- Links sales transactions to customer and product dimensions

-- Drop existing view if present
DROP VIEW IF EXISTS gold.fact_sales;

-- Create sales fact view connecting transactions to dimensions
CREATE VIEW gold.fact_sales as
SELECT
	csd.sls_ord_num AS order_number,
	dp.product_key ,
	dc.customer_key ,
	csd.sls_order_dt AS order_date,
	csd.sls_ship_dt AS shipping_date,
	csd.sls_due_dt AS due_date,
	csd.sls_sales AS sales_amount,
	csd.sls_quantity AS quantity,
	csd.sls_price AS price
FROM silver.crm_sales_details csd
LEFT JOIN gold.dim_products dp
ON csd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc
ON csd.sls_cust_id = dc.customer_id ;