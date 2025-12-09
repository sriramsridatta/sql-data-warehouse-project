/*
 🔹 LANGUAGE plpgsql — What it means
Tells Postgres which language the function/procedure body is written in.
plpgsql = PostgreSQL’s procedural language (supports variables, loops, exceptions).
If you omit it, Postgres assumes SQL language — no block structure, no variables, and most procedural logic fails.

🔹 AS $$ — Why we use it
$$ ... $$ is a string delimiter for procedure/function body.
Prevents conflicts with ' ' string quotes inside the procedure.
Cleaner than escaping every ' character.
You could use $$, $body$, $myblock$ — any tag works.

🔹 CREATE OR ALTER — The SQL Server Trap
Works in SQL Server, NOT in PostgreSQL.
In PostgreSQL:
Use CREATE OR REPLACE for functions.
For procedures: No OR REPLACE until newer versions — usually you must DROP PROCEDURE then re-create.
 */

/* Drop the procedure if it exists to allow recreation */
DROP PROCEDURE IF EXISTS silver.load_silver;

-- Create the stored procedure for loading and transforming silver layer data
CREATE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
-- Declare variables for tracking execution time
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
-- Begin the executable code block
BEGIN
	-- Raise a notice to log the start of the loading process
	RAISE NOTICE 'Loading data in silver layer';
	
	-- Record the start time
	start_time := now();
	-- Log the start of CRM customer info loading
	RAISE NOTICE 'Loading data in crm_cust_info';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.crm_cust_info;
	-- Insert cleaned and deduplicated customer data
	INSERT INTO silver.crm_cust_info
	SELECT
	t.cst_id ,
	t.cst_key  ,
	TRIM(t.cst_firstname) AS cst_firstname ,
	TRIM(t.cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr,
	t.cst_create_date
	FROM
		(SELECT * ,
		row_number() OVER (PARTITION BY cci.cst_id ORDER BY cci.cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info cci
		WHERE cci.cst_id IS NOT NULL)t
	WHERE t.flag_last = 1;
	
	
	-- Log the start of CRM product info loading
	RAISE NOTICE 'Loading data in crm_prd_info';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.crm_prd_info;
	-- Insert transformed product data with derived category IDs and end dates
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
	cpi.prd_id ,
	REPLACE(substr(cpi.prd_key, 1, 5), '-', '_') AS cat_id,
	substr(cpi.prd_key, 7, length(cpi.prd_key)) AS prd_key,
	cpi.prd_nm ,
	coalesce(cpi.prd_cost, 0) AS prd_cost ,
	CASE WHEN upper(trim(cpi.prd_line)) = 'M' THEN 'Mountain'
		WHEN upper(trim(cpi.prd_line)) = 'R' THEN 'Road'
		WHEN upper(trim(cpi.prd_line)) = 'S' THEN 'Other Sales'
		WHEN upper(trim(cpi.prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END
		AS prd_line,
	cpi.prd_start_dt:: date  ,
	(lead(cpi.prd_start_dt) OVER(PARTITION BY cpi.prd_key ORDER BY cpi.prd_start_dt) - 1):: date AS prd_end_dt
	FROM bronze.crm_prd_info cpi ;
	
	
	-- Log the start of CRM sales details loading
	RAISE NOTICE 'Loading data in crm_sales_details';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.crm_sales_details;
	-- Insert cleaned sales data with date conversions and sales recalculations
	INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price
	)
	SELECT
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	CASE WHEN sls_order_dt = 0 OR length(sls_order_dt::text) !=8 THEN NULL
		ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) !=8 THEN NULL
		ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR length(sls_due_dt::text) !=8 THEN NULL
		ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price)
		THEN sls_quantity * abs(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity ,
	CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales / nullif(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	
	
	-- Log the start of ERP customer loading
	RAISE NOTICE 'Loading data in erp_cust_az12';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.erp_cust_az12;
	-- Insert transformed ERP customer data with cleaned IDs and standardized genders
	INSERT  INTO silver.erp_cust_az12(
		cid ,
		bdate ,
		gen
	)
	SELECT
	CASE WHEN eca.cid LIKE 'NAS%' THEN SUBSTRING(eca.cid, 4, length(eca.cid))
		ELSE eca.cid
	END cid,
	CASE WHEN eca.bdate > now() THEN NULL
		ELSE eca.bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(eca.gen)) IN ('F', 'Female') THEN 'Female'
		WHEN UPPER(TRIM(eca.gen)) IN ('M', 'Male') THEN 'Male'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12 eca ;
	
	
	-- Log the start of ERP location loading
	RAISE NOTICE 'Loading data in erp_loc_a101';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.erp_loc_a101;
	-- Insert transformed location data with cleaned IDs and full country names
	INSERT INTO silver.erp_loc_a101 (
	cid ,
	cntry
	)
	SELECT
	replace(ela.cid, '-', '') AS cid,
	CASE WHEN trim(ela.cntry) in ('DE') THEN 'Germany'
		WHEN trim(ela.cntry) in ('US', 'USA') THEN 'United States'
		WHEN trim(ela.cntry) = '' OR ela.cntry IS NULL THEN 'n/a'
		ELSE trim(ela.cntry)
	END AS cntry
	FROM bronze.erp_loc_a101 ela ;
	
	
	-- Log the start of ERP category loading
	RAISE NOTICE 'Loading data in erp_px_cat_g1v2';
	-- Truncate the table to remove existing data
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	-- Insert category data directly from bronze layer
	INSERT INTO silver.erp_px_cat_g1v2 (
		id ,
		cat ,
		subcat ,
		maintenance
	)
	SELECT
	epcgv.id ,
	epcgv.cat ,
	epcgv.subcat ,
	epcgv.maintenance
	FROM bronze.erp_px_cat_g1v2 epcgv ;
	
	-- Log successful completion
	RAISE NOTICE 'Data loading to silver layer completed successfully.';
	
	-- Record the end time
	end_time := now();
	-- Calculate and log the total duration
	RAISE NOTICE 'Total duration: %', end_time - start_time;
	-- Exception handling block
	EXCEPTION
	    WHEN OTHERS THEN
	        -- Raise a notice with the error message
	        RAISE NOTICE 'Something went wrong: %', SQLERRM;
-- End the procedure
END;
$$;

-- Call the procedure to execute it
CALL silver.load_silver()