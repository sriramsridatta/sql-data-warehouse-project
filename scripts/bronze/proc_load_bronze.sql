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
DROP PROCEDURE IF EXISTS bronze.load_bronze;

-- Create the stored procedure for loading bronze layer data
CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
-- Declare variables for tracking execution time
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
-- Begin the executable code block
BEGIN
	-- Raise a notice to log the start of the loading process
	RAISE NOTICE 'Loading data in bronze layer';
	-- Record the start time
	start_time := NOW();
    -- Load CRM customer information
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.crm_cust_info;
    -- Copy data from CSV file into the table
    COPY bronze.crm_cust_info
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in crm_cust_info';

    -- Load CRM product information
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.crm_prd_info;
    -- Copy data from CSV file into the table
    COPY bronze.crm_prd_info
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in crm_prd_info';

    -- Load CRM sales details
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.crm_sales_details;
    -- Copy data from CSV file into the table
    COPY bronze.crm_sales_details
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in crm_sales_details';

    -- Load ERP customer data
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.erp_cust_az12;
    -- Copy data from CSV file into the table
    COPY bronze.erp_cust_az12
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in erp_cust_az12';

    -- Load ERP location data
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.erp_loc_a101;
    -- Copy data from CSV file into the table
    COPY bronze.erp_loc_a101
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in erp_loc_a101';

    -- Load ERP category data
    -- Truncate the table to remove existing data
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    -- Copy data from CSV file into the table
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/datta/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;
	-- Log the completion of this table load
	RAISE NOTICE 'Loading data in erp_px_cat_g1v2';

	-- Log the completion of all loads
	RAISE NOTICE 'Loading completed';
	-- Record the end time
	end_time := NOW();
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
CALL bronze.load_bronze();