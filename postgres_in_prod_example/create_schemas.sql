-- Create schema for bronze layer in postgres
CREATE SCHEMA IF NOT EXISTS raw;

-- -- Create schema for silver layer in postgres
-- CREATE SCHEMA IF NOT EXISTS silver;

-- -- Create schema for gold layer in postgres
-- CREATE SCHEMA IF NOT EXISTS gold;

-- Create SCD Type Tables fbased on CUSTOMERS in postgres
CREATE TABLE IF NOT EXISTS raw.customers_scd_t2 (
    customer_id INT,
    name VARCHAR(100),
    age INT,
    gender VARCHAR(10),
    signup_date DATE,
    last_updated TIMESTAMP,                        -- New column to track the last update time
    start_date TIMESTAMP NOT NULL DEFAULT NOW(),   -- Start date of the record
    end_date TIMESTAMP,                            -- End date of the record, NULL if current
    is_current BOOLEAN NOT NULL DEFAULT TRUE,      -- TRUE if the record reflects the most current value, and FALSE otherwise
    CONSTRAINT unique_customer_period UNIQUE (customer_id, start_date)  -- Ensures that the values in one or more columns are unique across all rows in the table
);
