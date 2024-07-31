-- models/s3_bucket/customers.sql

-- Incremental models: filter the rows that are new or updated

{{ config(
    materialized='incremental',
    location='s3://dbt-duckdb-ingestion-s3-parquet/customers_scd_t2/year={{ year }}/month={{ month }}/day={{ day }}/customers_scd_t2.parquet'
    )
}}

select
    "CustomerID" as customer_id,
    "Name" as name,
    "Age" as age,
    "Gender" as gender,
    "SignupDate" as signup_date
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data','customers') }}    -- Here, 'data' comes from the 'name:' tag in the sources.yml