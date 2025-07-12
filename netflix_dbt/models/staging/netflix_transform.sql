-- models/netflix_transform.sql

{{ config(materialized='table') }}  -- tells dbt to create a physical table 
-- you can also use 
-- 1. materialized='view' → dbt will create a view instead (lighter, but slower for queries). 
-- 2. materialized='incremental' → dbt will only add new/updated rows in existing table (good for huge datasets).
-- you can alao specify the schema and database in the config block, like this: schema = 'schema_name'
-- Now dbt will save it in schema_name
SELECT DISTINCT
    show_id,
    type,
    title,
    director,
    country,
    TO_DATE(date_added, 'MM-DD-YYYY') AS added_date,
    release_year,
    CONCAT(FLOOR(release_year / 10) * 10, 's') AS release_decade,
    SPLIT_PART(duration, ' ', 1) AS duration_value,
    SPLIT_PART(duration, ' ', 2) AS duration_unit,
    TRIM(SPLIT_PART(listed_in, ',', 1)) AS genre,
    rating
FROM {{ source('netflix', 'netflix_data') }}