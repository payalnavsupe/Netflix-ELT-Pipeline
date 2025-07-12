{{ config(materialized='table') }}

SELECT
    release_year,
    COUNT(*) AS total_releases
FROM
    {{ ref('netflix_transform') }}
WHERE
    release_year IS NOT NULL
GROUP BY
    release_year
ORDER BY
    release_year ASC
