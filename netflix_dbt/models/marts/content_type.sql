SELECT 
    type,
    COUNT(*) AS total_titles
FROM {{ ref('netflix_transform') }}

GROUP BY type
