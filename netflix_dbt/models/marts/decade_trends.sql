SELECT 
    release_decade,
    COUNT(*) AS total_titles
FROM {{ ref('netflix_transform') }}

GROUP BY release_decade
ORDER BY release_decade
