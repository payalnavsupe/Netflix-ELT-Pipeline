SELECT 
    genre,
    COUNT(*) AS total_titles
FROM {{ ref('netflix_transform') }}

WHERE genre IS NOT NULL
GROUP BY genre
ORDER BY total_titles DESC
LIMIT 5
