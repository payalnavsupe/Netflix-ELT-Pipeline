SELECT 
    ROUND(AVG(CAST(duration_value AS INT)), 2) AS avg_movie_duration
FROM {{ ref('netflix_transform') }}

WHERE type = 'Movie'
