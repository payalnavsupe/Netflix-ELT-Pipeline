{{ config(materialized='table') }}

SELECT
    country,
    COUNT(*) AS total_titles,
    COUNT(CASE WHEN type = 'Movie' THEN 1 END) AS total_movies,
    COUNT(CASE WHEN type = 'TV Show' THEN 1 END) AS total_shows
FROM
    {{ ref('netflix_transform') }}
GROUP BY
    country
