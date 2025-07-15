-- Film survey metrics aggregated by district and film name
WITH films_expanded AS (
    SELECT
        district_name,
        men_viewers,
        women_viewers,
        other_viewers,
        UNNEST(STRING_TO_ARRAY(film_name, ' ')) AS individual_film_name
    FROM {{ ref('film_survey_long') }}
    WHERE film_name IS NOT null
)

SELECT
    district_name,
    TRIM(individual_film_name) AS film_name,
    SUM(
        COALESCE(men_viewers, 0)
        + COALESCE(women_viewers, 0)
        + COALESCE(other_viewers, 0)
    ) AS total_viewer_count
FROM films_expanded
WHERE TRIM(individual_film_name) != ''
GROUP BY 1, 2
ORDER BY 1, 2
