-- Film survey metrics aggregated by district and vaas
WITH vaas_expanded AS (
    SELECT
        district_name,
        screening_year,
        screening_month,
        men_viewers,
        women_viewers,
        other_viewers,
        UNNEST(STRING_TO_ARRAY(vaas_names, ' ')) AS individual_vaas_name
    FROM {{ ref('film_survey_long') }}
    WHERE vaas_names IS NOT null
)

SELECT
    district_name,
    screening_year,
    screening_month,
    TRIM(individual_vaas_name) AS vaas_name,
    SUM(
        COALESCE(men_viewers, 0)
        + COALESCE(women_viewers, 0)
        + COALESCE(other_viewers, 0)
    ) AS total_viewer_count
FROM vaas_expanded
WHERE TRIM(individual_vaas_name) != ''
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
