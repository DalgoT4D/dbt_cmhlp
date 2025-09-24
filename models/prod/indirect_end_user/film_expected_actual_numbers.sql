--expected film viewership = 6 x No. of expected champions for each district

with expected_indirect_end_user as (
    SELECT
        p.district_name,
        p.film_expected_no as expected_film_viewership
    FROM {{ ref('film_expected_numbers') }} p
),

actual_indirect_end_user as (
    SELECT
        district_name,
        SUM(p.total_viewer_count) as actual_film_viewership
    FROM {{ ref('film_survey_film_metrics') }} p
    WHERE p.film_name IS NOT null
    GROUP BY p.district_name
)

SELECT
    e.district_name,
    e.expected_film_viewership,
    a.actual_film_viewership

FROM expected_indirect_end_user e
INNER JOIN actual_indirect_end_user a ON e.district_name = a.district_name