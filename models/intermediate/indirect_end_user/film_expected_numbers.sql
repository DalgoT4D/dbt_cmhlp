with expected_CH as (
    SELECT * FROM {{ref('champion_expected_number')}} t
),

expected_film_factor as (
    SELECT
        coalesce(btrim(p.district_name), '') AS district_name,
        coalesce((p.expected_film_factor)::numeric, 0) AS expected_film_factor
    FROM {{ref('expected_number')}} p
)

SELECT
    a.district_name,
    FLOOR((b.champion_expected_no * a.expected_film_factor)::numeric) AS film_expected_no
FROM expected_film_factor a INNER JOIN expected_CH b
ON a.district_name = b.district_name