with expected_CH as (
    SELECT * FROM {{ref('champion_expected_number')}} t
),

expected_CF_factor as (
    SELECT
        coalesce(btrim(p.district_name), '') AS district_name,
        coalesce((p.expected_CF_factor)::numeric, 0) AS expected_CF_factor
    FROM {{ref('expected_number')}} p
)

SELECT
    a.district_name,
    FLOOR((b.champion_expected_no * a.expected_CF_factor)::numeric) AS CF_expected_no
FROM expected_CF_factor a INNER JOIN expected_CH b
ON a.district_name = b.district_name