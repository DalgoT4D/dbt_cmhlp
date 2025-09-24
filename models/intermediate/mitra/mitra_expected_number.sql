with expected_CH as (
    SELECT * FROM {{ref('champion_expected_number')}} t
),

expected_mitras_factor as (
    SELECT
        coalesce(btrim(p.district_name), '') AS district_name,
        coalesce((p.expected_mitras_factor)::numeric, 0) AS expected_mitras_factor
    FROM {{ref('expected_number')}} p
)

SELECT
    a.district_name,
    FLOOR((b.champion_expected_no * a.expected_mitras_factor)::numeric) AS mitra_expected_no
FROM expected_mitras_factor a INNER JOIN expected_CH b
ON a.district_name = b.district_name