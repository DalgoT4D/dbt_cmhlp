with expected_vill as (
    SELECT
        t.district_name,
        t.expected_villages
    FROM {{ref('expected_number')}} t
)

SELECT
    coalesce(btrim(p.district_name), '') AS district_name,
    coalesce((p.expected_villages)::numeric, 0) AS village_expected_no
FROM expected_vill p