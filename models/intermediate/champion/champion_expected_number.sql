with expected_CH as (
    SELECT
        t.district_name,
        t.expected_CH
    FROM {{ref('expected_number')}} t
)

SELECT
    coalesce(btrim(p.district_name), '') AS district_name,
    coalesce((p.expected_CH)::numeric, 0) AS champion_expected_no
FROM expected_CH p