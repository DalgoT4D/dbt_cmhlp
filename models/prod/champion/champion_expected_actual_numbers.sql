--expected No. of expected champions for each district is supplied in seed data

with expected_chm as (
    SELECT
        p.district_name,
        p.champion_expected_no
    FROM {{ref('champion_expected_number')}} p
),

actual_chm as (
    SELECT
        p.district_name,
        count(distinct p.case_id) AS champion_actual_no
    FROM {{ref('champion_case_data_agg')}} p
    WHERE p.is_dropped_out = 'no'
    GROUP BY p.district_name
)

SELECT
    p.district_name,
    coalesce((p.champion_expected_no)::numeric, 0) AS champion_expected_no,
    coalesce((a.champion_actual_no)::numeric, 0) AS champion_actual_no
FROM expected_chm p
INNER JOIN actual_chm a 
ON p.district_name = a.district_name
