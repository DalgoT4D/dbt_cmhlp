with expected_villages as (
    SELECT
        p.district_name,
        p.village_expected_no
    FROM {{ref('village_expected_number')}} p
),

actual_villages as (
    SELECT
        p.district_name,
        count(distinct p.case_id) AS village_actual_no
    FROM {{ref('village_case_data_long')}} p
    GROUP BY p.district_name
)

SELECT
    p.district_name,
    coalesce((p.village_expected_no)::numeric, 0) AS village_expected_no,
    coalesce((a.village_actual_no)::numeric, 0) AS village_actual_no
FROM expected_villages p
INNER JOIN actual_villages a
ON p.district_name = a.district_name

