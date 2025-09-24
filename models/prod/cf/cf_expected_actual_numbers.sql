--No. of expected cf for each district is dereived from supplied seed data

with expected_cf as (
    SELECT
        p.district_name,
        p.CF_expected_no
    FROM {{ref('cf_expected_number')}} p
),

actual_cf as (
    SELECT
        p.district_name,
        count(distinct p.case_id) AS cf_actual_no
    FROM {{ref('cf_case_data')}} p
    WHERE p.status = 'active'
    GROUP BY p.district_name
)

SELECT
    p.district_name,
    coalesce((p.CF_expected_no)::numeric, 0) AS cf_expected_no,
    coalesce((a.cf_actual_no)::numeric, 0) AS cf_actual_no
FROM expected_cf p
INNER JOIN actual_cf a 
ON p.district_name = a.district_name
