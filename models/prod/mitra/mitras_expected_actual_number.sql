with expected_mitras as (
    SELECT
        p.district_name,
        p.mitra_expected_no
    FROM {{ref('mitra_expected_number')}} p
),

actual_mitras as (
    SELECT
        p.district_name,
        count(distinct p.case_id) AS mitra_actual_no
    FROM {{ref('mitra_case_data')}} p
    WHERE p.is_dropped_out = 'no'
    GROUP BY p.district_name
)

SELECT
    p.district_name,
    coalesce((p.mitra_expected_no)::numeric, 0) AS mitra_expected_no,
    coalesce((a.mitra_actual_no)::numeric, 0) AS mitra_actual_no
FROM expected_mitras p
INNER JOIN actual_mitras a
ON p.district_name = a.district_name

