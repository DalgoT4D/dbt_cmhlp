
with expected_SB_CMD_SMD as (
    SELECT
        p.district_name,
        CMD_expected_no,
        SMD_expected_no,
        SB_expected_no
    FROM {{ref('beneficiary_expected_number')}} p
),

actual_SB_CMD_SMD as (
    SELECT
        p.district_name,
        COUNT(DISTINCT CASE WHEN p.is_enrolled_for_cmd = 'yes' THEN p.case_id END) as CMD_actual_no,
        COUNT(DISTINCT CASE WHEN p.is_enrolled_for_smd = 'yes' THEN p.case_id END) as SMD_actual_no,
        COUNT(DISTINCT CASE WHEN p.is_enrolled_for_social_benefits = 'yes' THEN p.case_id END) as SB_actual_no
    FROM {{ref('beneficiary_case_data_agg')}} p
    WHERE p.is_dropped_out = 'no'
    GROUP BY p.district_name
)

SELECT
    p.district_name,
    p.CMD_expected_no,
    p.SMD_expected_no,
    p.SB_expected_no,
    a.CMD_actual_no,
    a.SMD_actual_no,
    a.SB_actual_no
FROM expected_SB_CMD_SMD p
INNER JOIN actual_SB_CMD_SMD a ON p.district_name = a.district_name