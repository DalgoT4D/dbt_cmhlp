with expected_CH as (
    SELECT * FROM {{ref('champion_expected_number')}} t
),

expected_beneficiary_factor as (
    SELECT
        coalesce(btrim(p.district_name), '') AS district_name,
        coalesce((p.expected_CMD_factor)::numeric, 0) AS expected_CMD_factor,
        coalesce((p.expected_SMD_factor)::numeric, 0) AS expected_SMD_factor,
        coalesce((p.expected_SB_factor)::numeric, 0) AS expected_SB_factor
    FROM {{ref('expected_number')}} p
)

SELECT
    a.district_name,
    FLOOR((b.champion_expected_no * a.expected_CMD_factor)::numeric) AS CMD_expected_no,
    FLOOR((b.champion_expected_no * a.expected_SMD_factor)::numeric) AS SMD_expected_no,
    FLOOR((b.champion_expected_no * a.expected_SB_factor)::numeric) AS SB_expected_no
FROM expected_beneficiary_factor a INNER JOIN expected_CH b
ON a.district_name = b.district_name