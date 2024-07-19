{{ config(
  materialized='table'
) }}

WITH joined_data AS (
    SELECT 
        smd.form_name, 
        smd.case_id, 
        smd.case_update_beneficiary_ready_for_treatment_smd,
        smd_ref.referral_has_met_psychiatrist,
        smd_ref.received_on,
        smd.case_update_case_name
    FROM 
        {{ ref('smd') }} smd 
    LEFT JOIN 
        {{ ref('smd_ref') }} smd_ref
    ON 
        smd.case_id = smd_ref.case_id
),

latest_cases AS (
    SELECT
        form_name,
        case_id,
        case_update_beneficiary_ready_for_treatment_smd,
        referral_has_met_psychiatrist,
        received_on,
        case_update_case_name,
        ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY received_on DESC) AS row_num
    FROM 
        joined_data
)

SELECT
    form_name,
    case_id,
    case_update_beneficiary_ready_for_treatment_smd,
    referral_has_met_psychiatrist,
    received_on,
    case_update_case_name
FROM
    latest_cases
WHERE
    row_num = 1

