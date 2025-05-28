-- This table pull the fields out of the beneficiary registration case data
-- There is also an edit form for the beneficiary registration case data.
-- The logic here is to first try to get the field from the edit form and if this is null/empty, 
-- read from the original form
-- These end users are the ones recieiving the benefits of the Atmiyata program

WITH cte AS (
    SELECT
        {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }}, -- metafields
        COALESCE(
            NULLIF(data -> 'properties' ->> 'age_reg', ''),
            NULLIF(data -> 'properties' ->> 'ben_age_reg', '')
        )::INTEGER AS age,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'name_edit_ben', ''),
            data -> 'properties' ->> 'ben_name_reg'
        ) AS name,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'caste', ''),
            data -> 'properties' ->> 'ben_caste_reg'
        ) AS caste,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'religion', ''), data -> 'properties' ->> 'ben_religion_reg'
        ) AS religion,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'gender_reg', ''), data -> 'properties' ->> 'ben_gender_reg'
        ) AS gender,
        data -> 'properties' ->> 'village_name' AS village_name,
        data -> 'properties' ->> 'ben_vaas_name_reg' AS vaas_name,
        data -> 'properties' ->> 'ben_disabled_reg' AS is_physically_disabled,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'is_this_beneficiary_referred_by_Mitra', ''),
            data -> 'properties' ->> 'ben_mitra_reg'
        ) AS is_referred_by_mitra,
        data -> 'properties' ->> 'ben_CMD_reg' AS is_enrolled_for_cmd,
        data -> 'properties' ->> 'session_follow_ups' AS is_enrolled_for_cmd_follow_ups,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'referral_smd', ''),
            data -> 'properties' ->> 'ben_SMD_reg'
        ) AS is_enrolled_for_smd,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'social_benefits', ''),
            data -> 'properties' ->> 'ben_SB_reg'
        ) AS is_enrolled_for_social_benefits
    FROM
        {{ ref('all_case_deduped') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'atmiyata_beneficiary'
)

SELECT
    cte.*,
    CASE
        WHEN cte.age < 18 THEN 'Below 18'
        WHEN cte.age BETWEEN 18 AND 25 THEN '18-25'
        WHEN cte.age BETWEEN 26 AND 30 THEN '26-30'
        WHEN cte.age BETWEEN 31 AND 35 THEN '31-35'
        WHEN cte.age BETWEEN 36 AND 40 THEN '36-40'
        WHEN cte.age BETWEEN 41 AND 50 THEN '41-50'
        WHEN cte.age > 50 THEN 'Above 51'
    END AS age_group
FROM cte
{{ filter_test_user_entries(cte) }}
