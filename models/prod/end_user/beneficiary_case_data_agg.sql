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
        ) AS is_enrolled_for_social_benefits,
        data -> 'properties' ->> 'reason_checklist_ben_dropout' AS reason_for_dropout,
        CASE
            WHEN data -> 'properties' ->> 'reason_checklist_ben_dropout' IS NOT null THEN 'yes'
            ELSE 'no'
        END AS is_dropped_out,
        -- CMD session statuses - coming from CMD session forms
        CASE
            WHEN data -> 'properties' ->> 'completed_session_0' IS null THEN 'not_yet_started'
            WHEN data -> 'properties' ->> 'completed_session_0' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_0_status,
        CASE
            WHEN data -> 'properties' ->> 'completed_session_1' IS null THEN 'not_yet_started'
            WHEN data -> 'properties' ->> 'completed_session_1' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_1_status,
        CASE
            WHEN data -> 'properties' ->> 'completed_session_2' IS null THEN 'not_yet_started'
            WHEN data -> 'properties' ->> 'completed_session_2' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_2_status,
        CASE
            WHEN data -> 'properties' ->> 'completed_session_3' IS null THEN 'not_yet_started'
            WHEN data -> 'properties' ->> 'completed_session_3' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_3_status,
        CASE
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_4_last',
                data -> 'properties' ->> 'completed_session_4'
            ) IS null THEN 'not_yet_started'
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_4_last',
                data -> 'properties' ->> 'completed_session_4'
            ) = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_4_status,
        CASE
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_5_last',
                data -> 'properties' ->> 'completed_session_5'
            ) IS null THEN 'not_yet_started'
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_5_last',
                data -> 'properties' ->> 'completed_session_5'
            ) = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_5_status,
        CASE
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_6_last',
                data -> 'properties' ->> 'completed_session_6'
            ) IS null THEN 'not_yet_started'
            WHEN COALESCE(
                data -> 'properties' ->> 'completed_session_6_last',
                data -> 'properties' ->> 'completed_session_6'
            ) = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_6_status,
        data -> 'properties' ->> 'after_which_session_ben_dropout' AS after_which_session_ben_dropout, -- dropout form
        CASE
            WHEN COALESCE(NULLIF(data -> 'properties' ->> 'after_which_session_ben_dropout', '')) IS NOT null
                THEN
                    REGEXP_REPLACE(
                        data -> 'properties' ->> 'after_which_session_ben_dropout', 'session_', '', 'g'
                    )::INTEGER
            WHEN data -> 'properties' ->> 'completed_session_6_last' IS NOT null THEN 6
            WHEN data -> 'properties' ->> 'completed_session_5_last' IS NOT null THEN 5
            WHEN data -> 'properties' ->> 'completed_session_4_last' IS NOT null THEN 4
            WHEN data -> 'properties' ->> 'completed_session_3' = 'yes' THEN 3
            WHEN data -> 'properties' ->> 'completed_session_2' = 'yes' THEN 2
            WHEN data -> 'properties' ->> 'completed_session_1' = 'yes' THEN 1
            WHEN data -> 'properties' ->> 'completed_session_0' = 'yes' THEN 0
        END AS last_cmd_session_completed_no,
        CASE
            WHEN NULLIF(data -> 'properties' ->> 'after_which_session_ben_dropout', '') IS NOT null
                THEN
                    REGEXP_REPLACE(
                        data -> 'properties' ->> 'after_which_session_ben_dropout', 'session_', '', 'g'
                    )::INTEGER
        END AS dropout_cmd_session_no
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
    END AS age_group,
    CASE
        WHEN cte.dropout_cmd_session_no IS NOT null THEN 'dropout'
        WHEN cte.last_cmd_session_completed_no IN (4, 5, 6) THEN 'completed'
        WHEN cte.last_cmd_session_completed_no >= 0 THEN 'ongoing'
        WHEN cte.is_enrolled_for_cmd = 'yes' THEN 'not_started'
        ELSE 'not_enrolled'
    END AS cmd_case_status
FROM cte
{{ filter_test_user_entries(cte) }}
