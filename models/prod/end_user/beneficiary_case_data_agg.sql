-- This table pull the fields out of the beneficiary registration case data
-- There is also an edit form for the beneficiary registration case data.
-- The logic here is to first try to get the field from the edit form and if this is null/empty,
-- read from the original form
-- These end users are the ones recieiving the benefits of the Atmiyata program

-- filtered: extract data -> 'properties' once and pre-compute repeated expressions
-- to avoid redundant JSONB parsing across 30+ field accesses
WITH filtered AS (
    SELECT
        {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }},
        data -> 'properties' AS props,
        -- Pre-compute the COALESCE(_last, original) for sessions 4-6,
        -- which are each used 2-3 times below
        COALESCE(
            data -> 'properties' ->> 'completed_session_4_last',
            data -> 'properties' ->> 'completed_session_4'
        ) AS session_4_val,
        COALESCE(
            data -> 'properties' ->> 'completed_session_5_last',
            data -> 'properties' ->> 'completed_session_5'
        ) AS session_5_val,
        COALESCE(
            data -> 'properties' ->> 'completed_session_6_last',
            data -> 'properties' ->> 'completed_session_6'
        ) AS session_6_val,
        -- Pre-extract _last-only variants used in last_cmd_session_completed_no
        data -> 'properties' ->> 'completed_session_4_last' AS session_4_last_val,
        data -> 'properties' ->> 'completed_session_5_last' AS session_5_last_val,
        data -> 'properties' ->> 'completed_session_6_last' AS session_6_last_val
    FROM {{ ref('all_case_deduped') }}
    -- Filter on the pre-materialized column instead of re-parsing JSON
    WHERE case_type = 'atmiyata_beneficiary'
),

cte AS (
    SELECT
        {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }},
        COALESCE(
            NULLIF(props ->> 'age_reg', ''),
            NULLIF(props ->> 'ben_age_reg', '')
        )::INTEGER AS age,
        COALESCE(
            NULLIF(props ->> 'name_edit_ben', ''),
            props ->> 'ben_name_reg'
        ) AS name,
        COALESCE(
            NULLIF(props ->> 'caste', ''),
            props ->> 'ben_caste_reg'
        ) AS caste,
        COALESCE(
            NULLIF(props ->> 'religion', ''), props ->> 'ben_religion_reg'
        ) AS religion,
        COALESCE(
            NULLIF(props ->> 'gender_reg', ''), props ->> 'ben_gender_reg'
        ) AS gender,
        props ->> 'village_name' AS village_name,
        props ->> 'ben_vaas_name_reg' AS vaas_name,
        props ->> 'ben_disabled_reg' AS is_physically_disabled,
        COALESCE(
            NULLIF(props ->> 'is_this_beneficiary_referred_by_Mitra', ''),
            props ->> 'ben_mitra_reg'
        ) AS is_referred_by_mitra,
        props ->> 'ben_CMD_reg' AS is_enrolled_for_cmd,
        props ->> 'session_follow_ups' AS is_enrolled_for_cmd_follow_ups,
        COALESCE(
            NULLIF(props ->> 'referral_smd', ''),
            props ->> 'ben_SMD_reg'
        ) AS is_enrolled_for_smd,
        COALESCE(
            NULLIF(props ->> 'social_benefits', ''),
            props ->> 'ben_SB_reg'
        ) AS is_enrolled_for_social_benefits,
        props ->> 'reason_checklist_ben_dropout' AS reason_for_dropout,
        CASE
            WHEN props ->> 'reason_checklist_ben_dropout' IS NOT null THEN 'yes'
            ELSE 'no'
        END AS is_dropped_out,
        -- CMD session statuses - coming from CMD session forms
        CASE
            WHEN props ->> 'completed_session_0' IS null THEN 'not_yet_started'
            WHEN props ->> 'completed_session_0' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_0_status,
        CASE
            WHEN props ->> 'completed_session_1' IS null THEN 'not_yet_started'
            WHEN props ->> 'completed_session_1' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_1_status,
        CASE
            WHEN props ->> 'completed_session_2' IS null THEN 'not_yet_started'
            WHEN props ->> 'completed_session_2' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_2_status,
        CASE
            WHEN props ->> 'completed_session_3' IS null THEN 'not_yet_started'
            WHEN props ->> 'completed_session_3' = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_3_status,
        CASE
            WHEN session_4_val IS null THEN 'not_yet_started'
            WHEN session_4_val = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_4_status,
        CASE
            WHEN session_5_val IS null THEN 'not_yet_started'
            WHEN session_5_val = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_5_status,
        CASE
            WHEN session_6_val IS null THEN 'not_yet_started'
            WHEN session_6_val = 'yes' THEN 'completed'
            ELSE 'invalid'
        END AS cmd_session_6_status,
        props ->> 'after_which_session_ben_dropout' AS after_which_session_ben_dropout, -- dropout form
        CASE
            WHEN COALESCE(NULLIF(props ->> 'after_which_session_ben_dropout', '')) IS NOT null
                THEN
                    REGEXP_REPLACE(
                        props ->> 'after_which_session_ben_dropout', 'session_', '', 'g'
                    )::INTEGER
            WHEN session_6_last_val IS NOT null THEN 6
            WHEN session_5_last_val IS NOT null THEN 5
            WHEN session_4_last_val IS NOT null THEN 4
            WHEN props ->> 'completed_session_3' = 'yes' THEN 3
            WHEN props ->> 'completed_session_2' = 'yes' THEN 2
            WHEN props ->> 'completed_session_1' = 'yes' THEN 1
            WHEN props ->> 'completed_session_0' = 'yes' THEN 0
        END AS last_cmd_session_completed_no,
        CASE
            WHEN NULLIF(props ->> 'after_which_session_ben_dropout', '') IS NOT null
                THEN
                    REGEXP_REPLACE(
                        props ->> 'after_which_session_ben_dropout', 'session_', '', 'g'
                    )::INTEGER
        END AS dropout_cmd_session_no,
        --- SMD referral fields
        props ->> 'beneficiary_ready_for_treatment_SMD' AS smd_referred_beneficiary_ready,
        props ->> 'referred_to_SMD' AS smd_referred_to_institution,
        props ->> 'has_the_beneficiary_met_the_psychiatrist_SMDref' AS smd_referred_beneficiary_met_doc,
        props ->> 'professional_type_SMDref' AS smd_referred_professional_type,
        --- Social benefits fields
        props ->> 'agreed_for_sb_SB' AS sb_beneficiary_agreed,
        CASE
            WHEN
                props ->> 'has_the_benefiary_applied_for_the_social_benefit_scheme_SB_follow-up' = 'yes'
                AND (
                    props ->> 'current_status_shramik_suraksha_yojana_SB_follow-up' = 'received_benefit'
                    OR props ->> 'current_status_vidhwa_pension_yojana_SB_follow-up' = 'received_benefit'
                    OR props ->> 'current_status_disability_certificate_SB_follow-up'
                    = 'received_benefit'
                    OR props
                    ->> 'current_status_mahatma_gandhi_rashtriya_gramin_rojgar_yojana_SB_follow-up'
                    = 'received_benefit'
                    OR props ->> 'current_status_scholarship_for_disabled_students_SB_follow-up'
                    = 'received_benefit'
                    OR props ->> 'current_status_vridh_pension_yojana_SB_follow-up' = 'received_benefit'
                    OR props
                    ->> 'current_status_indira_gandhi_national_disability_pension_scheme_SB_follow-up'
                    = 'received_benefit'
                    OR props ->> 'current_status_of_palak_mata_pita_yojana_SB_follow-up'
                    = 'received_benefit'
                    OR props ->> 'current_status_of_ayushman_bharat_yojana_SB_follow-up'
                    = 'received_benefit'
                )
                THEN 'yes'
            WHEN props ->> 'has_the_benefiary_applied_for_the_social_benefit_scheme_SB_follow-up' = 'yes'
                THEN 'no'
        END AS has_received_social_benefits
    FROM filtered
)

SELECT
    cte.*,
    champions.full_name AS champion_name,
    champions.username AS champion_username,
    --- org hierarchy
    cfs.full_name AS community_facilitator_name,
    cfs.username AS community_facilitator_username,
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
{{ fetch_org_hierarchy(cte, start_role = 'champion') }}
