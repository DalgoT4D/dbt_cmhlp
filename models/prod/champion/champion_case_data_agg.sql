-- This table pull the fields out of the champion registration case data
-- There is also an edit form for the champion registration case data.
-- The logic here is to first the field from the edit form and if this is null, read from the original form
-- The edit registration form fields have a suffix of _ECHR

WITH cte AS (
    SELECT
        {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }}, -- metafields
        COALESCE(
            NULLIF(data -> 'properties' ->> 'age_of_champion_CHR', ''),
            null
        )::INTEGER AS age,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'caste_ECHR', ''),
            NULLIF(data -> 'properties' ->> 'caste_CHR', ''),
            data -> 'properties' ->> 'caste'
        ) AS caste,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'religion_ECHR', ''), data -> 'properties' ->> 'religion_CHR'
        ) AS religion,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'champion_gender_ECHR', ''), data -> 'properties' ->> 'champion_gender_CHR'
        ) AS gender,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'champion_village_ECHR', ''),
            data -> 'properties' ->> 'champion_village_CHR'
        ) AS village_name,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'phc_name_ECHR', ''),
            data -> 'properties' ->> 'phc_name_CHR'
        ) AS phc_name,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'vaas_name_ECHR', ''), data -> 'properties' ->> 'vaas_name'
        ) AS vaas_name,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'champion_physically_disabled_ECHR', ''),
            data -> 'properties' ->> 'champion_physically_disabled_CHR'
        ) AS is_physically_disabled,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'occupation_ECHR', ''), data -> 'properties' ->> 'occupation_CHR'
        ) AS occupation,
        COALESCE(
            data -> 'properties' ->> 'does_the_champion_still_want_to_discountinue_CH_dropout', 'no'
        ) AS is_dropped_out,
        COALESCE(
            data -> 'properties' ->> 'reason_for_champion_drop_out_CH_dropout', 'N/A'
        ) as reason_for_dropout
    FROM
        {{ ref('all_case_deduped') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'atmiyata_champion'
)

SELECT
    cte.*,
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
    case cte.reason_for_dropout
        when 'dissatisfaction_with_the_work' then 'Dissatisfaction With Work'
        when 'dissatisfaction_with_the_CF' then 'Dissatisfaction With CF'
        when 'don''t_have_time_for_the_work' then 'Time Constraints'
        when 'spousal_conflict' then 'Spousal Conflict'
        when 'family_conflict' then 'Family Conflict'
        when 'community_conflict' then 'Community Conflict'
        when 'migration' then 'Migration'
        when 'physical_illeness' then 'Physical Illness'
        when 'accident_physical_injury' then 'Accident / Physical injury'
        when 'significant_life_event' then 'Significant Life Event'
        when 'unable_to_identify_the_issue' then 'CF Unable To Identify The Issue'
        when 'death' then 'Death'
        when 'suicide' then 'Suicide'
        when 'N/A' then 'N/A'
        else 'Multiple Reasons'
    END as reason_for_dropout_display

FROM cte
{{ fetch_org_hierarchy(cte, start_role = 'cf', remove_test_entries = 'yes') }}
