-- This table contains the case data of community facilitator
-- Currently we are only pulling the fields related to registration of the community facilitator
-- Dont worry about the metadata, it is handled in the all_case_deduped model


WITH cte AS (
    SELECT
        {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }},
        COALESCE(
            NULLIF(data -> 'properties' ->> 'age_of_cf_CFR', ''),
            NULLIF(data -> 'properties' ->> 'cf_age', ''),
            null
        )::INTEGER AS age,
        data -> 'properties' ->> 'cf_caste_CFR' AS caste,
        data -> 'properties' ->> 'cf_gender_CFR' AS gender,
        data -> 'properties' ->> 'cf_religion_CFR' AS religion,
        data -> 'properties' ->> 'email_address_CFR' AS email,
        data -> 'properties' ->> 'date_of_joining_CFR' AS date_of_joining,
        data -> 'properties' ->> 'cf_physically_disabled_CFR' AS physically_disabled,
        data -> 'properties' ->> 'phcs_alloted_to_the_cf_CFR' AS phcs_alloted,
        data -> 'properties' ->> 'date_of_training_started_CFR' AS date_of_training_started,
        data -> 'properties' ->> 'date_of_7th_day_of_training_CFR' AS date_of_7th_day_of_training,
        data -> 'properties' ->> 'end_date_of_management_training_CFR' AS end_date_of_management_training,
        data -> 'properties' ->> 'start_date_of_management_training_CFR' AS start_date_of_management_training,
        data -> 'properties' ->> 'material_given_to_cf_from_atmiyata_CFR' AS material_given
    FROM
        {{ ref('all_case_deduped') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'community_facilitator'
)

SELECT
    *,
    CASE
        WHEN age < 19 THEN 'Below 19'
        WHEN age BETWEEN 19 AND 23 THEN '19-23'
        WHEN age BETWEEN 24 AND 25 THEN '24-25'
        WHEN age BETWEEN 26 AND 30 THEN '26-30'
        WHEN age BETWEEN 31 AND 35 THEN '31-35'
        WHEN age > 35 THEN 'Above 36'
    END AS age_group
FROM cte
