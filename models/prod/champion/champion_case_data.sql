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
            NULLIF(data -> 'properties' ->> 'community_facilitator_cf_name_ECHR', ''),
            data -> 'properties' ->> 'community_facilitator_cf_name_CHR'
        ) AS community_facilitator_name
    FROM
        {{ ref('all_case_deduped') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'atmiyata_champion'
)

SELECT
    *,
    CASE
        WHEN age < 18 THEN 'Below 18'
        WHEN age BETWEEN 18 AND 25 THEN '18-25'
        WHEN age BETWEEN 26 AND 30 THEN '26-30'
        WHEN age BETWEEN 31 AND 35 THEN '31-35'
        WHEN age BETWEEN 36 AND 40 THEN '36-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        WHEN age > 50 THEN 'Above 51'
    END AS age_group
FROM cte
WHERE
    {{ missing_data_clause([
        'gender', 
        'religion', 
        'caste', 
        'is_physically_disabled', 
        'village_name', 
        'age'
    ], filter_type="out") }}
