-- This table pull the fields out of the champion registration case data
-- There is also an edit form for the champion registration case data.
-- The logic here is to first the field from the edit form and if this is null, read from the original form
-- The edit registration form fields have a suffix of _ECHR

SELECT
    {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }}, -- metafields
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
