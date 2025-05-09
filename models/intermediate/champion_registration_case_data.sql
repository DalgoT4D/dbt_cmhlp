-- This table pull the fields out of the champion registration case data
-- There is also an edit form for the champion registration case data.
-- The logic here is to first the field from the edit form and if this is null, read from the original form
-- The edit registration form fields have a suffix of _ECHR

WITH cte AS (
    SELECT
        district_name,
        indexed_on,
        data,
        data ->> 'case_id' AS case_id,
        data -> 'properties' ->> 'case_type' AS case_type,
        data -> 'properties' ->> 'case_type' AS case_name,
        COALESCE(NULLIF(data -> 'properties' ->> 'caste_ECHR', ''), data -> 'properties' ->> 'caste') AS caste,
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
        {{ ref('raw_case_data') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'atmiyata_champion'
),

-- there might updates to case & we might have multiple entries with the same case_id, we want to look at the latest one
deduplicated_cte AS (
  {{ dbt_utils.deduplicate(
      relation='cte',
      partition_by='case_id',
      order_by='indexed_on desc',
     )
  }}
)

SELECT * FROM deduplicated_cte
