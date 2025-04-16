-- This table contains the registration data of community facilitators

WITH cte AS (
    SELECT
        district_name,
        indexed_on,
        COALESCE(NULLIF(data -> 'properties' ->> 'cf_age', ''), null)::INTEGER AS age,
        data ->> 'case_id',
        data -> 'properties' ->> 'case_name' AS name,
        data -> 'properties' ->> 'case_type' AS case_type,
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
        {{ ref('raw_case_data') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'community_facilitator'
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
