WITH cte AS (
    SELECT
        district_name,
        -- religion
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'hindu_VMS', ''), '0'
        )::integer AS hindu_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'muslim_VMS', ''), '0'
        )::integer AS muslim_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'sikh_religion_VMS', ''), '0'
        )::integer AS sikh_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'jain_religion_VMS', ''), '0'
        )::integer AS jain_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'christian_religion_VMS', ''), '0'
        )::integer AS christian_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'buddhist_religion_VMS', ''), '0'
        )::integer AS buddhist_population,
        -- gender
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'men_in_the_village_VMS', ''), '0'
        )::integer AS men_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'women_in_the_village_VMS', ''), '0'
        )::integer AS women_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'transgender_in_the_village_VMS', ''), '0'
        )::integer AS transgender_population,
        -- caste
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'general_VMS', ''), '0'
        )::integer AS general_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'sc_st_VMS', ''), '0'
        )::integer AS sc_st_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'obc_sbc_VMS', ''), '0'
        )::integer AS obc_sbc_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'vj_nt_VMS', ''), '0'
        )::integer AS vj_nt_population,
        -- total
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'population_of_the_village_VMS', ''), '0'
        )::integer AS total_population,
        -- community health workers
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'no_of_anganwadi_workers_VMS', ''), '0'
        )::integer AS anganwadi_worker_population,
        COALESCE(
            NULLIF(data::json -> 'properties' ->> 'no_of_asha_workers_VMS', ''), '0'
        )::integer AS asha_worker_population,
        -- meta
        indexed_on,
        data::json -> 'properties' ->> 'case_type' AS case_type,
        data::json ->> 'case_id' AS case_id,
        data::json -> 'properties' ->> 'case_name' AS village_name,
        data::json -> 'properties' ->> 'community_facilitator_cf_name_VMS' AS community_facilitator_name
    FROM
        {{ ref('raw_case_data') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'village'
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
