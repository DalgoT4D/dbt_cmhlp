-- This table contains the survey data collected from the villages in Mehsana district of Gujarat. 
-- It includes the total count across various demographics like religion, caste, gender, etc.

SELECT
    {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }}, -- metafields
    -- religion
    COALESCE(
        NULLIF(data -> 'properties' ->> 'hindu_VMS', ''), '0'
    )::integer AS hindu_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'muslim_VMS', ''), '0'
    )::integer AS muslim_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'sikh_religion_VMS', ''), '0'
    )::integer AS sikh_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'jain_religion_VMS', ''), '0'
    )::integer AS jain_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'christian_religion_VMS', ''), '0'
    )::integer AS christian_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'buddhist_religion_VMS', ''), '0'
    )::integer AS buddhist_population,
    -- gender
    COALESCE(
        NULLIF(data -> 'properties' ->> 'men_in_the_village_VMS', ''), '0'
    )::integer AS men_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'women_in_the_village_VMS', ''), '0'
    )::integer AS women_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'transgender_in_the_village_VMS', ''), '0'
    )::integer AS transgender_population,
    -- caste
    COALESCE(
        NULLIF(data -> 'properties' ->> 'general_VMS', ''), '0'
    )::integer AS general_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'sc_st_VMS', ''), '0'
    )::integer AS sc_st_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'obc_sbc_VMS', ''), '0'
    )::integer AS obc_sbc_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'vj_nt_VMS', ''), '0'
    )::integer AS vj_nt_population,
    -- total
    COALESCE(
        NULLIF(data -> 'properties' ->> 'population_of_the_village_VMS', ''), '0'
    )::integer AS total_population,
    -- community health workers
    COALESCE(
        NULLIF(data -> 'properties' ->> 'no_of_anganwadi_workers_VMS', ''), '0'
    )::integer AS anganwadi_worker_population,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'no_of_asha_workers_VMS', ''), '0'
    )::integer AS asha_worker_population,
    data -> 'properties' ->> 'community_facilitator_cf_name_VMS' AS community_facilitator_name
FROM
    {{ ref('all_case_deduped') }}
WHERE
    data -> 'properties' ->> 'case_type' = 'village'
