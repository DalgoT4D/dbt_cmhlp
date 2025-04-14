WITH base_data AS (
    SELECT
        district_name,
        -- a case is created before any forms are submitted for it
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'hindu_VMS', ''), '0'
        )::integer AS hindu_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'muslim_VMS', ''), '0'
        )::integer AS muslim_population,

        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'sikh_religion_VMS', ''), '0'
        )::integer AS sikh_population,
        -- religion
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'jain_religion_VMS', ''), '0'
        )::integer AS jain_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'christian_religion_VMS', ''), '0'
        )::integer AS christian_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'buddhist_religion_VMS', ''), '0'
        )::integer AS buddhist_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'men_in_the_village_VMS', ''), '0'
        )::integer AS men_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'women_in_the_village_VMS', ''), '0'
        )::integer AS women_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'transgender_in_the_village_VMS', ''), '0'
        )::integer AS transgender_population,
        -- gender
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'general_VMS', ''), '0'
        )::integer AS general_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'sc_st_VMS', ''), '0'
        )::integer AS sc_st_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'obc_sbc_VMS', ''), '0'
        )::integer AS obc_sbc_population,
        -- caste
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'vj_nt_VMS', ''), '0'
        )::integer AS vj_nt_population,
        COALESCE(
            NULLIF(data::json -> 'form' -> 'case' -> 'update' ->> 'population_of_the_village_VMS', ''), '0'
        )::integer AS total_population,
        data::json -> 'form' -> 'case' -> 'create' ->> 'case_type' AS case_type,
        data::json -> 'form' -> 'case' -> 'create' ->> 'case_name' AS village_name,
        -- total
        data::json
        -> 'form'
        -> 'case'
        -> 'update'
        ->> 'community_facilitator_cf_name_VMS' AS community_facilitator_name
    FROM
        {{ ref('village_mapping_survey_form_merged') }}
),

-- Religion slices
{% set religions = ['hindu', 'muslim', 'sikh', 'jain', 'christian', 'buddhist'] %}

religion_slices AS (
    {% for religion in religions %}
        SELECT
            district_name,
            village_name,
            case_type,
            community_facilitator_name,
            'religion' AS slice,
            '{{ religion }}' AS category,
            {{ religion }}_population AS population
        FROM base_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Caste slices
{% set castes = ['general', 'sc_st', 'obc_sbc', 'vj_nt'] %}

caste_slices AS (
    {% for caste in castes %}
        SELECT
            district_name,
            village_name,
            case_type,
            community_facilitator_name,
            'caste' AS slice,
            '{{ caste }}' AS category,
            {{ caste }}_population AS population
        FROM base_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Gender slices
{% set genders = ['men', 'women', 'transgender'] %}

gender_slices AS (
    {% for gender in genders %}
        SELECT
            district_name,
            village_name,
            case_type,
            community_facilitator_name,
            'gender' AS slice,
            '{{ gender }}' AS category,
            {{ gender }}_population AS population
        FROM base_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Generic slices
generic_slices AS (
    SELECT
        district_name,
        village_name,
        case_type,
        community_facilitator_name,
        'total' AS slice,
        'total' AS category,
        total_population AS population
    FROM base_data
)

SELECT *
FROM religion_slices
UNION ALL
SELECT *
FROM caste_slices
UNION ALL
SELECT *
FROM gender_slices
UNION ALL
SELECT *
FROM generic_slices
