WITH village_data AS (
    SELECT *,
        --data & time of registration
        EXTRACT(MONTH FROM indexed_on) AS reg_month,
        EXTRACT(YEAR FROM indexed_on) AS reg_year
    FROM {{ ref('village_case_data') }}
    WHERE case_name IS NOT null AND TRIM(case_name) != ''
),

-- Religion slices
{% set religions = ['hindu', 'muslim', 'sikh', 'jain', 'christian', 'buddhist'] %}

religion_slices AS (
    {% for religion in religions %}
        SELECT
            district_name,
            case_name AS village_name,
            case_type,
            community_facilitator_name,
            'religion' AS slice,
            '{{ religion }}' AS category,
            {{ religion }}_population AS population,
            indexed_on,
            case_id
        FROM village_data
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
            case_name AS village_name,
            case_type,
            community_facilitator_name,
            'caste' AS slice,
            '{{ caste }}' AS category,
            {{ caste }}_population AS population,
            indexed_on,
            case_id
        FROM village_data
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
            case_name AS village_name,
            case_type,
            community_facilitator_name,
            'gender' AS slice,
            '{{ gender }}' AS category,
            {{ gender }}_population AS population,
            indexed_on,
            case_id
        FROM village_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Community healthy worker slices
{% set workers = ['asha_worker', 'anganwadi_worker'] %}

community_health_worker_slices AS (
    {% for worker in workers %}
        SELECT
            district_name,
            case_name AS village_name,
            case_type,
            community_facilitator_name,
            'community_health_worker' AS slice,
            '{{ worker }}' AS category,
            {{ worker }}_population AS population,
            indexed_on,
            case_id
        FROM village_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Generic slices
generic_slices AS (
    SELECT
        district_name,
        case_name AS village_name,
        case_type,
        community_facilitator_name,
        'total' AS slice,
        'total' AS category,
        total_population AS population,
        indexed_on,
        case_id
    FROM village_data
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
UNION ALL
SELECT *
FROM community_health_worker_slices
