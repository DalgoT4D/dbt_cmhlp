WITH champion_training_data AS (
    SELECT * FROM {{ ref('champion_training_form_data') }}
),

-- Gender slices
{% set genders = ['male', 'female'] %}

gender_slices AS (
    {% for gender in genders %}
        SELECT
            district_name,
            community_facilitator_name,
            training_batch_name,
            start_date,
            end_date,
            place_of_the_training,
            'gender' AS slice,
            '{{ gender }}' AS category,
            no_of_{{ gender }}_champions_completed AS total_count
        FROM champion_training_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Caste slices
{% set castes = ['general', 'sc_st', 'obc'] %}

caste_slices AS (
    {% for caste in castes %}
        SELECT
            district_name,
            community_facilitator_name,
            training_batch_name,
            start_date,
            end_date,
            place_of_the_training,
            'caste' AS slice,
            '{{ caste }}' AS category,
            no_of_{{ caste }}_champions_completed AS total_count
        FROM champion_training_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Villages covered
villages_covered_slice AS (
    SELECT
        district_name,
        community_facilitator_name,
        training_batch_name,
        start_date,
        end_date,
        place_of_the_training,
        'villages_covered' AS slice,
        'villages_covered' AS category,
        no_of_villages_covered AS total_count
    FROM champion_training_data
),


-- PHC (primary health center) covered
phcs_covered_slice AS (
    SELECT
        district_name,
        community_facilitator_name,
        training_batch_name,
        start_date,
        end_date,
        place_of_the_training,
        'phcs_covered' AS slice,
        'phcs_covered' AS category,
        no_of_phcs_covered AS total_count
    FROM champion_training_data
)

SELECT * FROM gender_slices
UNION ALL
SELECT * FROM caste_slices
UNION ALL
SELECT * FROM villages_covered_slice
UNION ALL
SELECT * FROM phcs_covered_slice
