-- This table extracts film screening survey data from the film survey forms
-- It converts viewer data into a long format with category types and values

WITH base_data AS (
    SELECT
        {{ dbt_utils.star(from=ref('film_survey_form_merged'), except=["data"]) }}, -- metafields
        data -> 'form' -> 'film_screening_survey' ->> 'film_name_films' AS film_name,
        data -> 'form' -> 'film_screening_survey' ->> 'date_of_screening_films' AS date_of_screening,
        data -> 'form' -> 'film_screening_survey' ->> 'from_which_vaas_people_viewed_the_film_films' AS vaas_names,
        --data & time of screening
        EXTRACT(MONTH FROM indexed_on) AS screening_month,
        EXTRACT(YEAR FROM indexed_on) AS screening_year,
        -- Add array versions for easier analysis
        STRING_TO_ARRAY(data -> 'form' -> 'film_screening_survey' ->> 'film_name_films', ' ') AS film_names_array,
        STRING_TO_ARRAY(
            data -> 'form' -> 'film_screening_survey' ->> 'from_which_vaas_people_viewed_the_film_films', ' '
        ) AS vaas_names_array,
        -- Gender data
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'men', ''
        )::INTEGER AS men_viewers,
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'women', ''
        )::INTEGER AS women_viewers,
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'others', ''
        )::INTEGER AS other_viewers,
        -- Disability data
        NULLIF(
            data -> 'form' -> 'film_screening_survey' ->> 'physically_disabled_people_viewing_films', ''
        )::INTEGER AS disabled_viewers,
        -- Caste data
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_sc_people',
            ''
        )::INTEGER AS sc_viewers,
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_st_people',
            ''
        )::INTEGER AS st_viewers,
        NULLIF(
            data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_obc_people',
            ''
        )::INTEGER AS obc_viewers,
        NULLIF(
            data
            -> 'form'
            -> 'film_screening_survey'
            -> 'castewise_number_of_viewers_films'
            ->> 'number_of_general_category_people',
            ''
        )::INTEGER AS general_category_viewers
    FROM {{ ref('film_survey_form_merged') }}
),

-- Gender slices
{% set genders = ['men', 'women', 'other'] %}

gender_slices AS (
    {% for gender in genders %}
        SELECT
            *,
            'gender' AS slice,
            '{{ gender }}' AS category,
            {{ gender }}_viewers AS viewer_count
        FROM base_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Caste slices
{% set castes = ['sc', 'st', 'obc', 'general_category'] %}

caste_slices AS (
    {% for caste in castes %}
        SELECT
            *,
            'caste' AS slice,
            '{{ caste }}' AS category,
            {{ caste }}_viewers AS viewer_count
        FROM base_data
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {% endfor %}
),

-- Disability slices
disability_slices AS (
    SELECT
        *,
        'disability' AS slice,
        'disabled' AS category,
        disabled_viewers AS viewer_count
    FROM base_data
)

SELECT *
FROM gender_slices
WHERE viewer_count IS NOT null
UNION ALL
SELECT *
FROM caste_slices
WHERE viewer_count IS NOT null
UNION ALL
SELECT *
FROM disability_slices
WHERE viewer_count IS NOT null
