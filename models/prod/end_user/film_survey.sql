-- This table extracts film screening survey data from the film survey forms
-- It aggregates viewer counts by gender and provides total viewership metrics

SELECT
    {{ dbt_utils.star(from=ref('film_survey_form_merged'), except=["data"]) }}, -- metafields
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'men', ''
    )::INTEGER AS men_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'women', ''
    )::INTEGER AS women_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'others', ''
    )::INTEGER AS other_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' ->> 'physically_disabled_people_viewing_films', ''
    )::INTEGER AS disabled_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_sc_people', ''
    )::INTEGER AS sc_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_st_people', ''
    )::INTEGER AS st_viewers,
    NULLIF(
        data -> 'form' -> 'film_screening_survey' -> 'castewise_number_of_viewers_films' ->> 'number_of_obc_people', ''
    )::INTEGER AS obc_viewers,
    NULLIF(
        data
        -> 'form'
        -> 'film_screening_survey'
        -> 'castewise_number_of_viewers_films'
        ->> 'number_of_general_category_people',
        ''
    )::INTEGER AS general_category_viewers,
    -- Caste-wise breakdown
    data -> 'form' -> 'film_screening_survey' ->> 'film_name_films' AS film_name,
    data -> 'form' -> 'film_screening_survey' ->> 'date_of_screening_films' AS date_of_screening,
    (
        COALESCE(
            NULLIF(data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'men', '')::INTEGER, 0
        )
        + COALESCE(
            NULLIF(data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'women', '')::INTEGER, 0
        )
        + COALESCE(
            NULLIF(data -> 'form' -> 'film_screening_survey' -> 'number_of_viewing_films' ->> 'others', '')::INTEGER, 0
        )
    ) AS total_views,
    data -> 'form' -> 'film_screening_survey' ->> 'from_which_vaas_people_viewed_the_film_films' AS vaas_name
FROM
    {{ ref('film_survey_form_merged') }}
