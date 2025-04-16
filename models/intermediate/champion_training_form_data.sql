-- The form data being extracted here has no case attached to it
-- The data details out the training of champions

SELECT
    district_name,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_villages_covered_CHT', ''), null
    )::INTEGER AS no_of_villages_covered,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_attended_the_training_CHT', ''), null
    )::INTEGER AS no_of_champions_attended,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_invited_for_the_training_CHT', ''),
        null
    )::INTEGER AS no_of_champions_invited,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_obc_champions_attended_the_training_CHT', ''),
        null
    )::INTEGER AS no_of_obc_champions_attended,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_who_completed_the_training_CHT', ''),
        null
    )::INTEGER AS no_of_champions_completed,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_male_champions_attended_the_training_CHT', ''),
        null
    )::INTEGER AS no_of_male_champions_attended,
    COALESCE(
        NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_sc_st_champions_attended_the_training_CHT', ''),
        null
    )::INTEGER AS no_of_sc_st_champions_attended,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_female_champions_attended_the_training_CHT', ''
        ),
        null
    )::INTEGER AS no_of_female_champions_attended,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_general_champions_attended_the_training_CHT', ''
        ),
        null
    )::INTEGER AS no_of_general_champions_attended,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_obc_champions_who_completed_the_training_CHT', ''
        ),
        null
    )::INTEGER AS no_of_obc_champions_completed,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_male_champions_who_completed_the_training_CHT', ''
        ),
        null
    )::INTEGER AS no_of_male_champions_completed,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_sc_st_champions_who_completed_the_training_CHT', ''
        ),
        null
    )::INTEGER AS no_of_sc_st_champions_completed,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_female_champions_who_completed_the_training_CHT',
            ''
        ),
        null
    )::INTEGER AS no_of_female_champions_completed,
    COALESCE(
        NULLIF(
            data -> 'form' -> 'champion_training_details' ->> 'no_of_general_champions_who_completed_the_training_CHT',
            ''
        ),
        null
    )::INTEGER AS no_of_general_champions_completed,
    data -> 'form' -> 'champion_training_details' ->> 'end_date_CHT' AS end_date,
    data -> 'form' -> 'champion_training_details' ->> 'phc_name_CHR' AS phc_name,
    data -> 'form' -> 'champion_training_details' ->> 'start_date_CHT' AS start_date,
    data -> 'form' -> 'champion_training_details' ->> 'champion_village_CHR' AS champion_village,
    data -> 'form' -> 'champion_training_details' ->> 'training_batch_name_CHT' AS training_batch_name,
    data -> 'form' -> 'champion_training_details' ->> 'place_of_the_training_CHT' AS place_of_the_training,
    data -> 'form' -> 'champion_training_details' ->> 'community_facilitator_cf_name_CHR' AS community_facilitator_name
FROM {{ ref('champion_training_form_merged') }}
