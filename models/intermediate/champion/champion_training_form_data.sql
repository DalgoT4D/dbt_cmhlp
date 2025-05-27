-- The form data being extracted here has no case attached to it
-- The data details out the training of champions
-- Also filter out the data that is under test location of the highest hierarchy (project_manager_test)

WITH cte AS (
    SELECT
        district_name,
        COALESCE(
            NULLIF(data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_attended_the_training_CHT', ''),
            null
        )::INTEGER AS no_of_champions_attended,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_invited_for_the_training_CHT', ''
            ),
            null
        )::INTEGER AS no_of_champions_invited,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_champions_who_completed_the_training_CHT', ''
            ),
            null
        )::INTEGER AS no_of_champions_completed,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_obc_champions_attended_the_training_CHT', ''
            ),
            null
        )::INTEGER AS no_of_obc_champions_attended,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_male_champions_attended_the_training_CHT', ''
            ),
            null
        )::INTEGER AS no_of_male_champions_attended,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_sc_st_champions_attended_the_training_CHT', ''
            ),
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
                data -> 'form' -> 'champion_training_details' ->> 'no_of_general_champions_attended_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_general_champions_attended,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_obc_champions_who_completed_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_obc_champions_completed,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'champion_training_details' ->> 'no_of_male_champions_who_completed_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_male_champions_completed,
        COALESCE(
            NULLIF(
                data
                -> 'form'
                -> 'champion_training_details'
                ->> 'no_of_sc_st_champions_who_completed_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_sc_st_champions_completed,
        COALESCE(
            NULLIF(
                data
                -> 'form'
                -> 'champion_training_details'
                ->> 'no_of_female_champions_who_completed_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_female_champions_completed,
        COALESCE(
            NULLIF(
                data
                -> 'form'
                -> 'champion_training_details'
                ->> 'no_of_general_champions_who_completed_the_training_CHT',
                ''
            ),
            null
        )::INTEGER AS no_of_general_champions_completed,
        data -> 'form' -> 'meta' ->> 'userID' AS user_id,
        data -> 'form' -> 'meta' ->> 'username' AS username,
        -- The below logic is used to count the number of villages covered. The column "champion_village_CHR" 
        -- is a multi-select value so the names of villages in the col "champion_village_CHR" 
        -- are separated by a space. Reading the count from here is more accurate 
        -- than directly reading from the column "no_of_villages_covered_CHT"
        CASE
            WHEN data -> 'form' -> 'champion_training_details' ->> 'champion_village_CHR' IS null THEN 0
            ELSE
                LENGTH(TRIM(data -> 'form' -> 'champion_training_details' ->> 'champion_village_CHR'))
                - LENGTH(
                    REPLACE(TRIM(data -> 'form' -> 'champion_training_details' ->> 'champion_village_CHR'), ' ', '')
                )
                + 1
        END AS no_of_villages_covered,
        -- PHCs are primary health centers. The column "phc_name_CHR" is a multi-select value so the 
        -- names of PHCs in the col "phc_name_CHR" are separated by a space
        CASE
            WHEN data -> 'form' -> 'champion_training_details' ->> 'phc_name_CHR' IS null THEN 0
            ELSE
                LENGTH(TRIM(data -> 'form' -> 'champion_training_details' ->> 'phc_name_CHR'))
                - LENGTH(REPLACE(TRIM(data -> 'form' -> 'champion_training_details' ->> 'phc_name_CHR'), ' ', '')) + 1
        END AS no_of_phcs_covered,
        -- TODO: cleanup the date format
        data -> 'form' -> 'champion_training_details' ->> 'start_date_CHT' AS start_date,
        data -> 'form' -> 'champion_training_details' ->> 'end_date_CHT' AS end_date, -- TODO: cleanup the date format
        data -> 'form' -> 'champion_training_details' ->> 'phc_name_CHR' AS phc_name,
        data -> 'form' -> 'champion_training_details' ->> 'champion_village_CHR' AS champion_village,
        data -> 'form' -> 'champion_training_details' ->> 'training_batch_name_CHT' AS training_batch_name,
        data -> 'form' -> 'champion_training_details' ->> 'place_of_the_training_CHT' AS place_of_the_training,
        data
        -> 'form'
        -> 'champion_training_details'
        ->> 'community_facilitator_cf_name_CHR' AS community_facilitator_name
    FROM {{ ref('champion_training_form_merged') }}
)

SELECT cte.*
FROM cte
LEFT JOIN {{ ref('commcare_user_locations') }} AS user_locations
    ON cte.user_id = user_locations.user_id
WHERE user_locations.hierarchy_pm_location_site_code NOT IN ('project_manager_test')
