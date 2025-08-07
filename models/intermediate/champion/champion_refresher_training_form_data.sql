-- The form data being extracted here has no case attached to it
-- The data details out the refresher training of champions
-- Also filter out the data that is under test location of the highest hierarchy (project_manager_test)

WITH cte AS (
    SELECT
        district_name,
        COALESCE(
            NULLIF(data -> 'form' -> 'title_refresher' ->> 'CH_attended_refresher', ''),
            null
        )::INTEGER AS no_of_champions_attended_refresher,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'title_refresher' ->> 'CH_invited_refresher', ''
            ),
            null
        )::INTEGER AS no_of_champions_invited_refresher,
        COALESCE(
            NULLIF(
                data -> 'form' -> 'title_refresher' ->> 'batch_refresher', ''
            ),
            null
        )::INTEGER AS refresher_training_batch,
        data -> 'form' -> 'meta' ->> 'userID' AS user_id,
        data -> 'form' -> 'meta' ->> 'username' AS username,
        data -> 'form' -> 'title_refresher' ->> 'date_refresher' AS refresher_training_date

    FROM {{ ref('champion_refresher_training_form_merged') }}
)

SELECT
    cte.*,
    cfs.full_name AS community_facilitator_name,
    cfs.username AS community_facilitator_username
FROM cte
{{ fetch_org_hierarchy(cte, start_role = 'cf', remove_test_entries = 'yes') }}
