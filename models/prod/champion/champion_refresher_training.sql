with champion_refresher_training as (
    SELECT * FROM {{ ref('champion_refresher_training_form_data') }}
)

SELECT
    *
FROM champion_refresher_training
