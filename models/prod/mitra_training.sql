SELECT
    district_name,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'number_of_mitras_that_completed_the_training_CFDW', ''), null
    )::INTEGER AS mitras_trained,
    COALESCE(
        NULLIF(
            data -> 'properties' ->> 'has_any_of_their_mitras_dropped_out_of_atmiyata_if_yes_please_enter_the_num_CFDW',
            ''
        ),
        null
    )::INTEGER AS mitras_dropped,
    COALESCE(
        NULLIF(data -> 'properties' ->> 'how_much_time_spent_for_this_work_mitra_training', ''), null
    )::INTEGER AS time_spent_in_minutes,
    case_id,
    case_type,
    data -> 'properties' ->> 'cf_name_CFDW' AS community_facilitator_name,
    data -> 'properties' ->> 'work_done_on_this_day_CFDW' AS work_done_on_this_day,
    TO_DATE(data -> 'properties' ->> 'date_of_the_mitra_training_CFDW', 'YYYY-MM-DD') AS training_date
FROM {{ ref('community_facilitator_daily_work_case_data') }}
WHERE data -> 'properties' ->> 'work_done_on_this_day_CFDW' = 'mitra_training'
