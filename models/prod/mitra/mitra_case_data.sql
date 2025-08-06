-- This table pulls case data for mitras. Currently we are looking at fields related to registration & dropout of mitras
-- Mitra registration only happens after training. So we can assume the 
-- data extracted below is of trained mitras

with cte as (
    SELECT
        district_name,
        indexed_on,
        data ->> 'case_id' AS case_id,
        data -> 'properties' ->> 'case_name' AS case_name,
        data -> 'properties' ->> 'case_type' AS case_type,
        data -> 'properties' ->> 'village_name_mitra' AS village_name,
        data -> 'properties' ->> 'phc_name_mitra' AS phc_name,
        data -> 'properties' ->> 'mitra_vaas_name_reg' AS vaas_name,
        data -> 'properties' ->> 'community_facilitator_cf_name_mitra' AS community_facilitator_name,
        TO_DATE(data -> 'properties' ->> 'date_of_mitra_training', 'YYYY-MM-DD') AS date_of_training,
        data -> 'properties' ->> 'mitra_name_reg' AS mitra_name,
        COALESCE(
            NULLIF(data -> 'properties' ->> 'mitra_age_reg', ''),
            null
        )::INTEGER AS mitra_age,
        CASE
            WHEN data -> 'properties' ->> 'village_name_mitra' IS null THEN 0
            ELSE
                LENGTH(TRIM(data -> 'properties' ->> 'village_name_mitra'))
                - LENGTH(REPLACE(TRIM(data -> 'properties' ->> 'village_name_mitra'), ',', ''))
                + 1
        END AS no_of_villages,
        COALESCE(
	        data -> 'properties' ->> 'does_the_mitra_still_want_to_discountinue_mitra_dropout', 'no'
        ) AS is_dropped_out,
        COALESCE(
            data -> 'properties' ->> 'reason_for_mitra_drop_out_CH_dropout', 'N/A'
        ) as reason_for_dropout

    FROM {{ ref('all_case_deduped') }}
    WHERE data -> 'properties' ->> 'case_type' = 'atmiyata_mitra'
)

SELECT
    cte.*,
    CASE
        WHEN cte.mitra_age < 18 THEN 'Below 18'
        WHEN cte.mitra_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN cte.mitra_age BETWEEN 26 AND 30 THEN '26-30'
        WHEN cte.mitra_age BETWEEN 31 AND 35 THEN '31-35'
        WHEN cte.mitra_age BETWEEN 36 AND 40 THEN '36-40'
        WHEN cte.mitra_age BETWEEN 41 AND 50 THEN '41-50'
        WHEN cte.mitra_age > 50 THEN 'Above 51'
    END AS age_group,
    case cte.reason_for_dropout
        when 'dissatisfaction_with_the_work' then 'Dissatisfaction With Work'
        when 'dissatisfaction_with_the_CF' then 'Dissatisfaction With CF'
        when 'don''t_have_time_for_the_work' then 'Time Constraints'
        when 'spousal_conflict' then 'Spousal Conflict'
        when 'family_conflict' then 'Family Conflict'
        when 'community_conflict' then 'Community Conflict'
        when 'migration' then 'Migration'
        when 'physical_illeness' then 'Physical Illness'
        when 'accident_physical_injury' then 'Accident / Physical injury'
        when 'significant_life_event' then 'Significant Life Event'
        when 'unable_to_identify_the_issue' then 'CF Unable To Identify The Issue'
        when 'death' then 'Death'
        when 'suicide' then 'Suicide'
        when 'N/A' then 'N/A'
        else 'Multiple Reasons'
    END as reason_for_dropout_display

FROM cte
