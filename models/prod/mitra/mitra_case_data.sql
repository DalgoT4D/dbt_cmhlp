-- This table pulls case data for mitras. Currently we are looking at fields related to registration of mitras
-- Mitra registration only happens after training. So we can assume the 
-- data extracted below is of trained mitras

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
    data -> 'properties' ->> 'mitra_age_reg' AS mitra_age,
    CASE
        WHEN data -> 'properties' ->> 'village_name_mitra' IS null THEN 0
        ELSE
            LENGTH(TRIM(data -> 'properties' ->> 'village_name_mitra'))
            - LENGTH(REPLACE(TRIM(data -> 'properties' ->> 'village_name_mitra'), ',', ''))
            + 1
    END AS no_of_villages
FROM {{ ref('all_case_deduped') }}
WHERE data -> 'properties' ->> 'case_type' = 'atmiyata_mitra'
