SELECT
    *,
    CASE
        WHEN age < 19 THEN 'Below 19'
        WHEN age BETWEEN 19 AND 23 THEN '19-23'
        WHEN age BETWEEN 24 AND 25 THEN '24-25'
        WHEN age BETWEEN 26 AND 30 THEN '26-30'
        WHEN age BETWEEN 31 AND 35 THEN '31-35'
        WHEN age > 35 THEN 'Above 36'
    END AS age_group
FROM {{ ref('community_facilitator_case_data') }}
