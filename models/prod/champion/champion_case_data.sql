SELECT champion_case_data_agg.*
FROM {{ ref('champion_case_data_agg') }} AS champion_case_data_agg
WHERE
    {{ missing_data_clause([
        'gender', 
        'religion', 
        'caste', 
        'is_physically_disabled', 
        'village_name', 
        'age'
    ], filter_type="out") }}
