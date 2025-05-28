SELECT * FROM {{ ref('champion_case_data_agg') }}
WHERE
    {{ missing_data_clause([
        'gender', 
        'religion', 
        'caste', 
        'is_physically_disabled', 
        'village_name', 
        'age'
    ], filter_type="in") }}
