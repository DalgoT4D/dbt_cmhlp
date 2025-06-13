SELECT beneficiary_case_data_agg.*
FROM {{ ref('beneficiary_case_data_agg') }} AS beneficiary_case_data_agg
WHERE
    {{ missing_data_clause([
        'caste', 
        'religion', 
        'gender',
        'is_physically_disabled', 
        'village_name', 
        'vaas_name',
        'age'
    ], filter_type="out") }}
