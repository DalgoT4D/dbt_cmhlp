SELECT * FROM {{ ref('beneficiary_case_data') }}
WHERE
{{ missing_data_clause([
    'caste', 
    'religion', 
    'gender',
    'is_physically_disabled', 
    'village_name', 
    'vaas_name'
], filter_type="in") }}
