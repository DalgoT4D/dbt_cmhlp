SELECT * FROM {{ ref('champion_case_data') }}
WHERE
    (gender IS null OR gender = '')
    OR (religion IS null OR religion = '')
    OR (caste IS null OR caste = '')
    OR (village_name IS null OR village_name = '')
    OR (is_physically_disabled IS null OR is_physically_disabled = '')
