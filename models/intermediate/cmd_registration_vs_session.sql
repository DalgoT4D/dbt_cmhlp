{{ config(
  materialized='table'
) }}



    SELECT 
        reg._airbyte_raw_id,
        reg.indexed_on,
        reg.id,
        reg.case_id,
        reg.user_id,
        reg.case_name,
        reg.case_type,
        CASE 
            WHEN ses.completed_session_4 IS NOT NULL THEN 'yes'
            ELSE 'no'
        END AS session_4_completed
    FROM 
        {{ ref('cmd_registration') }} reg
    LEFT JOIN 
        {{ ref('cmd_session_4') }} ses
    ON 
        reg.case_id = ses.case_id
