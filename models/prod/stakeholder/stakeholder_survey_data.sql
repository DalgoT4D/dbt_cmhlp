with cte as (
    SELECT
        {{ dbt_utils.star(from=ref('stakeholders_form_merged'), except=["data"]) }}, -- metafields
        --data & time of registration
        EXTRACT(MONTH FROM indexed_on) AS reg_month,
        EXTRACT(YEAR FROM indexed_on) AS reg_year,
        COALESCE(
            NULLIF(data -> 'form' -> 'commcare_usercase' -> 'case' -> 'update' ->> 'name_of_the_stakeholder', ''), data -> 'form' -> 'stakeholder_form' ->> 'name_of_the_stakeholder'
        ) AS name_of_the_stakeholder,
        COALESCE(
            NULLIF(data -> 'form' -> 'commcare_usercase' -> 'case' -> 'update' ->> 'date_of_visit_to_stakeholder', ''), data -> 'form' -> 'stakeholder_form' ->> 'date_of_visit_to_stakeholder'
        ) AS date_of_visit_to_stakeholder,
        COALESCE(
            NULLIF(data -> 'form' -> 'commcare_usercase' -> 'case' -> 'update' ->> 'designation_of_the_stakeholder', ''), data -> 'form' -> 'stakeholder_form' ->> 'designation_of_the_stakeholder'
        ) AS designation_of_the_stakeholder,
        COALESCE(
            NULLIF(data -> 'form' -> 'commcare_usercase' -> 'case' -> 'update' ->> 'what_are_the_points_discussed_in_stakeholder_meeting', ''), data -> 'form' -> 'stakeholder_form' ->> 'what_are_the_points_discussed_in_stakeholder_meeting'
        ) AS what_are_the_points_discussed_in_stakeholder_meeting

    FROM {{ ref('stakeholders_form_merged') }}
    
)

SELECT
    cte.*,
    CASE cte.designation_of_the_stakeholder
        WHEN 'phc_doctor' THEN 'PHC Doctor'
        WHEN 'phc_nurse' THEN 'PHC Nurse'
        WHEN 'phc_other_staff' THEN 'PHC other staff'
        WHEN 'asha_worker' THEN 'ASHA worker'
        WHEN 'anaganwadi_worker' THEN 'Anganwadi worker'
        WHEN 'sarpanch' THEN 'Sarpanch'
        WHEN 'upsanpanch' THEN 'Upsarpanch'
        WHEN 'influencial_person_in_the_village' THEN 'Influential person in the village'
        WHEN 'other_NGO_staff' THEN 'NGO staff'
        WHEN 'other' THEN 'Other'
        ELSE NULL
    END AS designation_of_the_stakeholder_display
FROM cte

