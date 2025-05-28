{% macro filter_test_user_entries(cte) -%}
INNER JOIN {{ ref('commcare_user_locations') }} AS user_locations
    ON cte.user_id = user_locations.user_id 
        AND user_locations.hierarchy_pm_location_site_code NOT IN ('project_manager_test')
{%- endmacro %}