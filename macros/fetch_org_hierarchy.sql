--- This will fetch the hierarchy from commcare_user_locations
--- Requesting for end_user: we will fetch their corresponding champion, cf
--- Requesting for champion: we will fetch their corresponding cf
--- This macro will only do the joins, selection of relevant we will need to be handled in the model
--- Note POs are not commcare users

--- start_role can be champion, cf

{% macro fetch_org_hierarchy(cte, start_role, remove_test_entries = 'yes') -%}
{% if start_role == 'champion' %}
LEFT JOIN {{ ref('commcare_user_locations') }} AS champions
    ON cte.user_id = champions.user_id AND champions.role = 'champion'
LEFT JOIN {{ ref('commcare_user_locations') }} AS cfs
    ON champions.hierarchy_cf_location_uuid = cfs.location_uuid AND cfs.role = 'community_facilitator'
{% if remove_test_entries == 'yes' %}
WHERE champions.hierarchy_pm_location_site_code NOT IN ('project_manager_test') and 
      cfs.hierarchy_pm_location_site_code NOT IN ('project_manager_test')
{% endif %}
{% elif start_role == 'cf' %}
LEFT JOIN {{ ref('commcare_user_locations') }} AS cfs
    ON cte.user_id = cfs.user_id AND cfs.role = 'community_facilitator'
{% if remove_test_entries == 'yes' %}
WHERE cfs.hierarchy_pm_location_site_code NOT IN ('project_manager_test')
{% endif %}
{% endif %}
{%- endmacro %}