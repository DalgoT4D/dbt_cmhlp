-- Here the assumption is each user is mapped to a single location.
-- If that is not the case, we will need to change the logic to use "commcare_location_uuids"

SELECT
    users.*,
    locations.location_uuid,
    locations.location_type_code,
    locations.hierarchy_cf_location_uuid,
    locations.hierarchy_cf_location_site_code,
    locations.hierarchy_pm_location_uuid,
    locations.hierarchy_pm_location_site_code
FROM {{ ref('commcare_user_data') }} AS users
LEFT JOIN {{ ref('org_locations') }} AS locations
    ON users.commcare_location_uuid = locations.location_uuid
