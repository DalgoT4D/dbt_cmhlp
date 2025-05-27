-- This table joins location data with their location types

WITH locations_cte AS (
    SELECT
        id,
        name,
        domain,
        parent,
        location_id AS location_uuid,
        longitude,
        site_code,
        substring(location_type FROM '/location_type/([0-9]+)/')::INTEGER AS location_type_id,
        substring(parent FROM '/location/([a-zA-Z0-9]+)/') AS parent_location_uuid
    FROM {{ source('org', 'location') }}
),

location_types_cte AS (
    SELECT
        id,
        code,
        name,
        domain,
        parent,
        shares_cases,
        administrative,
        view_descendants,
        substring(parent FROM '/location_type/([0-9]+)/')::INTEGER AS parent_id
    FROM {{ source('org', 'location_type') }}
),

locations_merged AS (
    SELECT
        locations_cte.id AS location_id,
        locations_cte.name AS location_name,
        locations_cte.domain AS location_domain,
        locations_cte.parent AS location_parent,
        locations_cte.location_uuid,
        locations_cte.longitude AS location_longitude,
        locations_cte.parent_location_uuid,
        locations_cte.site_code AS location_site_code,
        location_types_cte.id AS location_type_id,
        location_types_cte.code AS location_type_code,
        location_types_cte.name AS location_type_name,
        location_types_cte.domain AS location_type_domain,
        location_types_cte.parent_id AS parent_location_type_id,
        location_types_cte.shares_cases AS location_type_shares_cases,
        location_types_cte.administrative AS location_type_administrative,
        location_types_cte.view_descendants AS location_type_view_descendants
    FROM locations_cte
    LEFT JOIN location_types_cte ON
        locations_cte.location_type_id = location_types_cte.id
),

-- from this point we are trying to do self join to get the hierarchy of locations
map_community_facilitators AS (
    SELECT
        champion_locations.*,
        CASE
            WHEN champion_locations.location_type_code = 'community-facilitator' THEN champion_locations.location_uuid
            WHEN champion_locations.location_type_code = 'champion' THEN community_facilitators.location_uuid
        END AS hierarchy_cf_location_uuid,
        CASE
            WHEN
                champion_locations.location_type_code = 'community-facilitator'
                THEN champion_locations.location_site_code
            WHEN champion_locations.location_type_code = 'champion' THEN community_facilitators.location_site_code
        END AS hierarchy_cf_location_site_code
    FROM locations_merged AS champion_locations
    LEFT JOIN locations_merged AS community_facilitators
        ON
            champion_locations.parent_location_uuid = community_facilitators.location_uuid
            AND community_facilitators.location_type_code = 'community-facilitator'
),

map_project_managers AS (
    SELECT
        champions_cfs_locations.*,
        CASE
            WHEN
                champions_cfs_locations.location_type_code = 'project-manager'
                THEN champions_cfs_locations.location_uuid
            ELSE
                project_managers_locations.location_uuid
        END AS hierarchy_pm_location_uuid,
        CASE
            WHEN
                champions_cfs_locations.location_type_code = 'project-manager'
                THEN champions_cfs_locations.location_site_code
            ELSE
                project_managers_locations.location_site_code
        END AS hierarchy_pm_location_site_code
    FROM map_community_facilitators AS champions_cfs_locations
    LEFT JOIN map_community_facilitators AS intermediate_cfs
        ON champions_cfs_locations.hierarchy_cf_location_uuid = intermediate_cfs.location_uuid
    LEFT JOIN map_community_facilitators AS project_managers_locations
        ON
            intermediate_cfs.parent_location_uuid = project_managers_locations.location_uuid
            AND project_managers_locations.location_type_code = 'project-manager'
)

SELECT *
FROM
    map_project_managers
