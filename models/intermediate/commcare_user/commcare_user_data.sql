-- This table contains the details of user configured in the commcare backend

SELECT
    {{ dbt_utils.star(from=ref('all_case_deduped'), except=["data"]) }}, -- metafields
    data -> 'properties' ->> 'username' AS username,
    data -> 'properties' ->> 'first_name' AS first_name,
    data -> 'properties' ->> 'last_name' AS last_name,
    data -> 'properties' ->> 'Role' AS role
FROM
    {{ ref('all_case_deduped') }}
WHERE
    data -> 'properties' ->> 'case_type' = 'commcare-user'
