-- This table contains the daily work data of community facilitators
-- The work could range from training, visits, event prep, others, etc.

WITH cte AS (
    SELECT
        *,
        data ->> 'case_id' AS case_id,
        data -> 'properties' ->> 'case_type' AS case_type
    FROM
        {{ ref('raw_case_data') }}
    WHERE
        data -> 'properties' ->> 'case_type' = 'community_facilitator_daily-work'
),

-- there might updates to case & we might have multiple entries with the same case_id, we want to look at the latest one
deduplicated_cte AS (
  {{ dbt_utils.deduplicate(
      relation='cte',
      partition_by='case_id',
      order_by='indexed_on desc',
     )
  }}
)

SELECT * FROM deduplicated_cte
