{{ config(
  indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
  materialized='table'
) }}


with 
  case_cte as (
    SELECT
  _airbyte_raw_id,
  "data" ->> 'indexed_on' AS indexed_on,
  id AS id,
  "data" -> 'form' -> 'case' ->> '@case_id' AS case_id,
  "data" -> 'form' -> 'case' ->> '@user_id' AS user_id,
  "data" -> 'form' -> 'case' -> 'create' ->> 'case_name' AS case_name,
  "data" -> 'form' -> 'case' -> 'create' ->> 'case_type' AS case_type
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_CMD_reg' as cmd_registrations
FROM {{ source('source_commcare', 'cmd_registrations') }} )

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='id',
    order_by='indexed_on desc',
   )
}}
