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
  "data" -> 'form' -> 'case' -> 'update' ->> 'completed_session_4_last' AS completed_session_4,
  "data" -> 'form' -> 'case' -> 'update' ->> 'date_of_form_filling_4_last' AS date_4,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_CMD_reg' as ben_cmd_reg
FROM {{ source('source_commcare', 'cmd_session_4') }} )

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='id',
    order_by='indexed_on desc',
   )
}}



