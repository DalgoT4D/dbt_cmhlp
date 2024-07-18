{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'

) }}

with 
  case_cte as (
    select
      _airbyte_raw_id,
      "data" ->> 'indexed_on' AS indexed_on,
      id as id,
      "data" -> 'properties' ->> 'case_name' as case_name,
     "data" -> 'properties' ->> 'case_type' as case_type,
      ("data" ->  'closed')::boolean as closed,
      date("data" ->> 'date_closed'::text) as  date_closed
    from {{ source('source_commcare', 'raw_case') }} )



{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='id',
    order_by='indexed_on desc',
   )
}}