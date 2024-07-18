{{ config(
  materialized='table'
) }}


select * from {{ source('source_commcare', 'sb_follow_up') }}