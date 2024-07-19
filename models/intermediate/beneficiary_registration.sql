{{ config(
  indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
  materialized='table'
) }}



SELECT
  _airbyte_raw_id,
  "data" ->> 'indexed_on' AS indexed_on,
  id AS id,
  "data" -> 'form' -> 'case' ->> '@case_id' AS case_id,
  "data" -> 'form' -> 'case' ->> '@user_id' AS user_id,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_age_reg' AS beneficiary_age,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_religion_reg' AS beneficiary_religion,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_gender_reg' AS beneficiary_gender,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_caste_reg' AS beneficiary_caste,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_vaas_name_reg' AS beneficiary_vaas
FROM {{ source('source_commcare', 'beneficiary_registration') }}
