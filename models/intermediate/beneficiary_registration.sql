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
  case
		when "data" -> 'form' -> 'case' -> 'update' ->> 'ben_gender_reg' = 'male_gender_reg' then 'male'
		when "data" -> 'form' -> 'case' -> 'update' ->> 'ben_gender_reg' = 'female_gender_reg' then 'female'
		when "data" -> 'form' -> 'case' -> 'update' ->> 'ben_gender_reg' = 'trans_gender_reg' then 'transgender'
		when "data" -> 'form' -> 'case' -> 'update' ->> 'ben_gender_reg' = 'prefer_not_to_say' then 'prefer_not_to_say'
  END AS beneficiary_gender,
    case
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 10 and 15 then '[10-15]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 15 and 20 then '[15-20]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 20 and 25 then '[20-25]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 25 and 30 then '[25-30]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 30 and 35 then '[30-35]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 35 and 40 then '[35-40]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 40 and 45 then '[40-45]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 45 and 50 then '[45-50]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 50 and 55 then '[50-55]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 55 and 60 then '[55-60]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 60 and 65 then '[60-65]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 65 and 70 then '[65-70]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 70 and 75 then '[70-75]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 75 and 80 then '[75-80]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 80 and 85 then '[80-85]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 85 and 90 then '[85-90]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 90 and 95 then '[90-95]'
		when (data::json#>>'{form,case,update,ben_age_reg}')::int between 95 and 100 then '[95-100]'
		when (data::json#>>'{form,case,update,ben_age_reg}') is NULL then 'Null'
  END AS beneficiary_age_group,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_caste_reg' AS beneficiary_caste,
  "data" -> 'form' -> 'case' -> 'update' ->> 'ben_vaas_name_reg' AS beneficiary_vaas
FROM {{ source('source_commcare', 'beneficiary_registration') }}
