{{ config(
  materialized='table'
) }}

WITH json_data AS (
    SELECT
        data as jsonb_data
    FROM {{ source('source_commcare', 'smd_ref') }} )
,

parsed_data AS (
    SELECT
        json_data.jsonb_data ->> 'app_id' AS app_id,
        (json_data.jsonb_data ->> 'archived')::boolean AS archived,
        json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'content_type' AS form_content_type,
        (json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'length')::integer AS form_length,
        json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'url' AS form_url,
        json_data.jsonb_data ->> 'build_id' AS build_id,
        json_data.jsonb_data ->> 'domain' AS domain,
        json_data.jsonb_data ->> 'edited_by_user_id' AS edited_by_user_id,
        (json_data.jsonb_data ->> 'edited_on')::timestamp AS edited_on,
        json_data.jsonb_data -> 'form' -> 'case' ->> '@case_id' AS case_id,
        (json_data.jsonb_data -> 'form' -> 'case' ->> '@date_modified')::timestamp AS case_date_modified,
        json_data.jsonb_data -> 'form' -> 'case' ->> '@user_id' AS case_user_id,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'case_name' AS case_name,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'date_of_birth' AS case_date_of_birth,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'date_of_next_visit_to_professional_SMDref' AS case_date_of_next_visit,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'date_of_visit_to_professional_SMDref' AS case_date_of_visit,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'has_the_beneficiary_met_the_psychiatrist_SMDref' AS case_has_met_psychiatrist,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'owner_id' AS case_owner_id,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'professional_type_SMDref' AS case_professional_type,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'appVersion' AS meta_app_version,
        (json_data.jsonb_data -> 'form' -> 'meta' ->> 'app_build_version')::integer AS meta_app_build_version,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'commcare_version' AS meta_commcare_version,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'deviceID' AS meta_device_id,
        (json_data.jsonb_data -> 'form' -> 'meta' ->> 'drift')::integer AS meta_drift,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'geo_point' AS meta_geo_point,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'instanceID' AS meta_instance_id,
        (json_data.jsonb_data -> 'form' -> 'meta' ->> 'timeEnd')::timestamp AS meta_time_end,
        (json_data.jsonb_data -> 'form' -> 'meta' ->> 'timeStart')::timestamp AS meta_time_start,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'userID' AS meta_user_id,
        json_data.jsonb_data -> 'form' -> 'meta' ->> 'username' AS meta_username,
        json_data.jsonb_data -> 'form' ->> 'owner_id' AS owner_id,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'date_of_birth' AS referral_date_of_birth,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'date_of_next_visit_to_professional_SMDref' AS referral_date_of_next_visit,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'date_of_visit_to_professional_SMDref' AS referral_date_of_visit,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'has_the_beneficiary_met_the_psychiatrist_SMDref' AS referral_has_met_psychiatrist,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'name' AS referral_name,
        json_data.jsonb_data -> 'form' -> 'referral_utilization_form_SMDref' ->> 'professional_type_SMDref' AS referral_professional_type,
        json_data.jsonb_data ->> 'id' AS id,
        (json_data.jsonb_data ->> 'indexed_on')::timestamp AS indexed_on,
        (json_data.jsonb_data ->> 'initial_processing_complete')::boolean AS initial_processing_complete,
        (json_data.jsonb_data ->> 'is_phone_submission')::boolean AS is_phone_submission,
        (json_data.jsonb_data ->> 'received_on')::timestamp AS received_on,
        (json_data.jsonb_data ->> 'server_modified_on')::timestamp AS server_modified_on,
        json_data.jsonb_data ->> 'submit_ip' AS submit_ip,
        json_data.jsonb_data ->> 'type' AS type,
        json_data.jsonb_data ->> 'uiversion' AS uiversion,
        json_data.jsonb_data ->> 'version' AS version
    FROM json_data
)

SELECT * FROM parsed_data
