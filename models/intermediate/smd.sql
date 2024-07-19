{{ config(
  materialized='table'
) }}


WITH json_data AS (
    SELECT
       data as jsonb_data
    FROM {{ source('source_commcare', 'smd') }} 
),

parsed_data AS (
    SELECT
        json_data.jsonb_data ->> 'app_id' AS app_id,
        (json_data.jsonb_data ->> 'archived')::boolean AS archived,
        json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'content_type' AS attachments_form_content_type,
        (json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'length')::integer AS attachments_form_length,
        json_data.jsonb_data -> 'attachments' -> 'form.xml' ->> 'url' AS attachments_form_url,
        json_data.jsonb_data ->> 'build_id' AS build_id,
        json_data.jsonb_data ->> 'domain' AS domain,
        json_data.jsonb_data ->> 'edited_by_user_id' AS edited_by_user_id,
        (json_data.jsonb_data ->> 'edited_on')::timestamp AS edited_on,
        json_data.jsonb_data -> 'form' ->> '#type' AS form_type,
        json_data.jsonb_data -> 'form' ->> '@name' AS form_name,
        (json_data.jsonb_data -> 'form' ->> '@uiVersion')::integer AS form_ui_version,
        (json_data.jsonb_data -> 'form' ->> '@version')::integer AS form_version,
        json_data.jsonb_data -> 'form' ->> '@xmlns' AS form_xmlns,
        json_data.jsonb_data -> 'form' -> 'case' ->> '@case_id' AS case_id,
        (json_data.jsonb_data -> 'form' -> 'case' ->> '@date_modified')::timestamp AS case_date_modified,
        json_data.jsonb_data -> 'form' -> 'case' ->> '@user_id' AS case_user_id,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'SMD_completed' AS case_update_SMD_completed,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'beneficiary_ready_for_treatment_SMD' AS case_update_beneficiary_ready_for_treatment_SMD, 
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'case_name' AS case_update_case_name,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'date_of_birth' AS case_update_date_of_birth,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'date_of_referral_SMD' AS case_update_date_of_referral_SMD,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'owner_id' AS case_update_owner_id,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'referred_to_SMD' AS case_update_referred_to_SMD,
        json_data.jsonb_data -> 'form' -> 'case' -> 'update' ->> 'treatment_taken_in_the_past_SMD' AS case_update_treatment_taken_in_the_past_SMD,
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
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'SMD_completed' AS serious_mental_disorder_smd_SMD_completed,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'beneficiary_ready_for_treatment_SMD' AS serious_mental_disorder_smd_beneficiary_ready_for_treatment_SMD,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'date_of_birth' AS serious_mental_disorder_smd_date_of_birth,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'date_of_referral_SMD' AS serious_mental_disorder_smd_date_of_referral_SMD,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'name' AS serious_mental_disorder_smd_name,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'referred_to_SMD' AS serious_mental_disorder_smd_referred_to_SMD,
        json_data.jsonb_data -> 'form' -> 'serious_mental_disorder_smd' ->> 'treatment_taken_in_the_past_SMD' AS serious_mental_disorder_smd_treatment_taken_in_the_past_SMD,
        json_data.jsonb_data ->> 'id' AS id,
        (json_data.jsonb_data ->> 'indexed_on')::timestamp AS indexed_on,
        (json_data.jsonb_data ->> 'initial_processing_complete')::boolean AS initial_processing_complete,
        (json_data.jsonb_data ->> 'is_phone_submission')::boolean AS is_phone_submission,
        json_data.jsonb_data -> 'metadata' ->> 'appVersion' AS metadata_app_version,
        (json_data.jsonb_data -> 'metadata' ->> 'app_build_version')::integer AS metadata_app_build_version,
        json_data.jsonb_data -> 'metadata' ->> 'commcare_version' AS metadata_commcare_version,
        json_data.jsonb_data -> 'metadata' ->> 'deviceID' AS metadata_device_id,
        (json_data.jsonb_data -> 'metadata' ->> 'drift')::integer AS metadata_drift,
        json_data.jsonb_data -> 'metadata' ->> 'geo_point' AS metadata_geo_point,
        json_data.jsonb_data -> 'metadata' ->> 'instanceID' AS metadata_instance_id,
        json_data.jsonb_data -> 'metadata' ->> 'location' AS metadata_location,
        (json_data.jsonb_data -> 'metadata' ->> 'timeEnd')::timestamp AS metadata_time_end,
        (json_data.jsonb_data -> 'metadata' ->> 'timeStart')::timestamp AS metadata_time_start,
        json_data.jsonb_data -> 'metadata' ->> 'userID' AS metadata_user_id,
        json_data.jsonb_data -> 'metadata' ->> 'username' AS metadata_username,
        json_data.jsonb_data ->> 'problem' AS problem,
        (json_data.jsonb_data ->> 'received_on')::timestamp AS received_on,
        json_data.jsonb_data ->> 'resource_uri' AS resource_uri,
        (json_data.jsonb_data ->> 'server_modified_on')::timestamp AS server_modified_on,
        json_data.jsonb_data ->> 'submit_ip' AS submit_ip,
        (json_data.jsonb_data ->> 'uiversion')::integer AS uiversion,
        (json_data.jsonb_data ->> 'version')::integer AS version
    FROM json_data
)

SELECT * FROM parsed_data
