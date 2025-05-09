-- Deduplicate the all_case_meta_data table. 
-- For each district & case_type, we should take the latest record of case_id

{{ 
    dbt_utils.deduplicate(
        relation=ref('all_case_meta_data'),
        partition_by='district_name, case_type, case_id',
        order_by="indexed_on DESC"
    )
}}
