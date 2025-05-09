-- Deduplicate the all_case_meta_data table. 
-- We should take the latest record of case_id
-- A case in commcare can span across multiple apps (districts)

{{ 
    dbt_utils.deduplicate(
        relation=ref('all_case_meta_data'),
        partition_by='case_id',
        order_by="indexed_on DESC"
    )
}}
