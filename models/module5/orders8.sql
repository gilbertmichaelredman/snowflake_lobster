{{
  config(
    materialized = "incremental",
    unique_key = "O_ORDERKEY",
    merge_exclude_columns = ["AUDIT_INSERT_DTTM"]
  )
}}

select 
    *,
    current_timestamp() as audit_insert_dttm,
    current_timestamp() as audit_update_dttm
from {{ref('stg_raw__il_orders')}}


{% if is_incremental() %}
where O_UPDATEDATE > (select max(O_UPDATEDATE) from {{ this }})
{% endif %}

