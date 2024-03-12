{{config(
    materialized = "incremental",
    unique_key = "O_ORDER_KEY",
    merge_exclude_columns=['audit_insert_dttm'],
    pre_hook = "ALTER EXTERNAL TABLE {{source('lab_two', 'bc_orders')}} refresh"
)
}}

with 

source as (

    select * from {{ source('lab_two', 'bc_orders') }}

),

renamed as (

    select
        c1 as O_ORDERKEY,
        c2 as O_CUSTKEY,
        c3 as O_ORDERSTATUS,
        c4 as O_TOTALPRICE,
        c5 as O_ORDERDATE,
        c6 as O_ORDERPRIORITY,
        c7 as O_CLERK,
        c8 as O_SHIPPRIORITY,
        c9 as O_COMMENT,
        current_timestamp() as audit_insert_dttm
        

    from source

    {% if is_incremental() %}
    where O_ORDERDATE > (select max(O_ORDERDATE) from {{ this }})
    {% endif %}
)

select * from renamed
