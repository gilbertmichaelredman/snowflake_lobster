{{
    config(
        materialized = 'incremental',
        unique_key = ['L_ORDERKEY', 'L_LINENUMBER']
    )
}}

with
line_items as (
    select *
    from {{source("lab_two","bc_lineitems")}}
),

orders as (
    select * 
    from {{ref("stg_lab_two__bc_orders")}}
),

joined as (
    select li.*,
    NULL::timestamp as O_ORDERDATE,
    current_timestamp() as AUDIT_INSERT_DTTM,
    current_timestamp() as AUDIT_UPDATE_DTTM
from line_items li 
    left join orders ord 
    on li.c1 = ord.o_orderkey
    where ord.O_ORDERKEY IS NULL
)

select * 
from joined

