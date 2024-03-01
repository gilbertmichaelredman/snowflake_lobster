{{ config(
    materialized = "table",
    pre_hook="ALTER EXTERNAL TABLE raw.il_orders REFRESH" 
) }}

with 

source as (

    select * from {{ source('raw', 'il_orders') }}

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
        c10 as O_UPDATEDATE

    from source

)

select * from renamed

