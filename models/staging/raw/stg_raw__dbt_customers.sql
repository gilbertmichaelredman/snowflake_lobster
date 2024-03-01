{{ config(
    pre_hook="ALTER EXTERNAL TABLE raw.dbt_customers REFRESH" 
) }}

with 

source as (

    select * from {{ source('raw', 'dbt_customers') }}

),

renamed as (

    select
        c1 as C_CUSTKEY,
        c2 as C_NAME,
        c3 as C_ADDRESS,
        c4 as C_NATIONKEY, 
        c5 as C_PHONE,
        c6 as C_ACCTBAL, 
        c7 as C_MKTSEGMENT,
        c8 as C_COMMENT

    from source

)

select * from renamed

