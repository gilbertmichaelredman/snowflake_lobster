{% set max_orderdate_query %}
select coalesce(max(O_ORDERDATE), '1900-01-01') as max_orderdate from {{ ref('stg_lab_two__bc_orders') }}
{% endset %}

{% set max_orderdate_result = run_query(max_orderdate_query) %}
{% set max_orderdate = "" %}

{% if execute %}
    {% if max_orderdate_result and max_orderdate_result.columns[0].values() %}
        {% set max_orderdate = max_orderdate_result.columns[0].values()[0] | string %}
    {% endif %}
{% endif %}


{{
    config(
        materialized="incremental",
        unique_key=["L_ORDERKEY", "L_LINENUMBER"],
        merge_exclude_columns=["AUDIT_INSERT_DTTM"],
        pre_hook="ALTER EXTERNAL TABLE {{ source('lab_two', 'bc_lineitems') }} REFRESH",
    )
}}

with

    source as (select * from {{ source("lab_two", "bc_lineitems") }}),
    fails as ( select * from {{ref("bc_lineitem_fails")}}),

    renamed as (

        select
            c1 as l_orderkey,
            c2 as l_partkey,
            c3 as l_suppkey,
            c4 as l_linenumber,
            c5 as l_quantity,
            c6 as l_extendedprice,
            c7 as l_discount,
            c8 as l_tax,
            c9 as l_returnflag,
            c10 as l_linestatus,
            c11 as l_shipdate,
            c12 as l_commitdate,
            c13 as l_receiptdate,
            c14 as l_shipinstruct,
            c15 as l_shipmode,
            c16 as l_comment

        from source src

    ),

    transformed as (
        select
            rn.*,
            rn.l_orderkey || '-' || rn.l_linenumber as l_surrogatekey,
            bc_orders.o_orderdate,
            current_timestamp() as audit_insert_dttm,
            current_timestamp() as audit_update_dttm
        from renamed rn
        inner join
            {{ ref("stg_lab_two__bc_orders") }} bc_orders
            on bc_orders.o_orderkey = rn.l_orderkey

        {% if is_incremental() %}
            where o_orderdate > '{{max_orderdate}}'
        {% endif %}

    ),

unioned as (
    select * from transformed tf 
    union all
    select * from fails
)


select *
from unioned
