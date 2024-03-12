{{ config(
    materialized='table'
) }}


select 
    order_key::number(38,0) as order_key,

    split_part(single_string, '~',1 )::number(38,0) as cust_key,
    split_part(single_string, '~',2 )::varchar(1) as order_status,
    REPLACE(split_part(single_string, '~',3 ), '$', '')::number(12,2) as total_price,
    TO_DATE(split_part(single_string, '~',4 ), 'YYYY-DD-MM') as order_date,
    split_part(single_string, '~',5 )::varchar(15) as order_priority, 
    split_part(single_string, '~',6 )::varchar(15) as clerk,
    split_part(single_string, '~', 7 )::number(38,0) as ship_priority,
    split_part(single_string, '~',8 )::varchar(80) as comment


from {{ref('stg_raw__dbt_orders')}}

