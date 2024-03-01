with 

source as (

    select * from {{ source('raw', 'dbt_orders') }}

),

renamed as (

     select
        "c1" as order_key,
        concat(
            "c2", '~',
            "c3", '~',
            "c4",
            "c5", '~',
            "c6", '~',
            "c7", '~',
            "c8", '~',
            "c9", '~',
            "c10", 
            COALESCE("c11", ' '), 
            COALESCE("c12", ' '), '~'

        ) as single_string
 
    from source

)

select * from renamed
