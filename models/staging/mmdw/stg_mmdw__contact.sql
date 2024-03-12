with 

source as (

    select * from {{ source('mmdw', 'contact') }}

),

renamed as (

    select
        contact_id,
        first_name,
        last_name,
        company_name,
        business_phone_number as business_phone_num,
        business_phone_extension,
        mobile_phone_number as mobile_phone_num,
        home_phone_number as home_phone_num,
        address_line_1,
        address_line_2,
        address_line_3,
        city,
        state,
        postal_code,
        country_id,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
