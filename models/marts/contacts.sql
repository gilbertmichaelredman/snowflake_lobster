with 

countries as (
    select * from {{ref("countries")}}
),
contact as (
    select * from {{ref("stg_mmdw__contact")}}
),
shipment as (
    select * from {{ref("stg_tsdw__shipment")}}
),

contact_countries_joined as (
    select * from {{ref("int_contact_countries_joined")}}
),

final as (
    select 
        first_name,
        last_name,
        company_name,
        business_phone_num,
        mobile_phone_num,
        address_line_1,
        address_line_2,
        address_line_3,
        city,
        state,
        postal_code,
        ccj.country_id as country_key,

    from contact ctc
    join contact_countries_joined ccj on ctc.country_id = ccj.country_id
)

select * from final 