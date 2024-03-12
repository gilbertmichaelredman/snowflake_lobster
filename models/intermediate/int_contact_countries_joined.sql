with 

contact as (

    select * from {{source("mmdw","contact")}}

),

countries as (
    select * from {{ref('countries')}}
),

joined as (
    select ctc.country_id as country_id
    , ctr.country_name as country_name
    , ctr.alternate_country_name as alternate_country_name

    from contact ctc
    left join countries ctr on ctc.country_id = ctr.country_key
    )

select * from joined

