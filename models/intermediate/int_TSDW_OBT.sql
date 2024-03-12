with 
shipment as (
    select * from {{ref("stg_tsdw__shipment")}}
),

joined as (
    select shipper
    , shipper_contact
    from shipment

)

select * from joined