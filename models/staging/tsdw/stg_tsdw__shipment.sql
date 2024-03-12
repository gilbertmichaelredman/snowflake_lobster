with 

source as (

    select * from {{ source('tsdw', 'shipment') }}

),

renamed as (

    select
        _fivetran_id,
        shipper,
        destination_port,
        ship_load_volume,
        ship_imo,
        shipment_notes,
        ship_load_weight,
        receiver_contact,
        ship_flag_country_code,
        origination_country,
        ship_mmsi_number,
        shipper_contact,
        destination_country,
        receiver,
        scheduled_departure,
        ship_name,
        actual_departure,
        ship_registration_number,
        scheduled_arrival,
        origination_port,
        actual_arrival,
        shipment_id,
        ship_call_sign,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
