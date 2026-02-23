{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('bronze', 'raw_offers') }}

),

typed as (

    select
        id as offer_id,

        -- RAW columns (for quarantine / observability)
        datecreated   as created_date_raw,
        bookingid     as booking_id_raw,
        driverid      as driver_id_raw,
        routedistance as route_distance_raw,
        state         as state_raw,
        driverread    as driver_read_raw,

        -- cleaned helpers
        nullif(nullif(lower(trim(datecreated)), 'null'), '')     as datecreated_clean,
        nullif(nullif(lower(trim(bookingid)), 'null'), '')      as bookingid_clean,
        nullif(nullif(lower(trim(driverid)), 'null'), '')       as driverid_clean,
        nullif(nullif(lower(trim(routedistance)), 'null'), '')  as routedistance_clean,
        nullif(nullif(lower(trim(state)), 'null'), '')          as state_clean,
        nullif(nullif(lower(trim(driverread)), 'null'), '')     as driverread_clean,

        -- TYPED columns
        case
            when nullif(nullif(lower(trim(datecreated)), 'null'), '') is null then null
            else cast(datecreated as timestamptz)
        end as created_date_ts,

        nullif(nullif(trim(bookingid), ''), 'null') as booking_id,
        nullif(nullif(trim(driverid), ''), 'null')  as driver_id,

        case
            when nullif(nullif(lower(trim(routedistance)), 'null'), '') ~ '^\d+$'
                then cast(nullif(nullif(lower(trim(routedistance)), 'null'), '') as bigint)
            else null
        end as route_distance_m,

        upper(trim(state)) as state,

        case
            when driverread_clean in ('true','1','yes') then true
            when driverread_clean in ('false','0','no') then false
            else null
        end as driver_read,

        -- Lineage
        _batch_id,
        _source_uri,
        _ingested_at,
        _row_number

    from source

),

validated as (

    select
        *,

        -- Validation rules
        case
            when offer_id is null or trim(offer_id) = '' or lower(trim(offer_id)) = 'null'
                then 'MISSING_OFFER_ID'

            when bookingid_clean is null
                then 'MISSING_BOOKING_ID'

            when driverid_clean is null
                then 'MISSING_DRIVER_ID'

            when datecreated_clean is null
                then 'MISSING_CREATED_DATE'

            when routedistance_clean is not null
                 and not (routedistance_clean ~ '^\d+$')
                then 'INVALID_ROUTE_DISTANCE'

            when state_clean is null
                then 'MISSING_STATE'

            when upper(trim(state)) not in ('ACCEPTED', 'CANCELED')
                then 'INVALID_STATE'

            else null
        end as error_code,

        case
            when offer_id is null or trim(offer_id) = '' or lower(trim(offer_id)) = 'null'
                then 'offer id is NULL/blank'

            when bookingid_clean is null
                then 'bookingid is NULL/blank'

            when driverid_clean is null
                then 'driverid is NULL/blank'

            when datecreated_clean is null
                then 'datecreated is NULL/blank'

            when routedistance_clean is not null
                 and not (routedistance_clean ~ '^\d+$')
                then 'route distance not numeric'

            when state_clean is null
                then 'state is NULL/blank'

            when upper(trim(state)) not in ('ACCEPTED', 'CANCELED')
                then 'state not in ACCEPTED|CANCELED'

            else null
        end as error_detail

    from typed

)

select *
from validated;