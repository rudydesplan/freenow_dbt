{{ config(materialized='table') }}

with offers as (
    select *
    from {{ ref('fct_offer') }}
),

bookings as (
    select booking_id, accepted_driver_id, date_key as booking_date_key
    from {{ ref('fct_booking') }}
),

joined as (
    select
        o.driver_id,
        o.date_key as offer_date_key,
        b.booking_date_key,
        o.booking_id,
        case
            when b.accepted_driver_id = o.driver_id
                 and o.date_key = b.booking_date_key
            then 1
            else 0
        end as is_same_day_conversion
    from offers o
    left join bookings b
        on o.booking_id = b.booking_id
)

select
    driver_id,
    offer_date_key as date_key,

    count(*)::bigint as offers_sent,
    sum(is_same_day_conversion)::bigint as same_day_conversions,

    case
        when count(*) = 0 then 0
        else sum(is_same_day_conversion)::double precision / count(*)
    end as same_day_conversion_rate,

    current_timestamp as _processed_at

from joined
group by 1,2