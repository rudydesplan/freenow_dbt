{{ config(materialized='table') }}

with offers as (
    select *
    from {{ ref('fct_offer') }}
),

bookings as (
    select booking_id, accepted_driver_id
    from {{ ref('fct_booking') }}
),

joined as (
    select
        o.driver_id,
        o.date_key,
        o.booking_id,
        case
            when b.accepted_driver_id = o.driver_id then 1
            else 0
        end as is_assigned_from_offer
    from offers o
    left join bookings b
        on o.booking_id = b.booking_id
)

select
    driver_id,
    date_key,

    count(*)::bigint as offers_sent,
    sum(is_assigned_from_offer)::bigint as bookings_assigned_from_offers,
    count(distinct booking_id)::bigint as distinct_bookings_offered,

	case
		when count(distinct booking_id) = 0 then null
		else sum(is_assigned_from_offer)::double precision
			 / count(distinct booking_id)
	end as offer_to_booking_ratio_created_anchor,

    current_timestamp as _processed_at

from joined
group by 1,2