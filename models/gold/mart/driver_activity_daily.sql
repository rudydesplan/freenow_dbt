{{ config(materialized='table') }}

with spine as (
    select
        driver_id,
        date_key,
        date_day
    from {{ ref('spine_driver_day') }}
),

d_driver as (
    select
        driver_id,
        country,
        receive_marketing,
        date_registration,
        driver_rating,
        rating_count
    from {{ ref('dim_driver') }}
),

agg_offer_engagement as (
    select
        driver_id,
        date_key,
        offers_sent,
        offers_read,
        offers_accepted,
        offers_canceled,
        distinct_bookings_offered_count,
        avg_route_distance_m,
        p50_route_distance_m,
        p75_route_distance_m,
        p90_route_distance_m
    from {{ ref('agg_driver_offer_daily') }}
),

agg_booking_request as (
    select
        driver_id,
        date_key,
        bookings_assigned_count,
        sum_estimated_fare_eur,
        avg_estimated_fare_eur
    from {{ ref('agg_driver_booking_request_daily') }}
),

agg_offer_created_conv as (
    select
        driver_id,
        date_key,
        bookings_assigned_from_offers,
        distinct_bookings_offered,
        offer_to_booking_ratio_created_anchor
    from {{ ref('agg_driver_offer_created_daily') }}
),

agg_same_day as (
    select
        driver_id,
        date_key,
        same_day_conversions,
        same_day_conversion_rate
    from {{ ref('agg_driver_same_day_conversion') }}
),

final as (
    select
        s.driver_id,
        s.date_key,
        s.date_day,

        -- Dimensions
        dd.country,
        dd.receive_marketing,
        dd.date_registration,
        (s.date_day - dd.date_registration)::int as tenure_days,
        dd.driver_rating,
        dd.rating_count,

        -- ===============================
        -- Offer Engagement (Created-Date)
        -- ===============================
        coalesce(oe.offers_sent, 0)::bigint     as offers_sent,
        coalesce(oe.offers_read, 0)::bigint     as offers_read,
        coalesce(oe.offers_accepted, 0)::bigint as offers_accepted,
        coalesce(oe.offers_canceled, 0)::bigint as offers_canceled,
        coalesce(oe.distinct_bookings_offered_count, 0)::bigint as distinct_bookings_offered,

        -- ===============================
        -- Exposure-Level Rates (NULL if no exposure)
        -- ===============================
        case
            when oe.offers_sent is null or oe.offers_sent = 0 then null
            else oe.offers_read::double precision / oe.offers_sent
        end as read_rate,

        case
            when oe.offers_sent is null or oe.offers_sent = 0 then null
            else oe.offers_accepted::double precision / oe.offers_sent
        end as acceptance_rate,

        case
            when oe.offers_sent is null or oe.offers_sent = 0 then null
            else oe.offers_canceled::double precision / oe.offers_sent
        end as cancel_rate,

        -- ===============================
        -- Decision-Level Rates (NULL if no decisions)
        -- ===============================
        case
            when (coalesce(oe.offers_accepted,0) + coalesce(oe.offers_canceled,0)) = 0 then null
            else oe.offers_accepted::double precision
                 / (oe.offers_accepted + oe.offers_canceled)
        end as decision_acceptance_rate,

        case
            when (coalesce(oe.offers_accepted,0) + coalesce(oe.offers_canceled,0)) = 0 then null
            else oe.offers_canceled::double precision
                 / (oe.offers_accepted + oe.offers_canceled)
        end as decision_canceled_rate,

        -- ===============================
        -- Conditional After-Read Rates (NULL if no reads)
        -- ===============================
        case
            when oe.offers_read is null or oe.offers_read = 0 then null
            else oe.offers_accepted::double precision / oe.offers_read
        end as accept_after_read_rate,

        case
            when oe.offers_read is null or oe.offers_read = 0 then null
            else oe.offers_canceled::double precision / oe.offers_read
        end as cancel_after_read_rate,

        -- Distance metrics (naturally NULL if no rows)
        oe.avg_route_distance_m,
        oe.p50_route_distance_m,
        oe.p75_route_distance_m,
        oe.p90_route_distance_m,

        -- ===============================
        -- Booking Operational (Request-Date)
        -- ===============================
        coalesce(br.bookings_assigned_count, 0)::bigint as bookings_assigned_count_request_anchor,
        coalesce(br.sum_estimated_fare_eur, 0)          as sum_estimated_fare_eur_request_anchor,
        br.avg_estimated_fare_eur                       as avg_estimated_fare_eur_request_anchor,

        -- ===============================
        -- Offer Conversion (Created-Date)
        -- ===============================
        coalesce(oc.bookings_assigned_from_offers, 0)::bigint as bookings_assigned_from_offers_created_anchor,
        coalesce(oc.distinct_bookings_offered, 0)::bigint     as distinct_bookings_offered_created_anchor,
        oc.offer_to_booking_ratio_created_anchor              as offer_to_booking_ratio_created_anchor,

        -- ===============================
        -- Same-Day Conversion
        -- ===============================
        coalesce(sd.same_day_conversions, 0)::bigint as same_day_conversions,
        sd.same_day_conversion_rate                  as same_day_conversion_rate,

        current_timestamp as _processed_at,
		'{{ invocation_id }}' as _dbt_invocation_id,
		NULL::varchar as _openlineage_run_id

    from spine s
    join d_driver dd
        on dd.driver_id = s.driver_id

    left join agg_offer_engagement oe
        on oe.driver_id = s.driver_id
       and oe.date_key  = s.date_key

    left join agg_booking_request br
        on br.driver_id = s.driver_id
       and br.date_key  = s.date_key

    left join agg_offer_created_conv oc
        on oc.driver_id = s.driver_id
       and oc.date_key  = s.date_key

    left join agg_same_day sd
        on sd.driver_id = s.driver_id
       and sd.date_key  = s.date_key
)

select *
from final