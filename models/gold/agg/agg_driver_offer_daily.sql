{{ config(materialized='table') }}

with f as (
    select *
    from {{ ref('fct_offer') }}
)

select
    driver_id,
    date_key,

    count(*)::bigint as offers_sent,
    sum(case when is_read then 1 else 0 end)::bigint as offers_read,
    sum(case when is_accepted then 1 else 0 end)::bigint as offers_accepted,
    sum(case when is_canceled then 1 else 0 end)::bigint as offers_canceled,

    count(distinct booking_id)::bigint as distinct_bookings_offered_count,

    avg(route_distance_m)::double precision as avg_route_distance_m,
    percentile_cont(0.50) within group (order by route_distance_m) as p50_route_distance_m,
    percentile_cont(0.75) within group (order by route_distance_m) as p75_route_distance_m,
    percentile_cont(0.90) within group (order by route_distance_m) as p90_route_distance_m,

    current_timestamp as _processed_at

from f
group by 1,2