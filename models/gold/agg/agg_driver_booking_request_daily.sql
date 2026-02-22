{{ config(materialized='table') }}

with b as (
    select *
    from {{ ref('fct_booking') }}
)

select
    accepted_driver_id as driver_id,
    date_key,

    count(*)::bigint as bookings_assigned_count_request_anchor,
    sum(estimated_route_fare_eur)::double precision as sum_estimated_fare_eur_request_anchor,
    avg(estimated_route_fare_eur)::double precision as avg_estimated_fare_eur_request_anchor,

    current_timestamp as _processed_at

from b
where accepted_driver_id is not null
group by 1,2