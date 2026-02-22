{{ config(materialized='table') }}

with b as (
    select *
    from {{ ref('fct_booking') }}
),

only_assigned as (
    -- grain: driver_id/day basé sur request_date, comme ton DDL
    select
        accepted_driver_id as driver_id,
        date_key,
        estimated_route_fare_eur
    from b
    where accepted_driver_id is not null
)

select
    driver_id,
    date_key,

    count(*)::bigint as bookings_assigned_count,
    sum(estimated_route_fare_eur)::double precision as sum_estimated_fare_eur,
    avg(estimated_route_fare_eur)::double precision as avg_estimated_fare_eur,
    current_timestamp as _processed_at

from only_assigned
group by 1,2