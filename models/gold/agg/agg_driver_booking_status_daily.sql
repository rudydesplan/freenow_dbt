{{ config(materialized='table') }}

with b as (
    select *
    from {{ ref('fct_booking') }}
),

assigned as (
	select
		accepted_driver_id as driver_id,
		date_key,
		booking_status_key
	from b
	where accepted_driver_id is not null
)

select
    driver_id,
    date_key,
    booking_status_key,
    count(*)::bigint as booking_count,
    current_timestamp as _processed_at
from assigned
group by 1,2,3