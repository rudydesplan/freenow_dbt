{{ config(materialized='table') }}

with drivers as (

    select driver_id, date_registration
    from {{ ref('dim_driver') }}

),

dates as (

    select date_key, date_day
    from {{ ref('dim_date') }}

),

spine as (

    select
        d.driver_id,
        dt.date_key,
        dt.date_day
    from drivers d
    cross join dates dt
    WHERE dt.date_day BETWEEN DATE '2021-06-01' AND DATE '2021-06-14' AND dt.date_day >= d.date_registration

)

select * from spine