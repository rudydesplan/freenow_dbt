{{ config(materialized='table') }}

with date_spine as (

    select
        date_day,
        extract(year from date_day)*10000
        + extract(month from date_day)*100
        + extract(day from date_day) as date_key,

        extract(dow from date_day) as day_of_week,
        trim(to_char(date_day, 'Day'))   as day_name,
        case when extract(dow from date_day) in (0,6) then true else false end as is_weekend,

        extract(week from date_day) as week_of_year,
        date_trunc('week', date_day)::date as week_start_date,

        extract(month from date_day) as month,
        trim(to_char(date_day, 'Month')) as month_name,
        extract(quarter from date_day) as quarter,
        extract(year from date_day) as year

    from generate_series(
        '2021-06-01'::date,
        '2021-06-30'::date,
        interval '1 day'
    ) as date_day

)

select * from date_spine