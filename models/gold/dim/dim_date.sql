{{ config(materialized='table') }}

WITH date_spine AS (

    SELECT
        -- Force DATE type
        date_day::date AS date_day,

        -- Force INTEGER type
        (
            (extract(year  from date_day)::int * 10000) +
            (extract(month from date_day)::int * 100) +
            (extract(day   from date_day)::int)
        )::int AS date_key,

        -- Force SMALLINT types
        extract(dow from date_day)::smallint AS day_of_week,

        trim(to_char(date_day, 'Day'))::varchar AS day_name,

        CASE
            WHEN extract(dow from date_day) IN (0,6)
            THEN true
            ELSE false
        END AS is_weekend,

        extract(week from date_day)::smallint AS week_of_year,

        date_trunc('week', date_day)::date AS week_start_date,

        extract(month from date_day)::smallint AS month,

        trim(to_char(date_day, 'Month'))::varchar AS month_name,

        extract(quarter from date_day)::smallint AS quarter,

        extract(year from date_day)::smallint AS year

    FROM generate_series(
        '2021-06-01'::date,
        '2021-06-30'::date,
        interval '1 day'
    ) AS date_day

)

SELECT *
FROM date_spine