{{ config(materialized='table') }}

WITH src AS (

    SELECT
        booking_id,
        request_date_ts,
        status,
        accepted_driver_id,
        estimated_route_fare_eur
    FROM {{ ref('bookings') }}

),

date_dim AS (

    SELECT date_key, date_day
    FROM {{ ref('dim_date') }}

),

status_dim AS (

    SELECT booking_status_key, booking_status
    FROM {{ ref('dim_booking_status') }}

),

enriched AS (

    SELECT
        s.booking_id,
        s.request_date_ts,

        d.date_key,

        sd.booking_status_key,

        s.accepted_driver_id,
        s.estimated_route_fare_eur,

        CURRENT_TIMESTAMP AS _processed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        NULL::varchar AS _openlineage_run_id

    FROM src s

    JOIN date_dim d
      ON s.request_date_ts::date = d.date_day

    JOIN status_dim sd
      ON UPPER(s.status) = sd.booking_status

)

SELECT *
FROM enriched