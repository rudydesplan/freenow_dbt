{{ config(materialized='table') }}

WITH src AS (

    SELECT *
    FROM {{ ref('offers') }}   -- silver.offers

),

valid_bookings AS (

    SELECT booking_id
    FROM {{ ref('fct_booking') }}

),

date_dim AS (

    SELECT date_key, date_day
    FROM {{ ref('dim_date') }}

),

state_dim AS (

    SELECT offer_state_key, offer_state
    FROM {{ ref('dim_offer_state') }}

),

valid_offers AS (

    SELECT s.*
    FROM src s
    INNER JOIN valid_bookings b
        ON s.booking_id = b.booking_id

),

enriched AS (

    SELECT
        v.offer_id,
        v.booking_id,
        v.driver_id,
        v.created_date_ts,

        d.date_key,

        v.route_distance_m,
        sd.offer_state_key,
        v.driver_read,

        -- Derived flags
        (v.state = 'ACCEPTED') AS is_accepted,
        (v.state = 'CANCELED') AS is_canceled,
        COALESCE(v.driver_read, FALSE) AS is_read,

        CURRENT_TIMESTAMP AS _processed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        NULL::varchar AS _openlineage_run_id

    FROM valid_offers v

    JOIN date_dim d
      ON v.created_date_ts::date = d.date_day

    JOIN state_dim sd
      ON UPPER(v.state) = sd.offer_state

)

SELECT *
FROM enriched