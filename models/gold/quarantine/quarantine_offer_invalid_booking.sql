{{ config(materialized='table') }}

WITH offers AS (

    SELECT *
    FROM {{ ref('offers') }}   -- silver.offers

),

valid_bookings AS (

    SELECT booking_id
    FROM {{ ref('fct_booking') }}

)

SELECT
    o.offer_id,
    o.booking_id,
    o.driver_id,
    o.created_date_ts,
    o.route_distance_m,
    o.state,
    o.driver_read,

    'INVALID_BOOKING_FK' AS error_code,
    'booking_id not found in gold.fct_booking' AS error_detail,

    CURRENT_TIMESTAMP AS _quarantined_at

FROM offers o
LEFT JOIN valid_bookings b
  ON o.booking_id = b.booking_id

WHERE b.booking_id IS NULL