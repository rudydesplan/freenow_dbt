{{ config(materialized='table') }}

WITH offers_valid AS (
    SELECT *
    FROM {{ ref('stg_offers') }}
    WHERE error_code IS NULL
      AND offer_id IS NOT NULL
),

drivers AS (
    SELECT driver_id
    FROM {{ ref('drivers') }}
),

bookings AS (
    SELECT booking_id
    FROM {{ ref('bookings') }}
),

flagged AS (
    SELECT
        o.*,

        CASE
            WHEN d.driver_id IS NULL AND b.booking_id IS NULL THEN 'INVALID_DRIVER_AND_BOOKING_FK'
            WHEN d.driver_id IS NULL THEN 'INVALID_DRIVER_FK'
            WHEN b.booking_id IS NULL THEN 'INVALID_BOOKING_FK'
            ELSE NULL
        END AS fk_error_code,

        CASE
            WHEN d.driver_id IS NULL AND b.booking_id IS NULL THEN 'driver_id not found in silver.drivers AND booking_id not found in silver.bookings'
            WHEN d.driver_id IS NULL THEN 'driver_id not found in silver.drivers'
            WHEN b.booking_id IS NULL THEN 'booking_id not found in silver.bookings'
            ELSE NULL
        END AS fk_error_detail

    FROM offers_valid o
    LEFT JOIN drivers d
      ON o.driver_id = d.driver_id
    LEFT JOIN bookings b
      ON o.booking_id = b.booking_id
)

SELECT
    -- raw payload (exactly as received)
    offer_id           AS id,
    created_date_raw   AS datecreated,
    booking_id_raw     AS bookingid,
    driver_id_raw      AS driverid,
    route_distance_raw AS routedistance,
    state_raw          AS state,
    driver_read_raw    AS driverread,

    -- FK quarantine metadata
    fk_error_code      AS error_code,
    fk_error_detail    AS error_detail,

    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM flagged
WHERE fk_error_code IS NOT NULL