{{ config(materialized='table') }}

WITH bookings_valid AS (
    SELECT *
    FROM {{ ref('stg_bookings') }}
    WHERE error_code IS NULL
      AND booking_id IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bookings_valid
),

latest AS (
    SELECT *
    FROM deduplicated
    WHERE rn = 1
),

drivers AS (
    SELECT driver_id
    FROM {{ ref('drivers') }}
),

flagged AS (
    SELECT
        b.*,
        CASE
            WHEN b.accepted_driver_id IS NOT NULL AND d.driver_id IS NULL
            THEN 'INVALID_ACCEPTED_DRIVER_FK'
            ELSE NULL
        END AS fk_error_code,
        CASE
            WHEN b.accepted_driver_id IS NOT NULL AND d.driver_id IS NULL
            THEN 'accepted_driver_id not found in silver.drivers'
            ELSE NULL
        END AS fk_error_detail
    FROM latest b
    LEFT JOIN drivers d
      ON b.accepted_driver_id = d.driver_id
)

SELECT
    booking_id               AS id,
    request_date_raw         AS request_date,
    status_raw               AS status,
    accepted_driver_id_raw   AS id_driver,
    estimated_route_fare_raw AS estimated_route_fare,

    fk_error_code            AS error_code,
    fk_error_detail          AS error_detail,

    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM flagged
WHERE fk_error_code IS NOT NULL