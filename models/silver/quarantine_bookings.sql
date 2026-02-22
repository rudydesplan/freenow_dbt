{{ config(materialized='table') }}

SELECT
    -- raw payload (exactly as received)
    booking_id                 AS id,
    request_date_raw           AS request_date,
    status_raw                 AS status,
    accepted_driver_id_raw     AS id_driver,
    estimated_route_fare_raw   AS estimated_route_fare,

    -- quarantine metadata
    error_code,
    error_detail,
    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM {{ ref('stg_bookings') }}

WHERE error_code IS NOT NULL