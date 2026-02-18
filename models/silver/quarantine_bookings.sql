{{ config(materialized='table') }}

SELECT
    booking_id       AS id,
    request_date_raw AS request_date,
    status           AS status,
    accepted_driver_id AS id_driver,
    estimated_route_fare_raw AS estimated_route_fare,

    error_code,
    error_detail,

    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM {{ ref('stg_bookings') }}

WHERE error_code IS NOT NULL