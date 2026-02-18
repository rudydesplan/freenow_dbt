{{ config(materialized='table') }}

SELECT
    -- raw payload (as received / normalized strings)
    offer_id        AS id,
    datecreated_raw AS datecreated,
    booking_id      AS bookingid,
    driver_id       AS driverid,
    route_distance_raw AS routedistance,
    state           AS state,
    driver_read_raw AS driverread,

    -- quarantine metadata
    error_code,
    error_detail,
    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM {{ ref('stg_offers') }}

WHERE error_code IS NOT NULL