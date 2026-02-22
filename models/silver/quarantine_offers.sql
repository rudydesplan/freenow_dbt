{{ config(materialized='table') }}

SELECT
    -- raw payload (exactly as received)
    offer_id               AS id,
    created_date_raw       AS datecreated,
    booking_id_raw         AS bookingid,
    driver_id_raw          AS driverid,
    route_distance_raw     AS routedistance,
    state_raw              AS state,
    driver_read_raw        AS driverread,

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