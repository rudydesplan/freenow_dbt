{{ config(materialized='table') }}

SELECT
    booking_id,
    request_date_ts,
    status,
    accepted_driver_id,
    estimated_route_fare_eur,

    _batch_id AS _source_batch_id,
    _source_uri,
    _ingested_at AS _bronze_ingested_at,
    CURRENT_TIMESTAMP AS _processed_at,
    '{{ invocation_id }}' AS _dbt_invocation_id,
    NULL AS _openlineage_run_id

FROM {{ ref('stg_bookings') }}

WHERE error_code IS NULL
  AND booking_id IS NOT NULL