{{ config(materialized='table') }}

SELECT
    offer_id,
    created_date_ts,
    booking_id,
    driver_id,
    route_distance_m,
    state,
    driver_read,

    _batch_id AS _source_batch_id,
    _source_uri,
    _ingested_at AS _bronze_ingested_at,
    CURRENT_TIMESTAMP AS _processed_at,
    '{{ invocation_id }}' AS _dbt_invocation_id,
    NULL AS _openlineage_run_id

FROM {{ ref('stg_offers') }}

WHERE error_code IS NULL
  AND offer_id IS NOT NULL