{{ config(materialized='table') }}

WITH valid_bookings AS (

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
    FROM valid_bookings

)

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

FROM deduplicated
WHERE rn = 1