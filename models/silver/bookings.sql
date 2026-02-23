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

fk_clean AS (
    SELECT
        b.*
    FROM latest b
    LEFT JOIN drivers d
      ON b.accepted_driver_id = d.driver_id
    WHERE b.accepted_driver_id IS NULL
       OR d.driver_id IS NOT NULL
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
    NULL::varchar AS _openlineage_run_id

FROM fk_clean