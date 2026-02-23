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

joined AS (
    SELECT
        o.*
    FROM offers_valid o
    INNER JOIN drivers d
      ON o.driver_id = d.driver_id
    INNER JOIN bookings b
      ON o.booking_id = b.booking_id
)

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
    NULL::varchar AS _openlineage_run_id

FROM joined