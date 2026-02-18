{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_offers') }}

),

casted AS (

    SELECT
        id                        AS offer_id,
        CAST(datecreated AS TIMESTAMPTZ) AS created_date_ts,
        bookingid                 AS booking_id,
        driverid                  AS driver_id,
        CAST(routedistance AS BIGINT) AS route_distance_m,
        UPPER(state)              AS state,

        CASE
            WHEN LOWER(driverread) IN ('true', '1', 'yes') THEN TRUE
            WHEN LOWER(driverread) IN ('false', '0', 'no') THEN FALSE
            ELSE NULL
        END AS driver_read,

        _batch_id,
        _source_uri,
        _ingested_at

    FROM source

)

SELECT *
FROM casted