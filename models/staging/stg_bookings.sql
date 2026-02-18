{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_bookings') }}

),

casted AS (

    SELECT
        id AS booking_id,
        CAST(request_date AS TIMESTAMPTZ) AS request_date_ts,
        UPPER(status) AS status,
        id_driver AS accepted_driver_id,
        CAST(estimated_route_fare AS DOUBLE PRECISION) AS estimated_route_fare_eur,

        _batch_id,
        _source_uri,
        _ingested_at

    FROM source

)

SELECT *
FROM casted