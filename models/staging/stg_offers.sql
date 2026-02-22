{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_offers') }}

),

validated AS (

    SELECT
        id AS offer_id,

        -- RAW columns (for quarantine)

        datecreated   AS created_date_raw,
        bookingid     AS booking_id_raw,
        driverid      AS driver_id_raw,
        routedistance AS route_distance_raw,
        state         AS state_raw,
        driverread    AS driver_read_raw,

        CAST(datecreated AS TIMESTAMPTZ) AS created_date_ts,

        bookingid AS booking_id,
        driverid  AS driver_id,

        CASE
            WHEN routedistance ~ '^\d+$'
            THEN CAST(routedistance AS BIGINT)
            ELSE NULL
        END AS route_distance_m,

        UPPER(TRIM(state)) AS state,

        CASE
            WHEN LOWER(driverread) IN ('true', '1', 'yes') THEN TRUE
            WHEN LOWER(driverread) IN ('false', '0', 'no')  THEN FALSE
            ELSE NULL
        END AS driver_read,

        -- Lineage

        _batch_id,
        _source_uri,
        _ingested_at,
        _row_number,

        -- Validation rules

        CASE
            WHEN id IS NULL THEN 'MISSING_OFFER_ID'
            WHEN bookingid IS NULL THEN 'MISSING_BOOKING_ID'
            WHEN driverid IS NULL THEN 'MISSING_DRIVER_ID'
            WHEN datecreated IS NULL THEN 'MISSING_CREATED_DATE'
            WHEN routedistance IS NOT NULL AND NOT (routedistance ~ '^\d+$')
                THEN 'INVALID_ROUTE_DISTANCE'
            ELSE NULL
        END AS error_code,

        CASE
            WHEN id IS NULL THEN 'offer id is NULL'
            WHEN bookingid IS NULL THEN 'bookingid is NULL'
            WHEN driverid IS NULL THEN 'driverid is NULL'
            WHEN datecreated IS NULL THEN 'datecreated is NULL'
            WHEN routedistance IS NOT NULL AND NOT (routedistance ~ '^\d+$')
                THEN 'route distance not numeric'
            ELSE NULL
        END AS error_detail

    FROM source
)

SELECT *
FROM validated