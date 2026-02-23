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

        -- TYPED columns (safe casting)
        CASE
            WHEN datecreated IS NULL OR LOWER(TRIM(datecreated)) IN ('null', '') THEN NULL
            ELSE CAST(datecreated AS TIMESTAMPTZ)
        END AS created_date_ts,

        NULLIF(TRIM(bookingid), '') AS booking_id,
        NULLIF(TRIM(driverid), '')  AS driver_id,

        CASE
            WHEN routedistance ~ '^\d+$'
            THEN CAST(routedistance AS BIGINT)
            ELSE NULL
        END AS route_distance_m,

        UPPER(TRIM(state)) AS state,

        CASE
            WHEN LOWER(driverread) IN ('true', '1', 'yes')  THEN TRUE
            WHEN LOWER(driverread) IN ('false', '0', 'no')  THEN FALSE
            WHEN driverread IS NULL OR LOWER(TRIM(driverread)) IN ('null', '') THEN NULL
            ELSE NULL
        END AS driver_read,

        -- Lineage
        _batch_id,
        _source_uri,
        _ingested_at,
        _row_number,

        -- Validation rules
        CASE
            WHEN id IS NULL OR TRIM(id) = '' OR LOWER(TRIM(id)) = 'null'
                THEN 'MISSING_OFFER_ID'

            WHEN bookingid IS NULL OR TRIM(bookingid) = '' OR LOWER(TRIM(bookingid)) = 'null'
                THEN 'MISSING_BOOKING_ID'

            WHEN driverid IS NULL OR TRIM(driverid) = '' OR LOWER(TRIM(driverid)) = 'null'
                THEN 'MISSING_DRIVER_ID'

            WHEN datecreated IS NULL OR LOWER(TRIM(datecreated)) IN ('null', '')
                THEN 'MISSING_CREATED_DATE'

            WHEN routedistance IS NOT NULL AND NOT (routedistance ~ '^\d+$')
                THEN 'INVALID_ROUTE_DISTANCE'

            WHEN state IS NULL OR TRIM(state) = '' OR LOWER(TRIM(state)) = 'null'
                THEN 'MISSING_STATE'

            WHEN UPPER(TRIM(state)) NOT IN ('ACCEPTED', 'CANCELED')
                THEN 'INVALID_STATE'

            ELSE NULL
        END AS error_code,

        CASE
            WHEN id IS NULL OR TRIM(id) = '' OR LOWER(TRIM(id)) = 'null'
                THEN 'offer id is NULL/blank'

            WHEN bookingid IS NULL OR TRIM(bookingid) = '' OR LOWER(TRIM(bookingid)) = 'null'
                THEN 'bookingid is NULL/blank'

            WHEN driverid IS NULL OR TRIM(driverid) = '' OR LOWER(TRIM(driverid)) = 'null'
                THEN 'driverid is NULL/blank'

            WHEN datecreated IS NULL OR LOWER(TRIM(datecreated)) IN ('null', '')
                THEN 'datecreated is NULL/blank'

            WHEN routedistance IS NOT NULL AND NOT (routedistance ~ '^\d+$')
                THEN 'route distance not numeric'

            WHEN state IS NULL OR TRIM(state) = '' OR LOWER(TRIM(state)) = 'null'
                THEN 'state is NULL/blank'

            WHEN UPPER(TRIM(state)) NOT IN ('ACCEPTED', 'CANCELED')
                THEN 'state not in ACCEPTED|CANCELED'

            ELSE NULL
        END AS error_detail

    FROM source
)

SELECT *
FROM validated