{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_bookings') }}

),

validated AS (

    SELECT
        -- Business key
        id AS booking_id,

        -- RAW columns (for quarantine)

        request_date         AS request_date_raw,
        status               AS status_raw,
        id_driver            AS accepted_driver_id_raw,
        estimated_route_fare AS estimated_route_fare_raw,


        -- TYPED / STANDARDIZED columns

        CAST(request_date AS TIMESTAMPTZ) AS request_date_ts,

        UPPER(TRIM(status)) AS status,

        id_driver AS accepted_driver_id,

        CASE
            WHEN estimated_route_fare ~ '^\d+(\.\d+)?$'
            THEN CAST(estimated_route_fare AS DOUBLE PRECISION)
            ELSE NULL
        END AS estimated_route_fare_eur,

        -- Lineage

        _batch_id,
        _source_uri,
        _ingested_at,
        _row_number,

        -- Validation rules
		
        CASE
            WHEN id IS NULL THEN 'MISSING_BOOKING_ID'
            WHEN request_date IS NULL THEN 'MISSING_REQUEST_DATE'
            WHEN status IS NULL THEN 'MISSING_STATUS'
			
			WHEN NULLIF(NULLIF(LOWER(TRIM(estimated_route_fare)), 'null'), '') IS NOT NULL
			 AND NOT (NULLIF(NULLIF(LOWER(TRIM(estimated_route_fare)), 'null'), '') ~ '^\d+(\.\d+)?$')
			THEN 'INVALID_ESTIMATED_FARE'
				
            ELSE NULL
        END AS error_code,

        CASE
            WHEN id IS NULL THEN 'booking id is NULL'
            WHEN request_date IS NULL THEN 'request_date is NULL'
            WHEN status IS NULL THEN 'status is NULL'
            WHEN estimated_route_fare IS NOT NULL 
                 AND NOT (estimated_route_fare ~ '^\d+(\.\d+)?$')
                THEN 'estimated_route_fare not numeric'
            ELSE NULL
        END AS error_detail

    FROM source
)

SELECT *
FROM validated