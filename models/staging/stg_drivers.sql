{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_drivers') }}

),

-- =========================================
-- 1️⃣ TYPE CASTING / STANDARDIZATION
-- =========================================
typed AS (

    SELECT
        id AS driver_id,

        -- RAW columns (for quarantine / observability)
        date_registration AS date_registration_raw,
        driver_rating     AS driver_rating_raw,
        rating_count      AS rating_count_raw,
        receive_marketing AS receive_marketing_raw,
        country           AS country_raw,

        -- ===============================
        -- TYPED columns (safe casting)
        -- ===============================

		CASE
		  WHEN date_registration IS NULL OR LOWER(TRIM(date_registration)) IN ('null','') THEN NULL

		  -- Unix epoch seconds (10 digits)
		  WHEN TRIM(date_registration) ~ '^\d{10}$'
			THEN to_timestamp(CAST(TRIM(date_registration) AS BIGINT))::date

		  -- Datetime string: 'YYYY-MM-DD HH:MM:SS(.mmm)'
		  WHEN TRIM(date_registration) ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}'
			THEN CAST(LEFT(TRIM(date_registration), 10) AS DATE)

		  -- Pure date: 'YYYY-MM-DD'
		  WHEN TRIM(date_registration) ~ '^\d{4}-\d{2}-\d{2}$'
			THEN CAST(TRIM(date_registration) AS DATE)

		  ELSE NULL
		END AS date_registration

        CASE
            WHEN driver_rating ~ '^\d+(\.\d+)?$'
            THEN CAST(driver_rating AS DOUBLE PRECISION)
            ELSE NULL
        END AS driver_rating,

        CASE
            WHEN rating_count ~ '^\d+$'
            THEN CAST(rating_count AS BIGINT)
            ELSE NULL
        END AS rating_count,

        CASE
            WHEN LOWER(receive_marketing) IN ('true','1','yes') THEN TRUE
            WHEN LOWER(receive_marketing) IN ('false','0','no') THEN FALSE
            ELSE NULL
        END AS receive_marketing,

        UPPER(TRIM(country)) AS country,

        -- Lineage metadata
        _batch_id,
        _source_uri,
        _ingested_at,
        _row_number

    FROM source

),

-- =========================================
-- 2️⃣ BUSINESS VALIDATION LOGIC
-- =========================================
validated AS (

    SELECT
        *,

        CASE
            WHEN driver_id IS NULL THEN 'MISSING_DRIVER_ID'
            WHEN date_registration IS NULL THEN 'INVALID_DATE'
            WHEN driver_rating IS NOT NULL 
                 AND (driver_rating < 0 OR driver_rating > 5)
                 THEN 'INVALID_RATING_RANGE'
            ELSE NULL
        END AS error_code,

        CASE
            WHEN driver_id IS NULL THEN 'id is NULL'
            WHEN date_registration IS NULL THEN 'date_registration failed YYYY-MM-DD regex'
            WHEN driver_rating IS NOT NULL 
                 AND (driver_rating < 0 OR driver_rating > 5)
                 THEN 'driver_rating out of range 0..5'
            ELSE NULL
        END AS error_detail

    FROM typed

)

SELECT *
FROM validated