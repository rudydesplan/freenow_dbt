{{ config(materialized='table') }}

WITH src AS (

    SELECT
        driver_id,
        date_registration,
        country,
        receive_marketing,
        driver_rating,
        rating_count
    FROM {{ ref('drivers') }}   -- silver.drivers

),

enriched AS (

    SELECT
        -- Stable surrogate key (deterministic hash)
        {{ dbt_utils.generate_surrogate_key(['driver_id']) }} AS driver_key,

        driver_id,
        date_registration,
        country,
        receive_marketing,
        driver_rating,
        rating_count,

        CASE
			WHEN driver_rating IS NULL THEN NULL
			WHEN driver_rating >= 0  AND driver_rating < 3   THEN '0-3'
			WHEN driver_rating >= 3  AND driver_rating < 4   THEN '3-4'
			WHEN driver_rating >= 4  AND driver_rating < 4.5 THEN '4-4.5'
			WHEN driver_rating >= 4.5 AND driver_rating <= 5 THEN '4.5-5'
			ELSE 'invalid'
		END AS rating_bucket,

        (driver_rating IS NOT NULL) AS is_rated,

        CURRENT_TIMESTAMP AS _processed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        NULL::varchar AS _openlineage_run_id

    FROM src

)

SELECT *
FROM enriched