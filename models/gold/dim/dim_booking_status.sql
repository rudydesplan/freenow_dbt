{{ config(materialized='table') }}

WITH source_statuses AS (

    SELECT DISTINCT
        UPPER(status) AS booking_status
    FROM {{ ref('bookings') }}
    WHERE status IS NOT NULL

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['booking_status']) }} 
        AS booking_status_key,
    booking_status
FROM source_statuses
ORDER BY booking_status