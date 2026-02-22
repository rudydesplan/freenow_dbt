{{ config(
    materialized='incremental',
    unique_key='booking_status'
) }}

WITH source_statuses AS (

    SELECT DISTINCT
        UPPER(status) AS booking_status
    FROM {{ ref('bookings') }}  -- silver.bookings

    WHERE status IS NOT NULL

),

new_statuses AS (

    SELECT s.booking_status
    FROM source_statuses s

    LEFT JOIN {{ this }} d
      ON s.booking_status = d.booking_status

    WHERE d.booking_status IS NULL

)

-- For incremental runs, only insert new statuses
SELECT
    booking_status
FROM new_statuses