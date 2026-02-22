{{ config(materialized='table') }}

SELECT
    driver_id            AS id,
    date_registration_raw AS date_registration,
    driver_rating_raw     AS driver_rating,
    rating_count_raw      AS rating_count,
    receive_marketing_raw AS receive_marketing,
    country_raw           AS country,

    error_code,
    error_detail,

    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM {{ ref('stg_drivers') }}
WHERE error_code IS NOT NULL