{{ config(materialized='table') }}

SELECT
    driver_id AS id,
    NULL AS date_registration,
    NULL AS driver_rating,
    NULL AS rating_count,
    NULL AS receive_marketing,
    NULL AS country,

    error_code,
    'Validation failed in staging layer' AS error_detail,

    _batch_id,
    _source_uri,
    _ingested_at,
    _row_number,
    CURRENT_TIMESTAMP AS _quarantined_at

FROM {{ ref('stg_drivers') }}

WHERE error_code IS NOT NULL