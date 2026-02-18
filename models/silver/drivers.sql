{{ config(materialized='table') }}

SELECT
    driver_id,
    date_registration,
    driver_rating,
    rating_count,
    receive_marketing,
    country,

    _batch_id AS _source_batch_id,
    _source_uri,
    _ingested_at AS _bronze_ingested_at,
    CURRENT_TIMESTAMP AS _processed_at,
    '{{ invocation_id }}' AS _dbt_invocation_id,
    NULL AS _openlineage_run_id

FROM {{ ref('stg_drivers') }}

WHERE error_code IS NULL
  AND driver_id IS NOT NULL