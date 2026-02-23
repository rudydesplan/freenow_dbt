{{ config(materialized='table') }}

with valid as (
    select
        driver_id,
        date_registration,
        driver_rating,
        rating_count,
        receive_marketing,
        country,
        _batch_id    as _source_batch_id,
        _source_uri,
        _ingested_at as _bronze_ingested_at,
        _row_number
    from {{ ref('stg_drivers') }}
    where error_code is null
      and driver_id is not null
),

-- Step 1: remove perfect duplicates (keep the latest copy of the identical row)
dedup_full as (
    select *
    from (
        select
            v.*,
            row_number() over (
                partition by
                    driver_id,
                    date_registration,
                    driver_rating,
                    rating_count,
                    receive_marketing,
                    country
                order by _bronze_ingested_at desc, _row_number desc
            ) as rn_full
        from valid v
    ) x
    where rn_full = 1
),

-- Step 2: if driver_id still duplicated, keep latest version
rank_driver as (
    select
        d.*,
        row_number() over (
            partition by driver_id
            order by _bronze_ingested_at desc, _row_number desc
        ) as rn_driver
    from dedup_full d
)

select
    driver_id,
    date_registration,
    driver_rating,
    rating_count,
    receive_marketing,
    country,

    _source_batch_id,
    _source_uri,
    _bronze_ingested_at,
    current_timestamp as _processed_at,
    '{{ invocation_id }}' as _dbt_invocation_id,
    null as _openlineage_run_id
from rank_driver
where rn_driver = 1