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

-- Step 1: remove perfect duplicates (same rule as silver)
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

-- Step 2: detect remaining duplicates by driver_id (these differ in values)
rank_driver as (
    select
        d.*,
        row_number() over (
            partition by driver_id
            order by _bronze_ingested_at desc, _row_number desc
        ) as rn_driver,
        count(*) over (partition by driver_id) as driver_id_count
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
    _row_number,

    'DUPLICATE_DRIVER_ID_VARIANT' as error_code,
    'same driver_id appears with different attribute values; kept latest in silver' as error_detail,

    current_timestamp as _processed_at,
    '{{ invocation_id }}' as _dbt_invocation_id
from rank_driver
where driver_id_count > 1
  and rn_driver > 1;