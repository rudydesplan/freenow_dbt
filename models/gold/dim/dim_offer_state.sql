{{ config(materialized='table') }}

with states as (
    select distinct upper(state) as offer_state
    from {{ ref('offers') }}
    where state is not null
),

filtered as (
    -- garde uniquement les 2 attendus si tu veux être strict
    select offer_state
    from states
    where offer_state in ('ACCEPTED', 'CANCELED')
)

select
    row_number() over (order by offer_state)::smallint as offer_state_key,
    offer_state
from filtered