{{ config(materialized='table') }}

WITH source_states AS (

    SELECT DISTINCT
        UPPER(state) AS offer_state
    FROM {{ ref('offers') }}
    WHERE state IS NOT NULL

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['offer_state']) }} 
        AS offer_state_key,
    offer_state
FROM source_states
ORDER BY offer_state