{{ config(
    materialized='incremental',
) }}


WITH int_pbp_player AS (
    SELECT *
    FROM {{ ref("int_pbp__player") }}
)

SELECT *
FROM int_pbp_player