{{ config(
    materialized='incremental',
) }}


WITH int_pbp_team AS (
    SELECT *
    FROM {{ ref("int_pbp__team_gp") }}
)

SELECT *
FROM int_pbp_team