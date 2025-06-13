{{ config(
    materialized = 'incremental'
) }}

WITH stg_pbp_team_gp AS (
	SELECT *
	FROM {{ ref("stg_pbp__team_gp") }}
	{% if is_incremental() %}
	WHERE EXTRACT(YEAR FROM official_date) >= DATE_TRUNC('year', '{{ var("date_variable") }}')
	{% endif %}
),

calculate_gp AS (
    SELECT 
	    batter_team as team_full_name,
        EXTRACT(YEAR FROM CAST(official_date AS DATE)) AS season,
	    COUNT(DISTINCT game_source_id) AS gp
    FROM stg_pbp_team_gp
    GROUP BY batter_team, EXTRACT(YEAR FROM CAST(official_date AS DATE))
),

add_processdate AS (
	SELECT 
		*,
		'{{ var("date_variable", "1900-01-01") }}' as as_of_date
	FROM calculate_gp
)

SELECT *
FROM add_processdate