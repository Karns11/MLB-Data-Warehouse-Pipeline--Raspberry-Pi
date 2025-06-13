{{ config(
    materialized = 'incremental'
) }}


WITH source AS (
	SELECT *
	FROM {{ source("baseballr_db", "mlb_pbp_data") }}
	{% if is_incremental() %}
	WHERE official_date = '{{ var("date_variable", "1900-01-01") }}'
	{% endif %}
),

renamed AS (
	SELECT 
		matchup_batter_id AS batter_source_id,
		matchup_batter_fullname AS batter_full_name,
		batting_team AS batter_team,
		CAST(atBatIndex AS BIGINT) AS at_bat_index,
		game_pk AS game_source_id,
		result_event AS pa_result,
		details_description AS pa_description,
		result_rbi AS pa_rbis,
		type AS play_type,
		ispitch AS is_pitch,
		pitchnumber,
		count_strikes_start,
		count_balls_start,
		EXTRACT(YEAR FROM CAST(official_date AS DATE)) AS season,
		CAST(official_date AS DATE) AS official_date
	FROM source
),

add_flags AS (
	SELECT 
		*,
		CASE 
			WHEN pa_result IN ('Single', 'Double', 'Triple', 'Home Run') AND is_pitch = 'true' THEN 1
			ELSE 0
		END AS hit_flag,
		CASE 
			WHEN pa_result NOT LIKE '%Walk%' AND pa_result NOT LIKE '%Hit By Pitch%' AND pa_result NOT LIKE 'Sac%' AND pa_result NOT LIKE '%Catcher Interference%' AND pa_result NOT LIKE '%Intent Walk%' THEN 1
			ELSE 0
		END AS ab_flag,
		CASE 
			WHEN pa_result = 'Single' THEN 1
			WHEN pa_result = 'Double' THEN 2
			WHEN pa_result = 'Triple' THEN 3
			WHEN pa_result = 'Home Run' THEN 4
			ELSE 0
		END AS slg_flag
	FROM renamed
)

SELECT *
FROM add_flags