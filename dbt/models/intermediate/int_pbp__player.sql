{{ config(
    materialized = 'incremental'
) }}

WITH stg_pbp_player AS (
	SELECT *
	FROM {{ ref("stg_pbp__player") }}
	{% if is_incremental() %}
	WHERE EXTRACT(YEAR FROM official_date) >= DATE_TRUNC('year', '{{ var("date_variable") }}')
	{% endif %}
),

obtain_distinct_records AS (
    SELECT
        DISTINCT
		batter_source_id,
		batter_full_name,
		batter_team,
		season,
		at_bat_index,
		game_source_id,
		pa_result,
		pa_rbis,
		hit_flag,
		ab_flag,
		slg_flag
    FROM stg_pbp_player
	WHERE 1=1
	AND season = EXTRACT(YEAR FROM CAST('{{ var("date_variable", "1900-01-01") }}' AS DATE))
	AND 
    (
        play_type = 'pitch' OR 
        (
            play_type = 'no_pitch' AND (
                pa_description LIKE '%Automatic Ball%' AND 
                    (count_balls_start = 4 OR count_strikes_start = 3) OR 
                (
                    pa_description LIKE '%Automatic Strike%' AND 
                    (count_balls_start = 4 OR count_strikes_start = 3)
                )
            )
        )
    )
    AND pa_result NOT LIKE '%Caught Stealing%'
    AND pa_result NOT LIKE '%Pickoff%'
    AND pa_result NOT LIKE '%Runner Out%'
	AND pa_result NOT LIKE '%Wild Pitch%'
	AND pa_result NOT LIKE '%Wild Pitch%'
),

add_data_filter AS (
	SELECT *
	FROM obtain_distinct_records
	WHERE NOT (hit_flag = 0 AND pa_result IN ('Single', 'Double', 'Triple', 'Home Run'))
),

add_initial_statistics AS (
    SELECT 
		batter_source_id, 
		batter_full_name, 
		batter_team,
		season,
		COUNT(DISTINCT game_source_id) AS num_g,
        COUNT(*) AS PA,
        SUM(ab_flag) AS AB,
		SUM(hit_flag) AS num_hits,
		SUM(CASE WHEN pa_result = 'Single' THEN 1 ELSE 0 END) AS num_singles,
		SUM(CASE WHEN pa_result = 'Double' THEN 1 ELSE 0 END) AS num_doubles,
		SUM(CASE WHEN pa_result = 'Triple' THEN 1 ELSE 0 END) AS num_triples,
		SUM(CASE WHEN pa_result = 'Home Run' THEN 1 ELSE 0 END) AS num_hr,
		SUM(pa_rbis) as num_rbi,
		SUM(CASE WHEN pa_result IN ('Walk', 'Intent Walk') THEN 1 ELSE 0 END) AS num_walks,
		SUM(CASE WHEN pa_result LIKE '%Strikeout%' THEN 1 ELSE 0 END) AS num_strikeouts,
		SUM(slg_flag) as total_bases,
		SUM(CASE WHEN pa_result = 'Grounded Into DP' THEN 1 ELSE 0 END) AS num_gidp,
        SUM(CASE WHEN pa_result IN ('Hit By Pitch') THEN 1 ELSE 0 END) AS num_hbp,
		SUM(CASE WHEN pa_result = 'Sac Bunt' THEN 1 ELSE 0 END) AS num_sh,
		SUM(CASE WHEN pa_result LIKE '%Sac Fly%' THEN 1 ELSE 0 END) AS num_sf,
		SUM(CASE WHEN pa_result ='Intent Walk' THEN 1 ELSE 0 END) AS num_ibb
	FROM add_data_filter
	GROUP BY 
		batter_source_id, 
		batter_full_name, 
		batter_team,
		season
),

add_averages_and_slg AS (
	SELECT 
		*,
		ROUND(num_hits * 1.0 / NULLIF(AB, 0), 3) AS batting_avg,
		ROUND((num_hits + num_walks + num_hbp) * 1.0 / NULLIF(AB + num_walks + num_hbp + num_sf, 0), 3) AS obp,
		ROUND(total_bases * 1.0 / NULLIF(AB, 0), 3) AS slg
	FROM add_initial_statistics
),

add_ops AS (
	SELECT 
		*,
		obp + slg AS ops
	FROM add_averages_and_slg
),

add_processdate AS (
	SELECT 
		*,
		'{{ var("date_variable", "1900-01-01") }}' as as_of_date
	FROM add_ops
),

reorder_cols AS (
    SELECT 
        batter_source_id, 
		batter_full_name, 
		batter_team,
		num_g,
        PA,
        AB,
		num_hits,
		num_singles,
		num_doubles,
		num_triples,
		num_hr,
		num_rbi,
		num_walks,
		num_strikeouts,
        batting_avg,
        obp,
        slg,
        ops,
		total_bases,
		num_gidp,
        num_hbp,
		num_sh,
		num_sf,
		num_ibb,
		season,
		as_of_date
    FROM add_processdate
)

SELECT *
FROM reorder_cols
