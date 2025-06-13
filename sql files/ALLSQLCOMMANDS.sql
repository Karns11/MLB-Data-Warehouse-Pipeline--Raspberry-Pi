DELETE FROM raw.mlb_pbp_data;

DELETE FROM raw.mlb_people_data;

DELETE FROM raw.mlb_team_data;

DELETE FROM warehouse.dim_player;

DELETE FROM warehouse.dim_team;

DELETE FROM warehouse.fact_player_stats;

DELETE FROM warehouse.fact_player_stats_history;

DELETE FROM warehouse.fact_team_games;

DELETE FROM warehouse.fact_team_games_history;


CREATE SCHEMA raw;

CREATE TABLE raw.stg_people (
    source_id                    INTEGER,
    full_name                    TEXT,
    first_name                   TEXT,
    last_name                    TEXT,
    player_link                  TEXT,         -- was 'link'
    primary_number               TEXT,
    birth_date                   TEXT,
    birth_city                   TEXT,
    birth_state_province         TEXT,
    birth_country                TEXT,
    height                       TEXT,
    weight                       TEXT,
    is_active                    TEXT,      -- was 'active'
    draft_year                   INTEGER,
    mlb_debut_date               TEXT,
    strike_zone_top              NUMERIC(5,2),
    strike_zone_bottom           NUMERIC(5,2),
    primary_position_name        TEXT,
    primary_position_type        TEXT,
    primary_position_abbreviation TEXT,
    bat_side_code                TEXT,
    pitch_hand_code              TEXT,
    team                         TEXT,
    official_date                DATE
);




CREATE SCHEMA warehouse;

CREATE TABLE warehouse.dim_player (
    player_sk                    SERIAL PRIMARY KEY,             -- Auto-incrementing surrogate key
    source_id                    INTEGER,
    full_name                    TEXT,
    first_name                   TEXT,
    last_name                    TEXT,
    player_link                  TEXT,
    primary_number               TEXT,
    birth_date                   TEXT,
    birth_city                   TEXT,
    birth_state_province         TEXT,
    birth_country                TEXT,
    height                       TEXT,
    weight                       TEXT,
    is_active                    TEXT,
    draft_year                   INTEGER,
    mlb_debut_date               TEXT,
    strike_zone_top              NUMERIC(5,2),
    strike_zone_bottom           NUMERIC(5,2),
    primary_position_name        TEXT,
    primary_position_type        TEXT,
    primary_position_abbreviation TEXT,
    bat_side_code                TEXT,
    pitch_hand_code              TEXT,
    team                         TEXT,

    -- SCD Type 2 tracking fields
    effective_date               DATE NOT NULL,
    expire_date                  DATE,
    current_flag                 CHAR(1) NOT NULL DEFAULT 'Y'
);




CREATE TABLE raw.stg_teams (
    source_id            TEXT,
    team_full_name       TEXT,
    team_link            TEXT,
    team_code            TEXT,
    file_code            TEXT,
    team_abbreviation    TEXT,
    team_name            TEXT,
    location_name        TEXT,
    first_year_of_play   TEXT,
    short_name           TEXT,
    franchise_name       TEXT,
    club_name            TEXT,
    active               TEXT,  -- derived from 'active' in source
    venue_name           TEXT,
    league_name          TEXT,
    division_name        TEXT,
    sport_name           TEXT,
    official_date        DATE
);





CREATE TABLE warehouse.dim_team (
	team_sk              SERIAL PRIMARY KEY,             -- Auto-incrementing surrogate key
    source_id            TEXT,
    team_full_name       TEXT,
    team_link            TEXT,
    team_code            TEXT,
    file_code            TEXT,
    team_abbreviation    TEXT,
    team_name            TEXT,
    location_name        TEXT,
    first_year_of_play   TEXT,
    short_name           TEXT,
    franchise_name       TEXT,
    club_name            TEXT,
    is_active               TEXT,  -- derived from 'active' in source
    venue_name           TEXT,
    league_name          TEXT,
    division_name        TEXT,
    sport_name           TEXT,

    -- SCD Type 2 tracking fields
    effective_date                 DATE NOT NULL,
    expire_date                    DATE,
    current_flag                   CHAR(1) NOT NULL DEFAULT 'Y'

);


CREATE TABLE warehouse.fact_player_stats_history (
    player_sk         INTEGER,
    team_sk           INTEGER,
    num_g             BIGINT,
    pa                BIGINT,
    ab                BIGINT,
    num_hits          BIGINT,
    num_singles       BIGINT,
    num_doubles       BIGINT,
    num_triples       BIGINT,
    num_hr            BIGINT,
    num_rbi           BIGINT,
    num_walks         BIGINT,
    num_strikeouts    BIGINT,
    batting_avg       NUMERIC,
    obp               NUMERIC,
    slg               NUMERIC,
    ops               NUMERIC,
    total_bases       BIGINT,
    num_gidp          BIGINT,
	num_hbp           BIGINT,
	num_sh            BIGINT,
	num_sf            BIGINT,
	num_ibb           BIGINT,
	season            NUMERIC,
	as_of_date        DATE
);



CREATE TABLE warehouse.fact_team_games_history (
    team_sk           INTEGER,
    season            NUMERIC,
    gp                BIGINT,
	as_of_date        DATE
);


CREATE TABLE warehouse.dim_date (
	as_of_date          DATE,
    date_id             INTEGER,
    day                 INTEGER,
    day_suffix          TEXT,
    day_name            TEXT,
    weekday_ind         BOOLEAN,
    weekday_num         INTEGER,
    week_of_month       INTEGER,
    week_of_year        INTEGER,
    iso_week            INTEGER,
    month               INTEGER,
    month_name          TEXT,
    month_abbrev        TEXT,
    quarter             INTEGER,
    quarter_name        TEXT,
    year                INTEGER,
    iso_year            INTEGER,
    year_month          TEXT,
    year_quarter        TEXT,
    first_day_of_month  DATE,
    last_day_of_month   DATE,
    first_day_of_qtr    DATE,
    last_day_of_qtr     DATE,
    first_day_of_year   DATE,
    last_day_of_year    DATE,
    day_of_year         INTEGER,
    is_holiday          BOOLEAN,
    holiday_name        TEXT,
    fiscal_month        INTEGER,
    fiscal_quarter      INTEGER,
    fiscal_year         INTEGER,
    is_weekend          BOOLEAN,
    is_last_day_of_month BOOLEAN,
    is_first_day_of_month BOOLEAN,
    prev_day            DATE,
    next_day            DATE,
    prev_month          TEXT,
    next_month          TEXT,
    prev_year           INTEGER,
    next_year           INTEGER,
    days_in_month       INTEGER,
    leap_year           BOOLEAN,
    iso_day_of_week     INTEGER,
    unix_timestamp      BIGINT,
    julian_date         DOUBLE PRECISION,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

