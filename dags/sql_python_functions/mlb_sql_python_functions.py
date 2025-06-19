from sqlalchemy import create_engine

def add_people_data_to_staging(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        TRUNCATE TABLE raw.stg_people; 

        INSERT INTO raw.stg_people (
        source_id, full_name, first_name, last_name, player_link,
        primary_number, birth_date, birth_city, birth_state_province,
        birth_country, height, weight, is_active, draft_year,
        mlb_debut_date, strike_zone_top, strike_zone_bottom,
        primary_position_name, primary_position_type,
        primary_position_abbreviation, bat_side_code,
        pitch_hand_code, team, official_date
        )
        SELECT
            DISTINCT
            id                       AS source_id,
            full_name,
            first_name,
            last_name,
            link                     AS player_link,
            primary_number,
            birth_date,
            birth_city,
            birth_state_province,
            birth_country,
            height,
            weight,
            active                   AS is_active,
            draft_year,
            mlb_debut_date,
            strike_zone_top,
            strike_zone_bottom,
            primary_position_name,
            primary_position_type,
            primary_position_abbreviation,
            bat_side_code,
            pitch_hand_code,
            team,
            official_date::DATE
        FROM raw.mlb_people_data
        WHERE official_date = '{date_str}';
    """

    with engine.begin() as conn:
        conn.execute(query)



def update_people_dim(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        UPDATE warehouse.dim_player d
        SET 
            expire_date = s.official_date - INTERVAL '1 day',
            current_flag = 'N'
        FROM raw.stg_people s
        WHERE d.source_id = s.source_id
        AND d.current_flag = 'Y'
        AND (
            d.full_name IS DISTINCT FROM s.full_name OR
            d.first_name IS DISTINCT FROM s.first_name OR
            d.last_name IS DISTINCT FROM s.last_name OR
            d.player_link IS DISTINCT FROM s.player_link OR
            d.primary_number IS DISTINCT FROM s.primary_number OR
            d.birth_date IS DISTINCT FROM s.birth_date OR
            d.birth_city IS DISTINCT FROM s.birth_city OR
            d.birth_state_province IS DISTINCT FROM s.birth_state_province OR
            d.birth_country IS DISTINCT FROM s.birth_country OR
            d.height IS DISTINCT FROM s.height OR
            d.weight IS DISTINCT FROM s.weight OR
            d.is_active IS DISTINCT FROM s.is_active OR
            d.draft_year IS DISTINCT FROM s.draft_year OR
            d.mlb_debut_date IS DISTINCT FROM s.mlb_debut_date OR
            d.strike_zone_top IS DISTINCT FROM s.strike_zone_top OR
            d.strike_zone_bottom IS DISTINCT FROM s.strike_zone_bottom OR
            d.primary_position_name IS DISTINCT FROM s.primary_position_name OR
            d.primary_position_type IS DISTINCT FROM s.primary_position_type OR
            d.primary_position_abbreviation IS DISTINCT FROM s.primary_position_abbreviation OR
            d.bat_side_code IS DISTINCT FROM s.bat_side_code OR
            d.pitch_hand_code IS DISTINCT FROM s.pitch_hand_code OR
            d.team IS DISTINCT FROM s.team
        );
    """

    with engine.begin() as conn:
        conn.execute(query)       




def insert_people_dim_data(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        INSERT INTO warehouse.dim_player (
        source_id, full_name, first_name, last_name, player_link,
        primary_number, birth_date, birth_city, birth_state_province,
        birth_country, height, weight, is_active, draft_year,
        mlb_debut_date, strike_zone_top, strike_zone_bottom,
        primary_position_name, primary_position_type,
        primary_position_abbreviation, bat_side_code,
        pitch_hand_code, team, effective_date, expire_date, current_flag
    )
    SELECT 
        s.source_id, s.full_name, s.first_name, s.last_name, s.player_link,
        s.primary_number, s.birth_date, s.birth_city, s.birth_state_province,
        s.birth_country, s.height, s.weight, s.is_active, s.draft_year,
        s.mlb_debut_date, s.strike_zone_top, s.strike_zone_bottom,
        s.primary_position_name, s.primary_position_type,
        s.primary_position_abbreviation, s.bat_side_code,
        s.pitch_hand_code, s.team, s.official_date,
        DATE '9999-12-31', 'Y'
    FROM raw.stg_people s
    LEFT JOIN warehouse.dim_player d
    ON s.source_id = d.source_id AND d.current_flag = 'Y'
    WHERE d.source_id IS NULL
    OR (
        d.full_name IS DISTINCT FROM s.full_name OR
        d.first_name IS DISTINCT FROM s.first_name OR
        d.last_name IS DISTINCT FROM s.last_name OR
        d.player_link IS DISTINCT FROM s.player_link OR
        d.primary_number IS DISTINCT FROM s.primary_number OR
        d.birth_date IS DISTINCT FROM s.birth_date OR
        d.birth_city IS DISTINCT FROM s.birth_city OR
        d.birth_state_province IS DISTINCT FROM s.birth_state_province OR
        d.birth_country IS DISTINCT FROM s.birth_country OR
        d.height IS DISTINCT FROM s.height OR
        d.weight IS DISTINCT FROM s.weight OR
        d.is_active IS DISTINCT FROM s.is_active OR
        d.draft_year IS DISTINCT FROM s.draft_year OR
        d.mlb_debut_date IS DISTINCT FROM s.mlb_debut_date OR
        d.strike_zone_top IS DISTINCT FROM s.strike_zone_top OR
        d.strike_zone_bottom IS DISTINCT FROM s.strike_zone_bottom OR
        d.primary_position_name IS DISTINCT FROM s.primary_position_name OR
        d.primary_position_type IS DISTINCT FROM s.primary_position_type OR
        d.primary_position_abbreviation IS DISTINCT FROM s.primary_position_abbreviation OR
        d.bat_side_code IS DISTINCT FROM s.bat_side_code OR
        d.pitch_hand_code IS DISTINCT FROM s.pitch_hand_code OR
        d.team IS DISTINCT FROM s.team
    );
    """

    with engine.begin() as conn:
        conn.execute(query)




def add_team_data_to_staging(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        TRUNCATE TABLE raw.stg_teams; 

        INSERT INTO raw.stg_teams (
        source_id, team_full_name, team_link, team_code, file_code, team_abbreviation, 
        team_name, location_name, first_year_of_play, short_name, franchise_name,
        club_name, active, venue_name, league_name, division_name, sport_name, official_date

        )
        SELECT
            DISTINCT
            team_id AS source_id,
            team_full_name,
            link AS team_link,
            team_code,
            file_code,
            team_abbreviation,
            team_name,
            location_name,
            first_year_of_play,
            short_name,
            franchise_name,
            club_name,
            active AS is_active,
            venue_name,
            league_name,
            division_name,
            sport_name,
            official_date::DATE
        FROM raw.mlb_team_data
        WHERE official_date = '{date_str}';
    """

    with engine.begin() as conn:
        conn.execute(query)



def update_team_dim(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        UPDATE warehouse.dim_team d
        SET 
            expire_date = s.official_date - INTERVAL '1 day',
            current_flag = 'N'
        FROM raw.stg_teams s
        WHERE d.source_id = s.source_id
        AND d.current_flag = 'Y'
        AND (
            d.source_id IS DISTINCT FROM s.source_id OR
            d.team_full_name IS DISTINCT FROM s.team_full_name OR
            d.team_link IS DISTINCT FROM s.team_link OR
            d.team_code IS DISTINCT FROM s.team_code OR
            d.file_code IS DISTINCT FROM s.file_code OR
            d.team_abbreviation IS DISTINCT FROM s.team_abbreviation OR
            d.team_name IS DISTINCT FROM s.team_name OR
            d.location_name IS DISTINCT FROM s.location_name OR
            d.first_year_of_play IS DISTINCT FROM s.first_year_of_play OR
            d.short_name IS DISTINCT FROM s.short_name OR
            d.franchise_name IS DISTINCT FROM s.franchise_name OR
            d.club_name IS DISTINCT FROM s.club_name OR
            d.is_active IS DISTINCT FROM s.active OR
            d.venue_name IS DISTINCT FROM s.venue_name OR
            d.league_name IS DISTINCT FROM s.league_name OR
            d.division_name IS DISTINCT FROM s.division_name OR
            d.sport_name IS DISTINCT FROM s.sport_name 
        );
    """

    with engine.begin() as conn:
        conn.execute(query)



def insert_team_dim_data(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        INSERT INTO warehouse.dim_team (
        source_id, team_full_name, team_link, team_code, file_code, team_abbreviation, 
        team_name, location_name, first_year_of_play, short_name, franchise_name,
        club_name, is_active, venue_name, league_name, division_name, sport_name, effective_date, expire_date, current_flag
    )
    SELECT 
        s.source_id, s.team_full_name, s.team_link, s.team_code, s.file_code, s.team_abbreviation, 
        s.team_name, s.location_name, s.first_year_of_play, s.short_name, s.franchise_name,
        s.club_name, s.active, s.venue_name, s.league_name, s.division_name, s.sport_name, s.official_date,
        DATE '9999-12-31', 'Y'
    FROM raw.stg_teams s
    LEFT JOIN warehouse.dim_team d
    ON s.source_id = d.source_id AND d.current_flag = 'Y'
    WHERE d.source_id IS NULL
    OR (
        d.team_full_name IS DISTINCT FROM s.team_full_name OR
        d.team_link IS DISTINCT FROM s.team_link OR
        d.team_code IS DISTINCT FROM s.team_code OR
        d.file_code IS DISTINCT FROM s.file_code OR
        d.team_abbreviation IS DISTINCT FROM s.team_abbreviation OR
        d.team_name IS DISTINCT FROM s.team_name OR
        d.location_name IS DISTINCT FROM s.location_name OR
        d.first_year_of_play IS DISTINCT FROM s.first_year_of_play OR
        d.short_name IS DISTINCT FROM s.short_name OR
        d.franchise_name IS DISTINCT FROM s.franchise_name OR
        d.club_name IS DISTINCT FROM s.club_name OR
        d.is_active IS DISTINCT FROM s.active OR
        d.venue_name IS DISTINCT FROM s.venue_name OR
        d.league_name IS DISTINCT FROM s.league_name OR
        d.division_name IS DISTINCT FROM s.division_name OR
        d.sport_name IS DISTINCT FROM s.sport_name
    );
    """

    with engine.begin() as conn:
        conn.execute(query)



def add_surrogate_keys_to_fact(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        DROP TABLE IF EXISTS warehouse.fact_player_stats;

        CREATE TABLE warehouse.fact_player_stats AS

        WITH season_to_date_fact AS (
        SELECT * 
        FROM raw.player_season_to_date_fact
        WHERE as_of_date = '{date_str}'
        ),

        people_dim AS (
            SELECT *
            FROM warehouse.dim_player
            WHERE expire_date = '9999-12-31'
        ),

        teams_dim AS (
            SELECT *
            FROM warehouse.dim_team
            WHERE expire_date = '9999-12-31'
        )

        SELECT 
            people_dim.player_sk, 
            teams_dim.team_sk,
            season_to_date_fact.num_g,
            season_to_date_fact.pa,
            season_to_date_fact.ab,
            season_to_date_fact.num_hits,
            season_to_date_fact.num_singles,
            season_to_date_fact.num_doubles,
            season_to_date_fact.num_triples,
            season_to_date_fact.num_hr,
            season_to_date_fact.num_rbi,
            season_to_date_fact.num_walks,
            season_to_date_fact.num_strikeouts,
            season_to_date_fact.batting_avg,
            season_to_date_fact.obp,
            season_to_date_fact.slg,
            season_to_date_fact.ops,
            season_to_date_fact.total_bases,
            season_to_date_fact.num_gidp,
            season_to_date_fact.num_hbp,
            season_to_date_fact.num_sh,
            season_to_date_fact.num_sf,
            season_to_date_fact.num_ibb,
            season_to_date_fact.season,
            season_to_date_fact.as_of_date::DATE
        FROM season_to_date_fact
        JOIN people_dim
            ON people_dim.source_id = season_to_date_fact.batter_source_id
            AND people_dim.full_name = season_to_date_fact.batter_full_name
            AND people_dim.team = season_to_date_fact.batter_team
        JOIN teams_dim
            ON teams_dim.team_full_name = season_to_date_fact.batter_team;
    """

    with engine.begin() as conn:
        conn.execute(query)



def append_stats(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        INSERT INTO warehouse.fact_player_stats_history
        SELECT * 
        FROM warehouse.fact_player_stats
        WHERE as_of_date = '{date_str}';
    """

    with engine.begin() as conn:
        conn.execute(query)



def add_surrogate_keys_to_team_games_fact(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        DROP TABLE IF EXISTS warehouse.fact_team_games;

        CREATE TABLE warehouse.fact_team_games AS

        WITH team_season_to_date_fact AS (
        SELECT * 
        FROM raw.team_season_to_date_fact
        WHERE as_of_date = '{date_str}'
        ),

        teams_dim AS (
            SELECT *
            FROM warehouse.dim_team
            WHERE expire_date = '9999-12-31'
        )

        SELECT 
            teams_dim.team_sk,
            team_season_to_date_fact.season,
            team_season_to_date_fact.gp,
            team_season_to_date_fact.as_of_date::DATE
        FROM team_season_to_date_fact
        JOIN teams_dim
            ON teams_dim.team_full_name = team_season_to_date_fact.team_full_name;
    """

    with engine.begin() as conn:
        conn.execute(query)



def append_team_game_stats(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        INSERT INTO warehouse.fact_team_games_history
        SELECT * 
        FROM warehouse.fact_team_games
        WHERE as_of_date = '{date_str}';
    """

    with engine.begin() as conn:
        conn.execute(query)


def update_is_currently_qualified_field(date_str: str):
    # Create SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://root:root@my_postgres:5432/baseballr_db')

    query = f"""
        ALTER TABLE warehouse.fact_player_stats
        ADD COLUMN is_currently_qualified SMALLINT DEFAULT 0;

        WITH qualified_players AS (
        SELECT 
            fps.player_sk,
            fps.team_sk,
            fps.as_of_date
        FROM warehouse.fact_player_stats fps
        JOIN warehouse.fact_team_games_history ftgh
            ON fps.team_sk = ftgh.team_sk
        AND fps.as_of_date = ftgh.as_of_date
        WHERE 1=1
            AND fps.as_of_date = (SELECT MAX(as_of_Date) FROM warehouse.fact_player_stats)
            AND fps.pa >= 3.1 * ftgh.gp
        )
        UPDATE warehouse.fact_player_stats fps
        SET is_currently_qualified = 1
        FROM qualified_players qp
        WHERE fps.player_sk = qp.player_sk
        AND fps.team_sk = qp.team_sk
        AND fps.as_of_date = qp.as_of_date;

        

        UPDATE warehouse.fact_player_stats_history
        SET is_currently_qualified = 0;

        WITH qualified_players AS (
        SELECT 
            fpsh.player_sk,
            fpsh.team_sk,
            fpsh.as_of_date
        FROM warehouse.fact_player_stats_history fpsh
        JOIN warehouse.fact_team_games_history ftgh
            ON fpsh.team_sk = ftgh.team_sk
        AND fpsh.as_of_date = ftgh.as_of_date
        WHERE 1=1
            AND fpsh.as_of_date = (SELECT MAX(as_of_Date) FROM warehouse.fact_player_stats_history)
            AND fpsh.pa >= 3.1 * ftgh.gp
        )
        UPDATE warehouse.fact_player_stats_history fpsh
        SET is_currently_qualified = 1
        FROM qualified_players qp
        WHERE fpsh.player_sk = qp.player_sk
        AND fpsh.team_sk = qp.team_sk
        AND fpsh.as_of_date = qp.as_of_date;
    """

    with engine.begin() as conn:
        conn.execute(query)