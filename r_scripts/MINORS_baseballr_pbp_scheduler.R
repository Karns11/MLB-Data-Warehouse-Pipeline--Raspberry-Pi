#Load all packages
library(baseballr)
library(dplyr)
library(tidyr)
library(lubridate)
library(progressr)
library(stringr)
library(readr)
library(DBI)
library(RPostgres)
library(purrr)

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop("Please provide a process date")
}

passed_processdate <- as.Date(args[1])

cat("passed processdate: ", as.character(passed_processdate), "\n")

pg_host <- Sys.getenv("MY_PG_HOST")
pg_port <- Sys.getenv("MY_PG_PORT")
pg_user <- Sys.getenv("MY_PG_USER")
pg_password <- Sys.getenv("MY_PG_PASSWORD")
pg_db <- Sys.getenv("MY_PG_DB")

# Connect to PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  host = pg_host,
  port = pg_port,
  user = pg_user,
  password = pg_password,
  dbname = pg_db
)


year <- year(passed_processdate)

print("Obtaining all regular season games for current year...")
payload_all_games_schedule <- purrr::map_df(.x = year,
                                            ~baseballr::mlb_schedule(season = .x, level_ids = c("11","12","13", "14", "16"))) # nolint

all_possible_processdates <- payload_all_games_schedule %>%
  filter(season == year) %>%
  filter(series_description == "Regular Season") %>%
  filter(status_status_code == "F" | status_status_code == "FR") %>%
  filter(status_detailed_state != "Postponed") %>%
  distinct(date)



valid_dates <- as.Date(as.character(all_possible_processdates$date))

cat("Valid regular season games for current year:\n")
cat(format(valid_dates), sep = "\n")



valid_passed_processdate <- passed_processdate[passed_processdate %in% valid_dates] # nolint


print("Obtaining all dates already in db...")
query <- "SELECT DISTINCT processdate FROM raw.MINORS_pbp_data;"
all_processdates_in_table <- dbGetQuery(con, query)

already_processed <- as.Date(all_processdates_in_table$processdate)

if (length(already_processed) == 0) {
  cat("No dates already in db...", "\n")
} else {
  cat("all dates already in db:\n")
  cat(format(already_processed), sep = "\n")
}

date_sequence <- valid_passed_processdate[!valid_passed_processdate %in% already_processed] # nolint


if (length(date_sequence) == 0) {
  print("No valid dates to process. Exiting.")
  quit(save = "no", status = 99)
} else {
  print("DETERMINED... Running process for the following date: ")
  print(date_sequence)
}




#Define columns that will be kept from api call
column_names <- c("game_pk", "game_date", "index", "startTime", "endTime",
                  "isPitch", "type", "playId", "pitchNumber", "details.description", # nolint
                  "details.event", "details.awayScore", "details.homeScore",
                  "details.isScoringPlay", "details.hasReview", "details.code",
                  "details.ballColor", "details.isInPlay", "details.isStrike",
                  "details.isBall", "details.call.code", "details.call.description", # nolint
                  "count.balls.start", "count.strikes.start", "count.outs.start", # nolint
                  "player.id", "player.link", "pitchData.strikeZoneTop",
                  "pitchData.strikeZoneBottom", "details.fromCatcher",
                  "pitchData.coordinates.x", "pitchData.coordinates.y",
                  "hitData.trajectory", "hitData.hardness", "hitData.location",
                  "hitData.coordinates.coordX", "hitData.coordinates.coordY",
                  "actionPlayId", "details.eventType", "details.runnerGoing",
                  "position.code", "position.name", "position.type",
                  "position.abbreviation", "battingOrder", "atBatIndex",
                  "result.type", "result.event", "result.eventType",
                  "result.description", "result.rbi", "result.awayScore",
                  "result.homeScore", "about.atBatIndex", "about.halfInning",
                  "about.inning", "about.startTime", "about.endTime",
                  "about.isComplete", "about.isScoringPlay", "about.hasReview",
                  "about.hasOut", "about.captivatingIndex", "count.balls.end",
                  "count.strikes.end", "count.outs.end", "matchup.batter.id",
                  "matchup.batter.fullName", "matchup.batter.link",
                  "matchup.batSide.code", "matchup.batSide.description",
                  "matchup.pitcher.id", "matchup.pitcher.fullName",
                  "matchup.pitcher.link", "matchup.pitchHand.code",
                  "matchup.pitchHand.description", "matchup.splits.batter",
                  "matchup.splits.pitcher", "matchup.splits.menOnBase",
                  "batted.ball.result", "home_team", "home_level_id",
                  "home_level_name", "home_parentOrg_id", "home_parentOrg_name",
                  "home_league_id", "home_league_name", "away_team",
                  "away_level_id", "away_level_name", "away_parentOrg_id",
                  "away_parentOrg_name", "away_league_id", "away_league_name",
                  "batting_team", "fielding_team", "last.pitch.of.ab", "pfxId",
                  "details.trailColor", "details.type.code", "details.type.description", # nolint
                  "pitchData.startSpeed", "pitchData.endSpeed", "pitchData.zone", # nolint
                  "pitchData.typeConfidence", "pitchData.plateTime",
                  "pitchData.extension", "pitchData.coordinates.aY",
                  "pitchData.coordinates.aZ", "pitchData.coordinates.pfxX",
                  "pitchData.coordinates.pfxZ", "pitchData.coordinates.pX",
                  "pitchData.coordinates.pZ", "pitchData.coordinates.vX0",
                  "pitchData.coordinates.vY0", "pitchData.coordinates.vZ0",
                  "pitchData.coordinates.x0", "pitchData.coordinates.y0",
                  "pitchData.coordinates.z0", "pitchData.coordinates.aX",
                  "pitchData.breaks.breakAngle", "pitchData.breaks.breakLength",
                  "pitchData.breaks.breakY", "pitchData.breaks.spinRate",
                  "pitchData.breaks.spinDirection", "hitData.launchSpeed",
                  "hitData.launchAngle", "hitData.totalDistance", "injuryType",
                  "umpire.id", "umpire.link", "details.isOut",
                  "pitchData.breaks.breakVertical",
                  "pitchData.breaks.breakVerticalInduced",
                  "pitchData.breaks.breakHorizontal", "details.disengagementNum",# nolint
                  "isSubstitution", "replacedPlayer.id",
                  "replacedPlayer.link", "result.isOut", "about.isTopInning",
                  "matchup.postOnFirst.id", "matchup.postOnFirst.fullName",
                  "matchup.postOnFirst.link", "matchup.postOnThird.id",
                  "matchup.postOnThird.fullName", "matchup.postOnThird.link",
                  "matchup.postOnSecond.id", "matchup.postOnSecond.fullName",
                  "matchup.postOnSecond.link")


all_pbp_df_list <- list()
all_player_dim_df_list <- list()


#Make call to api for each date in sequence.

for (current_date in date_sequence) {

  print(paste("Starting iteration for date:", as.character(current_date)))

  current_date <- as.Date(current_date, origin = "1970-01-01")

  #Extract the year month and day from date
  year <- year(current_date)
  month <- month(current_date)
  day <- day(current_date)
  current_date <- as.character(current_date)

  #Grab all games for the given year, using mlb_schedule
  payload_all_games_schedule <- purrr::map_df(.x = year,
                                              ~baseballr::mlb_schedule(season = .x, level_ids = c("11","12","13", "14", "16"))) # nolint

  #Filter where season = current year
  entire_schedule_this_year <- payload_all_games_schedule %>%
    filter(season == year)

  #Filter where series description = regular season games
  regular_Season_games <- entire_schedule_this_year %>% # nolint
    filter(series_description == "Regular Season")

  #Filter where the status of the game is Final (F)
  regular_Season_games_final <- regular_Season_games %>% # nolint
    filter(status_status_code == "F" | status_status_code == "FR")

  #Do not include postponed games
  regular_Season_games_final <- regular_Season_games_final %>% # nolint
    filter(status_detailed_state != "Postponed")

  #Make sure the date of the game = current date in date sequence
  regular_Season_games_final <- regular_Season_games_final %>% # nolint
    filter(as.Date(date) == current_date)

  ###Get each distinct team_id's for the given year using mlb_team
  sport_ids <- c(11, 12, 13, 14, 16)

  distinct_teams <- map_dfr(sport_ids, ~ mlb_teams(season = year, sport_ids = .x)) %>% #nolint
    distinct(team_id) %>%
    arrange(team_id)

  #Make only only active teams are kept in regular_Season_games_final df
  distinct_teams_home_games <- regular_Season_games_final %>%
    filter(teams_home_team_id %in% distinct_teams$team_id)

  #Obtain all pbp data for every game_pk in distinct_teams_home_games df using get_pbp_mlb # nolint
  payload_distinct_team_home_games <- with_progress({ # nolint
    p_1 <- progressor(along = distinct_teams_home_games$game_pk)
    purrr::map_df(.x = distinct_teams_home_games$game_pk, ~{
      p_1()
      baseballr::get_pbp_mlb(game_pk = .x)
    })
  })

  #Only keep relevant columns as defined from above
  payload_distinct_team_home_games <- payload_distinct_team_home_games[, column_names] # nolint
  payload_distinct_team_home_games <- as.data.frame(payload_distinct_team_home_games) # nolint

  #create barrel field using launch angle and speed
  payload_distinct_team_home_games$launch_angle <- payload_distinct_team_home_games$hitData.launchAngle # nolint
  payload_distinct_team_home_games$launch_speed <- payload_distinct_team_home_games$hitData.launchSpeed # nolint

  payload_distinct_team_home_games <- code_barrel(payload_distinct_team_home_games) # nolint
  payload_distinct_team_home_games <- select(payload_distinct_team_home_games, -launch_speed, -launch_angle) # nolint


  #Create processdate field
  processdate <- as.character(current_date)
  payload_distinct_team_home_games$processdate <- processdate # nolint
  payload_distinct_team_home_games$official_date <- current_date # nolint

  # Replace any non-alphanumeric character in columns with an underscore
  colnames(payload_distinct_team_home_games) <- gsub("[^[:alnum:]_]", "_", colnames(payload_distinct_team_home_games))  # nolint
  colnames(payload_distinct_team_home_games) <- tolower(colnames(payload_distinct_team_home_games)) # nolint


  pbp_data_table_name <- DBI::Id(schema = "raw", table = "MINORS_pbp_data")
  # Write to PostgreSQL table (append mode)
  dbWriteTable(
    con,
    name = pbp_data_table_name,
    value = payload_distinct_team_home_games,
    append = TRUE,
    row.names = FALSE
  )

  print(paste("finished PBP DATA iteration for:", current_date))

  batter_ids_df <- payload_distinct_team_home_games %>%
    distinct(matchup_batter_id, batting_team) %>%
    dplyr::rename("player_id" = "matchup_batter_id",
                  "team" = "batting_team")

  pitcher_ids_df <- payload_distinct_team_home_games %>%
    distinct(matchup_pitcher_id, fielding_team) %>%
    dplyr::rename("player_id" = "matchup_pitcher_id",
                  "team" = "fielding_team")

  players_list_df <- rbind(batter_ids_df, pitcher_ids_df)


  #Define columns that will be kept from api call
  mlb_people_column_names <- c(
    "id", "full_name", "link", "first_name", "last_name", "primary_number",
    "birth_date", "current_age", "birth_city", "birth_state_province", "birth_country", # nolint
    "height", "weight", "active", "use_name", "use_last_name", "middle_name",
    "boxscore_name", "gender", "is_player", "is_verified", "draft_year",
    "mlb_debut_date", "name_first_last", "name_slug", "first_last_name",
    "last_first_name", "last_init_name", "init_last_name", "full_fml_name",
    "full_lfm_name", "strike_zone_top", "strike_zone_bottom", "nick_name",
    "primary_position_code", "primary_position_name", "primary_position_type",
    "primary_position_abbreviation", "bat_side_code", "bat_side_description",
    "pitch_hand_code", "pitch_hand_description"
  )

  mlb_people_result_df <- mlb_people(person_ids  = players_list_df$player_id)

  mlb_people_result_df <- mlb_people_result_df[, mlb_people_column_names] # nolint

  mlb_people_result_df <- mlb_people_result_df %>%
    inner_join(select(players_list_df, player_id, team), by=c("id"="player_id")) # nolint

  mlb_people_result_df$official_date <- current_date

  mlb_people_result_df <- as.data.frame(mlb_people_result_df)

  people_data_table_name <- DBI::Id(schema = "raw", table = "MINORS_people_data") #nolint
  # Write to PostgreSQL table (append mode)
  dbWriteTable(
    con,
    name = people_data_table_name,
    value = mlb_people_result_df,
    append = TRUE,
    row.names = FALSE
  )

  print(paste("finished PEOPLE DATA iteration for:", current_date))


  sport_ids <- c(11, 12, 13, 14, 16)

  all_mlb_teams <- map_dfr(sport_ids, ~ mlb_teams(season = year, sport_ids = .x)) %>% #nolint
    distinct(team_id) %>%
    arrange(team_id)

  all_mlb_teams <- as.data.frame(all_mlb_teams)

  all_mlb_teams$official_date <- current_date

  team_data_table_name <- DBI::Id(schema = "raw", table = "MINORS_team_data")
  # Write to PostgreSQL table (append mode)
  dbWriteTable(
    con,
    name = team_data_table_name,
    value = all_mlb_teams,
    append = TRUE,
    row.names = FALSE
  )

  print(paste("finished TEAM DATA iteration for:", current_date))

}