import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('/home/workspace/airflow/Configs/ars.cfg')

IAM_ROLE_ARN = config['IAM_ROLE']['ARN'] # Extract the Amazon Resource Name from the IAM Role in the config file

# DROP TRANSFORM TABLES

teams_table_drop = "DROP TABLE IF EXISTS dimTeams CASCADE"
players_table_drop = "DROP TABLE IF EXISTS dimPlayers CASCADE"
league_table_drop = "DROP TABLE IF EXISTS dimleague CASCADE"
standings_table_drop = "DROP TABLE IF EXISTS dimStandings CASCADE"
fixtures_table_drop = "DROP TABLE IF EXISTS dimFixtures CASCADE"
rounds_table_drop = "DROP TABLE IF EXISTS dimRounds CASCADE"
squads_table_drop = "DROP TABLE IF EXISTS dimSquads CASCADE"
season_metrics_teams_table_drop = "DROP TABLE IF EXISTS factSeasonMetricsTeams CASCADE"
season_metrics_players_table_drop = "DROP TABLE IF EXISTS factSeasonMetricsPlayers CASCADE"
gw_metrics_teams_table_drop = "DROP TABLE IF EXISTS factGameweekMetricsTeams CASCADE"
gw_metrics_players_table_drop = "DROP TABLE IF EXISTS factGameweekMetricsPlayers CASCADE"
gw_lineups_players_table_drop = "DROP TABLE IF EXISTS dimGameweekLineupPlayers CASCADE"
gw_lineups_desc_table_drop = "DROP TABLE IF EXISTS dimGameweekLineupDesc CASCADE"
gw_fixture_events_table_drop = "DROP TABLE IF EXISTS factFixtureEvents CASCADE"

# Create transform tables

# Teams analytical table
teams_table_create = ("""
CREATE TABLE IF NOT EXISTS dimTeams
(
 team_id BIGINT PRIMARY KEY,
 team_country TEXT,
 team_founded_date DATE,
 is_national_team BOOL,
 team_logo TEXT,
 team_name TEXT,
 team_venue_address TEXT,
 team_venue_capacity BIGINT,
 team_venue_city TEXT,
 team_venue_name TEXT,
 team_venue_surface TEXT,
 league_name TEXT,
 league_id BIGINT REFERENCES dimleague(league_id),
 last_data_update TIMESTAMP
)
""")

# Players analytical table
players_table_create = ("""
CREATE TABLE IF NOT EXISTS dimPlayers
(
 player_id BIGINT PRIMARY KEY,
 player_age INT,
 player_birth_country TEXT,
 player_birth_place TEXT,
 player_firstname TEXT,
 player_height TEXT,
 player_lastname TEXT,
 player_nationality TEXT,
 player_current_squad_number INT,
 player_full_name TEXT,
 player_current_position TEXT,
 player_weight TEXT,
 player_current_team_id BIGINT REFERENCES dimTeams(team_id),
 Preferred_Foot TEXT,
 International_Reputation INT,
 last_data_update TIMESTAMP
)
""")

# League analytical table
league_table_create = ("""
CREATE TABLE IF NOT EXISTS dimLeague
(
  league_id BIGINT PRIMARY KEY,
  league_description TEXT,
  last_data_update TIMESTAMP
)
""")

# Standings analytical table
standings_table_create = ("""
CREATE TABLE IF NOT EXISTS dimStandings
(
  standing_key BIGINT IDENTITY(1,1) PRIMARY KEY,
  rank_status TEXT,
  form TEXT,
  lastUpdate_api TEXT,
  rank INT,
  team_id BIGINT REFERENCES dimTeams(team_id),
  league_id BIGINT REFERENCES dimleague(league_id),
  gameweek INT,
  last_data_update TIMESTAMP
)
""")

# Fixtures analytical table
fixtures_table_create = ("""
CREATE TABLE IF NOT EXISTS dimFixtures
(
 fixture_id BIGINT PRIMARY KEY,
 awayTeam_team_id BIGINT REFERENCES dimTeams(team_id),
 elapsed INT,
 event_date DATE,
 event_timestamp TIMESTAMP,
 firstHalfStart TIMESTAMP,
 homeTeam_team_id BIGINT REFERENCES dimTeams(team_id),
 league_id BIGINT REFERENCES dimleague(league_id),
 goalsAwayTeam INT,
 goalsHomeTeam INT,
 score_fulltime TEXT,
 score_halftime TEXT,
 secondHalfStart TIMESTAMP,
 referee TEXT,
 fixture_round TEXT,
 fixture_status TEXT,
 fixture_statusShort TEXT,
 fixture_venue TEXT,
 fixture_result TEXT,
 fixture_awayTeam_points INT,
 fixture_homeTeam_points INT,
 last_data_update TIMESTAMP
 )
""")

# Rounds analytical table
rounds_table_create= ("""
CREATE TABLE IF NOT EXISTS dimRounds
(
 gameweek_round_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 fixture_round_description TEXT,
 gameweek_start_date DATE,
 gameweek_end_date DATE,
 league_id BIGINT REFERENCES dimleague(league_id),
 last_data_update TIMESTAMP
 )
""")

# Squads analytical table
squads_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSquads
(
 squad_submit_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 player_squad_number INT,
 player_id BIGINT REFERENCES dimPlayers(player_id),
 team_id BIGINT REFERENCES dimTeams(team_id),
 league_id BIGINT REFERENCES dimleague(league_id),
 last_data_update TIMESTAMP
)
""")

# Team Season stats analytical table
season_metrics_teams_table_create = ("""
CREATE TABLE IF NOT EXISTS factSeasonMetricsTeams
(
 season_teams_metrics_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 team_id BIGINT REFERENCES dimTeams(team_id),
 league_id BIGINT REFERENCES dimleague(league_id),
 overall_draws INT,
 overall_goalsAgainst INT,
 overall_goalsFor INT,
 overall_lose INT,
 overall_matchesPlayed INT,
 overall_wins INT,
 away_draws INT,
 away_goalsAgainst INT,
 away_goalsFor INT,
 away_lose INT,
 away_matchesPlayed INT,
 away_wins INT,
 goalsDiff INT,
 home_draws INT,
 home_goalsAgainst INT,
 home_goalsFor INT,
 home_lose INT,
 home_matchesPlayed INT,
 home_wins INT,
 lastUpdate TEXT,
 points INT,
 last_data_update TIMESTAMP
)
""")

# Player Season Stats analytical table
season_metrics_players_table_create = ("""
CREATE TABLE IF NOT EXISTS factSeasonMetricsPlayers
(
 season_player_metrics_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 player_id BIGINT REFERENCES dimPlayers(player_id),
 team_id BIGINT REFERENCES dimTeams(team_id),
 league_id BIGINT REFERENCES dimleague(league_id),
 season_captain INT,
 season_cards_red INT,
 season_cards_yellow INT,
 season_cards_yellowred INT,
 season_dribbles_attempts INT,
 season_dribbles_success INT,
 season_duels_total INT,
 season_duels_won INT,
 season_fouls_committed INT,
 season_fouls_drawn INT,
 season_games_appearances INT,
 season_games_lineups INT,
 season_games_minutes_played INT,
 season_goals_assists INT,
 season_goals_conceded INT,
 season_goals_total INT,
 season_injured TEXT,
 season_passes_accuracy INT,
 season_passes_key INT,
 season_passes_total INT,
 season_penalty_committed INT,
 season_penalty_missed INT,
 season_penalty_saved INT,
 season_penalty_success INT,
 season_penalty_won INT,
 season_rating NUMERIC,
 season_shots_on INT,
 season_shots_total INT,
 season_substitutes_bench INT,
 season_substitutes_in INT,
 season_substitutes_out INT,
 season_tackles_blocks INT,
 season_tackles_interceptions INT,
 season_tackles_total INT,
 fifa_Overall INT,
 fifa_Potential INT,
 fifa_Value TEXT,
 fifa_Wage TEXT,
 fifa_Weak_Foot_rating INT,
 fifa_Skill_Moves_rating INT,
 Work_Rate TEXT,
 fifa_LS_rating INT,
 fifa_ST_rating INT,
 fifa_RS_rating INT,
 fifa_LW_rating INT,
 fifa_LF_rating INT,
 fifa_CF_rating INT,
 fifa_RF_rating INT,
 fifa_RW_rating INT,
 fifa_LAM_rating INT,
 fifa_CAM_rating INT,
 fifa_RAM_rating INT,
 fifa_LM_rating INT,
 fifa_LCM_rating INT,
 fifa_CM_rating INT,
 fifa_RCM_rating INT,
 fifa_RM_rating INT,
 fifa_LWB_rating INT,
 fifa_LDM_rating INT,
 fifa_CDM_rating INT,
 fifa_RDM_rating INT,
 fifa_RWB_rating INT,
 fifa_LB_rating INT,
 fifa_LCB_rating INT,
 fifa_CB_rating INT,
 fifa_RCB_rating INT,
 fifa_RB_rating INT,
 fifa_Crossing_rating INT,
 fifa_Finishing_rating INT,
 fifa_HeadingAccuracy_rating INT,
 fifa_ShortPassing_rating INT,
 fifa_Volleys_rating INT,
 fifa_Dribbling_rating INT,
 fifa_Curve_rating INT,
 fifa_FKAccuracy_rating INT,
 fifa_LongPassing_rating INT,
 fifa_BallControl_rating INT,
 fifa_Acceleration_rating INT,
 fifa_SprintSpeed_rating INT,
 fifa_Agility_rating INT,
 fifa_Reactions_rating INT,
 fifa_Balance_rating INT,
 fifa_ShotPower_rating INT,
 fifa_Jumping_rating INT,
 fifa_Stamina_rating INT,
 fifa_Strength_rating INT,
 fifa_LongShots_rating INT,
 fifa_Aggression_rating INT,
 fifa_Interceptions_rating INT,
 fifa_Positioning_rating INT,
 fifa_Vision_rating INT,
 fifa_Penalties_rating INT,
 fifa_Composure_rating INT,
 fifa_Marking_rating INT,
 fifa_StandingTackle_rating INT,
 fifa_SlidingTackle_rating INT,
 fifa_GKDiving_rating INT,
 fifa_GKHandling_rating INT,
 fifa_GKKicking_rating INT,
 fifa_GKPositioning_rating INT,
 fifa_GKReflexes_rating INT,
 fantasy_total_points INT,
 fantasy_creativity_rating NUMERIC,
 fantasy_influence_rating NUMERIC,
 fantasy_threat_rating NUMERIC,
 fantasy_bonus_rating INT,
 fantasy_bps_rating INT,
 fantasy_selected_by_percent NUMERIC,
 last_data_update TIMESTAMP
)
""")

# Team Gameweek Stats analytical table
gw_metrics_teams_table_create = ("""
CREATE TABLE IF NOT EXISTS factGameweekMetricsTeams
(
 fixture_gameweek_team_stat_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 fixture_id BIGINT REFERENCES dimFixtures(fixture_id),
 awayTeam_team_id BIGINT REFERENCES dimTeams(team_id),
 homeTeam_team_id BIGINT REFERENCES dimTeams(team_id),
 Ball_Possession_away TEXT,
 Ball_Possession_home TEXT,
 Blocked_Shots_away INT,
 Blocked_Shots_home INT,
 Corner_Kicks_away INT,
 Corner_Kicks_home INT,
 Fouls_away INT,
 Fouls_home INT,
 Goalkeeper_Saves_away INT,
 Goalkeeper_Saves_home INT,
 Offsides_away INT,
 Offsides_home INT,
 Passes_Percent_away TEXT,
 Passes_Percent_home TEXT,
 Passes_accurate_away INT,
 Passes_accurate_home INT,
 Red_Cards_away INT,
 Red_Cards_home INT,
 Shots_insidebox_away INT,
 Shots_insidebox_home INT,
 Shots_off_goal_away INT,
 Shots_off_goal_home INT,
 Shots_on_goal_away INT,
 Shots_on_goal_home INT,
 Shots_outsidebox_away INT,
 Shots_outsidebox_home INT,
 Total_Shots_away INT,
 Total_Shots_home INT,
 Total_Passes_away INT,
 Total_Passes_home INT,
 Yellow_Cards_away INT,
 Yellow_Cards_home INT,
 last_data_update TIMESTAMP
)
""")

# Player Gameweek Stats analytical table
gw_metrics_players_table_create = ("""
CREATE TABLE IF NOT EXISTS factGameweekMetricsPlayers
(
 fixture_gameweek_player_stat_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 player_id BIGINT REFERENCES dimPlayers(player_id),
 fixture_id BIGINT REFERENCES dimFixtures(fixture_id),
 team_id BIGINT REFERENCES dimTeams(team_id),
 player_fixture_captain BOOL,
 player_fixture_cards_red INT,
 player_fixture_cards_yellow INT,
 player_fixture_dribbles_attempts INT,
 player_fixture_dribbles_past INT,
 player_fixture_dribbles_success INT,
 player_fixture_duels_total INT,
 player_fixture_duels_won INT,
 player_fixture_event_id BIGINT,
 player_fixture_fouls_committed INT,
 player_fixture_fouls_drawn INT,
 player_fixture_goals_assists INT,
 player_fixture_goals_conceded INT,
 player_fixture_goals_total INT,
 player_fixture_minutes_played INT,
 player_fixture_offsides INT,
 player_fixture_passes_accuracy INT,
 player_fixture_passes_key INT,
 player_fixture_passes_total INT,
 player_fixture_penalty_committed INT,
 player_fixture_penalty_missed INT,
 player_fixture_penalty_saved INT,
 player_fixture_penalty_success INT,
 player_fixture_penalty_won INT,
 player_fixture_rating NUMERIC,
 player_fixture_shots_on INT,
 player_fixture_shots_total INT,
 player_fixture_substitute BOOL,
 player_fixture_tackles_blocks INT,
 player_fixture_tackles_interceptions INT,
 player_fixture_tackles_total INT,
 updateAt_api BIGINT,
 player_fixture_attempted_passes INT,
 player_fixture_big_chances_created INT,
 player_fixture_big_chances_missed INT,
 player_fixture_fantasy_bonus INT,
 player_fixture_fantasy_bps INT,
 player_fixture_clean_sheets INT,
 player_fixture_clearances_blocks_interceptions INT,
 player_fixture_completed_passes INT,
 player_fixture_fantasy_creativity_rating NUMERIC,
 player_fixture_errors_leading_to_goal INT,
 player_fixture_errors_leading_to_goal_attempt INT,
 player_fixture_fantasy_influence_rating NUMERIC,
 player_fixture_open_play_crosses INT,
 player_fixture_own_goals INT,
 player_fixture_recoveries INT,
 player_fixture_saves INT,
 player_fixture_selected BIGINT,
 player_fixture_tackled INT,
 player_fixture_target_missed INT,
 player_fixture_fantasy_threat_rating INT,
 player_fixture_fantasy_total_points INT,
 player_fixture_fantasy_transfers_in BIGINT,
 player_fixture_fantasy_transfers_out BIGINT,
 player_fixture_fantasy_value INT,
 last_data_update TIMESTAMP
)
""")

# Gameweek Lineup Descriptions analytical table
gw_lineups_desc_table_create = ("""
CREATE TABLE IF NOT EXISTS dimGameweekLineupDesc
(
 fixture_lineup_desc_id INT IDENTITY(1,1) PRIMARY KEY,
 fixture_id BIGINT REFERENCES dimFixtures(fixture_id),
 home_team_name TEXT,
 home_team_formation TEXT,
 home_team_coach_id BIGINT,
 home_team_coach TEXT,
 away_team_name TEXT,
 away_team_formation TEXT,
 away_team_coach_id BIGINT,
 away_team_coach TEXT,
 last_data_update TIMESTAMP
 )
""")

# Gameweek Lineup Players analytical table
gw_lineups_players_table_create = ("""
CREATE TABLE IF NOT EXISTS dimGameweekLineupPlayers
(
 fixture_lineup_id INT IDENTITY(1,1) PRIMARY KEY,
 fixture_id BIGINT REFERENCES dimFixtures(fixture_id),
 player_id BIGINT REFERENCES dimPlayers(player_id),
 player_number INT,
 player_pos TEXT,
 player_team_id BIGINT REFERENCES dimTeams(team_id),
 lineup_type TEXT,
 last_data_update TIMESTAMP
 )
""")

# Gameweek Fixture Events analytical table
gw_fixture_events_table_create = ("""
CREATE TABLE IF NOT EXISTS factFixtureEvents
(
 event_id INT IDENTITY(1,1) PRIMARY KEY,
 fixture_id BIGINT REFERENCES dimFixtures(fixture_id),
 player_id BIGINT REFERENCES dimPlayers(player_id),
 team_id BIGINT REFERENCES dimTeams(team_id),
 assist_player_id BIGINT REFERENCES dimPlayers(player_id),
 event_detail TEXT,
 event_match_minute INT,
 event_match_minute_plus INT,
 event_timestamp TIMESTAMP,
 fixture_timestamp TIMESTAMP,
 event_type TEXT,
 last_data_update TIMESTAMP
)
""")

# QUERY LISTS

# Create transform tables in redshift
create_transform_table_queries = [league_table_create, teams_table_create, players_table_create, fixtures_table_create, standings_table_create, rounds_table_create, squads_table_create, season_metrics_teams_table_create, season_metrics_players_table_create, gw_metrics_teams_table_create, gw_metrics_players_table_create, gw_lineups_desc_table_create, gw_lineups_players_table_create, gw_fixture_events_table_create]

# Create the SQL statements to drop any existing staging tables to allow overwriting
drop_transform_table_queries = [league_table_drop, teams_table_drop, players_table_drop, fixtures_table_drop, standings_table_drop, rounds_table_drop, squads_table_drop, season_metrics_teams_table_drop, season_metrics_players_table_drop, gw_metrics_teams_table_drop, gw_metrics_players_table_drop, gw_lineups_desc_table_drop, gw_lineups_players_table_drop, gw_fixture_events_table_drop]