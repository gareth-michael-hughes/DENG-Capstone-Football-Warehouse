import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('/home/workspace/airflow/Configs/ars.cfg')

# Set out credentials and bucket paths for copy from S3 to redshift method
IAM_ROLE_ARN = config['IAM_ROLE']['ARN'] # Extract the Amazon Resource Name from the IAM Role in the config file
REGION = config['REGION']['REG']

# S3 bucket object names
FANTASY_GW_DATA = config['S3']["FANTASY_GW_DATA"]
FANTASY_OVERALL_DATA = config['S3']["FANTASY_OVERALL_DATA"]
FIFA_RATINGS_DATA = config['S3']["FIFA_RATINGS_DATA"]
FIXTURE_EVENTS_DATA = config['S3']["FIXTURE_EVENTS_DATA"]
LINEUP_DESCRIPTIVE_DATA = config['S3']["LINEUP_DESCRIPTIVE_DATA"]
LINEUP_PLAYER_DATA = config['S3']["LINEUP_PLAYER_DATA"]
FIXTURE_LIST_DATA = config['S3']["FIXTURE_LIST_DATA"]
FIXTURE_STATS_DATA = config['S3']["FIXTURE_STATS_DATA"]
PLAYER_FIXTURE_STATS_DATA = config['S3']["PLAYER_FIXTURE_STATS_DATA"]
PLAYER_STATS_DATA = config['S3']["PLAYER_STATS_DATA"]
ROUNDS_DATA = config['S3']["ROUNDS_DATA"]
STANDINGS_DATA = config['S3']["STANDINGS_DATA"]
TEAM_LIST_DATA = config['S3']["TEAM_LIST_DATA"]
TEAM_SQUADS_DATA = config['S3']["TEAM_SQUADS_DATA"]
PLAYER_MAP_DATA = config['S3']["PLAYER_MAP_DATA"]

# S3 bucket json path file locations for formatting in copy to redshift
TEAM_LIST_PATH = config['JSON_FILE_PATH']['TEAM_LIST']
STANDINGS_PATH = config['JSON_FILE_PATH']['STANDINGS']
FIXTURE_LIST_PATH = config['JSON_FILE_PATH']['FIXTURE_LIST']
ROUNDS_PATH = config['JSON_FILE_PATH']['ROUNDS']
FIXTURE_EVENTS_PATH = config['JSON_FILE_PATH']['FIXTURE_EVENTS']
LINEUP_DESC_PATH = config['JSON_FILE_PATH']['LINEUP_DESC']
LINEUP_PLAYER_PATH = config['JSON_FILE_PATH']['LINEUP_PLAYER']
PLAYER_STATS_PATH = config['JSON_FILE_PATH']['PLAYER_STATS']
FIXTURE_STATS_PATH = config['JSON_FILE_PATH']['FIXTURE_STATS']
PLAYER_FIXTURE_STATS_PATH = config['JSON_FILE_PATH']['PLAYER_FIXTURE_STATS']
TEAM_SQUADS_PATH = config['JSON_FILE_PATH']['TEAM_SQUADS']

# DROP STAGING TABLES

staging_teams_table_drop = "DROP TABLE IF EXISTS staging_teams"
staging_standings_table_drop = "DROP TABLE IF EXISTS staging_standings"
staging_fixtures_table_drop = "DROP TABLE IF EXISTS staging_fixtures"
staging_rounds_table_drop = "DROP TABLE IF EXISTS staging_rounds"
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_lineups_descriptive_table_drop = "DROP TABLE IF EXISTS staging_lineups_descriptive"
staging_lineups_players_table_drop = "DROP TABLE IF EXISTS staging_lineups_players"
staging_player_stats_table_drop = "DROP TABLE IF EXISTS staging_player_stats"
staging_fixture_stats_table_drop = "DROP TABLE IF EXISTS staging_fixture_stats"
staging_fixture_player_stats_table_drop = "DROP TABLE IF EXISTS staging_fixture_player_stats"
staging_team_squads_table_drop = "DROP TABLE IF EXISTS staging_team_squads"
staging_player_fifa_ratings_table_drop = "DROP TABLE IF EXISTS staging_player_fifa_ratings"
staging_player_fantasy_stats_overall_table_drop = "DROP TABLE IF EXISTS staging_player_fantasy_stats_overall"
staging_player_fantasy_stats_gw_table_drop = "DROP TABLE IF EXISTS staging_player_fantasy_stats_gw"
staging_player_map_table_drop = "DROP TABLE IF EXISTS staging_player_map_table"

# CREATE STAGING TABLES

# teams
staging_teams_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_teams
(
 code TEXT,
 country TEXT,
 founded INT,
 is_national BOOL,
 logo TEXT,
 name TEXT,
 team_id BIGINT,
 venue_address TEXT,
 venue_capacity BIGINT,
 venue_city TEXT,
 venue_name TEXT,
 venue_surface TEXT
 )
""")

# standings
staging_standings_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_standings
(
 all_draw INT,
 all_goalsAgainst INT,
 all_goalsFor INT,
 all_lose INT,
 all_matchsPlayed INT,
 all_win INT,
 away_draw INT,
 away_goalsAgainst INT,
 away_goalsFor INT,
 away_lose INT,
 away_matchsPlayed INT,
 away_win INT,
 description TEXT,
 forme TEXT,
 goalsDiff INT,
 group_name TEXT,
 home_draw INT,
 home_goalsAgainst INT,
 home_goalsFor INT,
 home_lose INT,
 home_matchsPlayed INT,
 home_win INT,
 lastUpdate TEXT,
 logo TEXT,
 points INT,
 rank INT,
 status TEXT,
 teamName TEXT,
 team_id BIGINT
 )
""")

# fixtures
staging_fixtures_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_fixtures
(
 awayTeam_logo TEXT,
 awayTeam_team_id BIGINT,
 awayTeam_team_name TEXT,
 elapsed INT,
 event_date TEXT,
 event_timestamp BIGINT,
 firstHalfStart BIGINT,
 fixture_id BIGINT,
 goalsAwayTeam INT,
 goalsHomeTeam INT,
 homeTeam_logo TEXT,
 homeTeam_team_id BIGINT,
 homeTeam_team_name TEXT,
 league_country TEXT,
 league_flag TEXT,
 league_id BIGINT,
 league_logo TEXT,
 league_name TEXT,
 referee TEXT,
 round TEXT,
 score_extratime TEXT,
 score_fulltime TEXT,
 score_halftime TEXT,
 score_penalty TEXT,
 secondHalfStart BIGINT,
 status TEXT,
 statusShort TEXT,
 venue TEXT
 )
""")

# Rounds
staging_rounds_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_rounds
(
 fixtures TEXT
 )
""")

# events
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
 assist TEXT,
 assist_id BIGINT,
 comments TEXT,
 detail TEXT,
 elapsed INT,
 elapsed_plus INT,
 player TEXT,
 player_id BIGINT,
 teamName TEXT,
 team_id BIGINT,
 type TEXT,
 fixture_id BIGINT
 )
""")

# lineups descriptive
staging_lineups_descriptive_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_lineups_descriptive
(
 team_name TEXT,
 formation TEXT,
 coach_id BIGINT,
 coach TEXT,
 fixture_id BIGINT
 )
""")

# lineups players
staging_lineups_players_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_lineups_players
(
 number INT,
 player TEXT,
 player_id BIGINT,
 pos CHAR,
 team_id BIGINT,
 type TEXT,
 fixture_id BIGINT
 )
""")

# player stats
staging_player_stats_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_player_stats
(
 age INT,
 birth_country TEXT,
 birth_date TEXT,
 birth_place TEXT,
 captain INT,
 cards_red INT,
 cards_yellow INT,
 cards_yellowred INT,
 dribbles_attempts INT,
 dribbles_success INT,
 duels_total INT,
 duels_won INT,
 firstname TEXT,
 fouls_committed INT,
 fouls_drawn INT,
 games_appearances INT,
 games_lineups INT,
 games_minutes_played INT,
 goals_assists INT,
 goals_conceded INT,
 goals_total INT,
 height TEXT,
 injured TEXT,
 lastname TEXT,
 league TEXT,
 nationality TEXT,
 number TEXT,
 passes_accuracy INT,
 passes_key INT,
 passes_total INT,
 penalty_committed INT,
 penalty_missed INT,
 penalty_saved INT,
 penalty_success INT,
 penalty_won INT,
 player_id BIGINT,
 player_name TEXT,
 position TEXT,
 rating NUMERIC,
 season TEXT,
 shots_on INT,
 shots_total INT,
 substitutes_bench INT,
 substitutes_in INT,
 substitutes_out INT,
 tackles_blocks INT,
 tackles_interceptions INT,
 tackles_total INT,
 team_id BIGINT,
 team_name TEXT,
 weight TEXT
 )
""")

# fixture stats"
staging_fixture_stats_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_fixture_stats
(
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
 fixture_id BIGINT
 )
""")

# fixture player stats"
staging_fixture_player_stats_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_fixture_player_stats
(
 captain BOOL,
 cards_red INT,
 cards_yellow INT,
 dribbles_attempts INT,
 dribbles_past INT,
 dribbles_success INT,
 duels_total INT,
 duels_won INT,
 event_id BIGINT,
 fouls_committed INT,
 fous_drawn INT,
 goals_assists INT,
 goals_conceded INT,
 goals_total INT,
 minutes_played INT,
 number INT,
 offsides INT,
 passes_accuracy INT,
 passes_key INT,
 passes_total INT,
 penalty_committed INT,
 penalty_missed INT,
 penalty_saved INT,
 penalty_success INT,
 penalty_won INT,
 player_id BIGINT,
 player_name TEXT,
 position CHAR,
 rating TEXT,
 shots_on INT,
 shots_total INT,
 substitute BOOL,
 tackles_blocks INT,
 tackles_interceptions INT,
 tackles_total INT,
 team_id BIGINT,
 team_name TEXT,
 updateAt BIGINT,
 fixture_id BIGINT
 )
""")

# team squads
staging_team_squads_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_team_squads
(
 age INT,
 birth_country TEXT,
 birth_date TEXT,
 birth_place TEXT,
 firstname TEXT,
 height TEXT, 
 lastname TEXT,
 nationality TEXT,
 number TEXT,
 player_id BIGINT,
 player_name TEXT,
 position TEXT,
 weight TEXT,
 team_id BIGINT
 )
""")

# fifa player ratings
staging_player_fifa_ratings_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_player_fifa_ratings
(
 row_number BIGINT,
 Player_id BIGINT,
 Name TEXT,
 Age INT,
 Photo TEXT,
 Nationality TEXT,
 Flag TEXT,
 Overall INT,
 Potential INT,
 Club TEXT,
 Club_logo TEXT,
 Value TEXT,
 Wage TEXT,
 Special TEXT,
 Preferred_Foot TEXT,
 Internation_Reputation TEXT,
 Weak_Foot TEXT,
 Skill_Moves TEXT,
 Work_Rate TEXT,
 Body_Type TEXT,
 Real_Face TEXT,
 Position TEXT,
 Jersey_Number TEXT,
 Joined_Club TEXT,
 Loaned_From_Club TEXT,
 Contract_Valid_Until TEXT,
 Height TEXT,
 Weight TEXT,
 LS TEXT,
 ST TEXT,
 RS TEXT,
 LW TEXT,
 LF TEXT,
 CF TEXT,
 RF TEXT,
 RW TEXT,
 LAM TEXT,
 CAM TEXT,
 RAM TEXT,
 LM TEXT,
 LCM TEXT,
 CM TEXT,
 RCM TEXT,
 RM TEXT,
 LWB TEXT,
 LDM TEXT,
 CDM TEXT,
 RDM TEXT,
 RWB TEXT,
 LB TEXT,
 LCB TEXT,
 CB TEXT,
 RCB TEXT,
 RB TEXT,
 Crossing TEXT,
 Finishing TEXT,
 HeadingAccuracy TEXT,
 ShortPassing TEXT,
 Volleys TEXT,
 Dribbling TEXT,
 Curve TEXT,
 FKAccuracy TEXT,
 LongPassing TEXT,
 BallControl TEXT,
 Acceleration TEXT,
 SprintSpeed TEXT,
 Agility TEXT,
 Reactions TEXT,
 Balance TEXT,
 ShotPower TEXT,
 Jumping TEXT,
 Stamina TEXT,
 Strength TEXT,
 LongShots TEXT,
 Aggression TEXT,
 Interceptions TEXT,
 Positioning TEXT,
 Vision TEXT,
 Penalties TEXT,
 Composure TEXT,
 Marking TEXT,
 StandingTackle TEXT,
 SlidingTackle TEXT,
 GKDiving TEXT,
 GKHandling TEXT,
 GKKicking TEXT,
 GKPositioning TEXT,
 GKReflexes TEXT,
 Release_Clause TEXT
 )
""")

# player fantasy stats overall
staging_player_fantasy_stats_overall_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_player_fantasy_stats_overall
(
 first_name TEXT,
 second_name TEXT,
 goals_scored NUMERIC,
 assists NUMERIC,
 total_points NUMERIC,
 minutes BIGINT,
 goals_conceded NUMERIC,
 creativity NUMERIC,
 influence NUMERIC,
 threat NUMERIC,
 bonus NUMERIC,
 bps NUMERIC,
 ict_index NUMERIC,
 clean_sheets NUMERIC,
 red_cards NUMERIC,
 yellow_cards NUMERIC,
 selected_by_percent NUMERIC,
 now_cost NUMERIC,
 player_id BIGINT
 )
""")

# player fantasy stats by gameweek
staging_player_fantasy_stats_gw_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_player_fantasy_stats_gw
(
 name TEXT,
 assists TEXT,
 attempted_passes TEXT,
 big_chances_created TEXT,
 big_chances_missed TEXT,
 bonus TEXT,
 bps TEXT,
 clean_sheets TEXT,
 clearances_blocks_interceptions TEXT,
 completed_passes TEXT,
 creativity TEXT,
 dribbles TEXT,
 ea_index TEXT,
 element TEXt,
 errors_leading_to_goal TEXT,
 errors_leading_to_goal_attempt TEXT,
 fixture TEXT,
 fouls TEXT,
 goals_conceded TEXT,
 goals_scored TEXT,
 ict_index TEXT,
 id BIGINT,
 influence TEXT,
 key_passes TEXT,
 kickoff_time TEXT,
 kickoff_time_formatted TEXT,
 loaned_in TEXT,
 loaned_out TEXT,
 minutes TEXT,
 offside TEXT,
 open_play_crosses TEXT,
 opponent_team TEXT, 
 own_goals TEXT,
 penalties_conceded TEXT,
 penalties_missed TEXT,
 penalties_saved TEXT,
 recoveries TEXT,
 red_cards TEXT,
 round TEXT,
 saves TEXT,
 selected BIGINT,
 tackled TEXT,
 tackles TEXT,
 target_missed TEXT,
 team_a_score TEXT,
 team_h_score TEXT,
 threat TEXT,
 total_points TEXT,
 transfers_balance TEXT,
 transfers_in TEXT,
 transfers_out TEXT,
 value TEXT,
 was_home TEXT,
 winning_goals TEXT,
 yellow_cards TEXT
 )
""")

# player map table
staging_player_map_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_player_map_table
(
 player_original_id BIGINT,
 fifa_id BIGINT,
 fantasy_id BIGINT
 )
""")


# LOAD TO STAGING TABLES

# copy teams
staging_teams_table_copy = ("""
    copy staging_teams from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
    """).format(TEAM_LIST_DATA, IAM_ROLE_ARN, REGION, TEAM_LIST_PATH)

# copy standings
staging_standings_table_copy = ("""
    copy staging_standings from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(STANDINGS_DATA, IAM_ROLE_ARN, REGION, STANDINGS_PATH)

# copy fixtures
staging_fixtures_table_copy = ("""
    copy staging_fixtures from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(FIXTURE_LIST_DATA, IAM_ROLE_ARN, REGION, FIXTURE_LIST_PATH)

# copy rounds
staging_rounds_table_copy = ("""
    copy staging_rounds from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(ROUNDS_DATA, IAM_ROLE_ARN, REGION, ROUNDS_PATH)

# copy events
staging_events_table_copy = ("""
    copy staging_events from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(FIXTURE_EVENTS_DATA, IAM_ROLE_ARN, REGION, FIXTURE_EVENTS_PATH)

# copy lineups descriptive
staging_lineups_descriptive_table_copy = ("""
    copy staging_lineups_descriptive from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(LINEUP_DESCRIPTIVE_DATA, IAM_ROLE_ARN, REGION, LINEUP_DESC_PATH)

# copy lineups players
staging_lineups_players_table_copy = ("""
    copy staging_lineups_players from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(LINEUP_PLAYER_DATA, IAM_ROLE_ARN, REGION, LINEUP_PLAYER_PATH)

# copy player stats
staging_player_stats_table_copy = ("""
    copy staging_player_stats from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(PLAYER_STATS_DATA, IAM_ROLE_ARN, REGION, PLAYER_STATS_PATH)

# copy fixture stats
staging_fixture_stats_table_copy = ("""
    copy staging_fixture_stats from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(FIXTURE_STATS_DATA, IAM_ROLE_ARN, REGION, FIXTURE_STATS_PATH)

# copy fixture player stats
staging_fixture_player_stats_table_copy = ("""
    copy staging_fixture_player_stats from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(PLAYER_FIXTURE_STATS_DATA, IAM_ROLE_ARN, REGION, PLAYER_FIXTURE_STATS_PATH)

# copy team squads
staging_team_squads_table_copy = ("""
    copy staging_team_squads from '{}'
    iam_role {}
    compupdate off
    REGION {}
    FORMAT AS JSON {} truncatecolumns;
""").format(TEAM_SQUADS_DATA, IAM_ROLE_ARN, REGION, TEAM_SQUADS_PATH)

# copy fifa ratings
staging_player_fifa_ratings_table_copy = ("""
    copy staging_player_fifa_ratings from '{}'
    iam_role {}
    compupdate off
    REGION {}
    EMPTYASNULL
    IGNOREHEADER 1
    ACCEPTINVCHARS
    csv;
""").format(FIFA_RATINGS_DATA, IAM_ROLE_ARN, REGION)

# copy fantasy overall stats
staging_player_fantasy_stats_overall_table_copy = ("""
    copy staging_player_fantasy_stats_overall from '{}'
    iam_role {}
    compupdate off
    REGION {}
    EMPTYASNULL
    IGNOREHEADER 1
    ACCEPTINVCHARS
    csv;
""").format(FANTASY_OVERALL_DATA, IAM_ROLE_ARN, REGION)

# copy fantasy gameweek stats
staging_player_fantasy_stats_gw_table_copy = ("""
    copy staging_player_fantasy_stats_gw from '{}'
    iam_role {}
    compupdate off
    REGION {}
    EMPTYASNULL
    IGNOREHEADER 1
    ACCEPTINVCHARS
    csv;
""").format(FANTASY_GW_DATA, IAM_ROLE_ARN, REGION)


# copy fantasy gameweek stats
staging_player_map_table_copy = ("""
    copy staging_player_map_table from '{}'
    iam_role {}
    compupdate off
    REGION {}
    EMPTYASNULL
    IGNOREHEADER 1
    ACCEPTINVCHARS
    csv;
""").format(PLAYER_MAP_DATA, IAM_ROLE_ARN, REGION)


# QUERY LISTS

# Create staging tables in redshift
create_stage_table_queries = [staging_teams_table_create, staging_standings_table_create, staging_fixtures_table_create, staging_rounds_table_create, staging_events_table_create, staging_lineups_descriptive_table_create, staging_lineups_players_table_create, staging_player_stats_table_create, staging_fixture_stats_table_create, staging_fixture_player_stats_table_create, staging_team_squads_table_create, staging_player_fifa_ratings_table_create, staging_player_fantasy_stats_overall_table_create, staging_player_fantasy_stats_gw_table_create, staging_player_map_table_create]

# Create the SQL statements to drop any existing staging tables to allow overwriting
drop_stage_table_queries = [staging_teams_table_drop, staging_standings_table_drop, staging_fixtures_table_drop, staging_rounds_table_drop, staging_events_table_drop, staging_lineups_descriptive_table_drop, staging_lineups_players_table_drop, staging_player_stats_table_drop, staging_fixture_stats_table_drop, staging_fixture_player_stats_table_drop, staging_team_squads_table_drop, staging_player_fifa_ratings_table_drop, staging_player_fantasy_stats_overall_table_drop, staging_player_fantasy_stats_gw_table_drop, staging_player_map_table_drop] 

# Create the SQL statements to copy records from files in S3 into Redshift tables
copy_table_queries = [staging_teams_table_copy, staging_standings_table_copy, staging_fixtures_table_copy, staging_rounds_table_copy, staging_events_table_copy, staging_lineups_descriptive_table_copy, staging_lineups_players_table_copy, staging_player_stats_table_copy, staging_fixture_stats_table_copy, staging_fixture_player_stats_table_copy, staging_team_squads_table_copy, staging_player_fifa_ratings_table_copy, staging_player_fantasy_stats_overall_table_copy, staging_player_fantasy_stats_gw_table_copy, staging_player_map_table_copy]