from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LocalResetOperator
from airflow.operators import BucketResetOperator
from data_quality import DataQualityOperator
from datetime import datetime, timedelta
import os
import configparser
from helpers import SqlQueries

config = configparser.ConfigParser()
config.read('/home/workspace/airflow/Configs/ars.cfg')


# S3 bucket object names
FANTASY_GW_DATA = config['S3']["FANTASY_GW_DATA"][(23-len(config['S3']["FANTASY_GW_DATA"])):]
FANTASY_OVERALL_DATA = config['S3']["FANTASY_OVERALL_DATA"][(23-len(config['S3']["FANTASY_OVERALL_DATA"])):]
FIFA_RATINGS_DATA = config['S3']["FIFA_RATINGS_DATA"][(23-len(config['S3']["FIFA_RATINGS_DATA"])):]
FIXTURE_EVENTS_DATA = config['S3']["FIXTURE_EVENTS_DATA"][(23-len(config['S3']["FIXTURE_EVENTS_DATA"])):]
LINEUP_DESCRIPTIVE_DATA = config['S3']["LINEUP_DESCRIPTIVE_DATA"][(23-len(config['S3']["LINEUP_DESCRIPTIVE_DATA"])):]
LINEUP_PLAYER_DATA = config['S3']["LINEUP_PLAYER_DATA"][(23-len(config['S3']["LINEUP_PLAYER_DATA"])):]
FIXTURE_LIST_DATA = config['S3']["FIXTURE_LIST_DATA"][(23-len(config['S3']["FIXTURE_LIST_DATA"])):]
FIXTURE_STATS_DATA = config['S3']["FIXTURE_STATS_DATA"][(23-len(config['S3']["FIXTURE_STATS_DATA"])):]
PLAYER_FIXTURE_STATS_DATA = config['S3']["PLAYER_FIXTURE_STATS_DATA"][(23-len(config['S3']["PLAYER_FIXTURE_STATS_DATA"])):]
PLAYER_STATS_DATA = config['S3']["PLAYER_STATS_DATA"][(23-len(config['S3']["PLAYER_STATS_DATA"])):]
ROUNDS_DATA = config['S3']["ROUNDS_DATA"][(23-len(config['S3']["ROUNDS_DATA"])):]
STANDINGS_DATA = config['S3']["STANDINGS_DATA"][(23-len(config['S3']["STANDINGS_DATA"])):]
TEAM_LIST_DATA = config['S3']["TEAM_LIST_DATA"][(23-len(config['S3']["TEAM_LIST_DATA"])):]
TEAM_SQUADS_DATA = config['S3']["TEAM_SQUADS_DATA"][(23-len(config['S3']["TEAM_SQUADS_DATA"])):]
PLAYER_MAP_DATA = config['S3']["PLAYER_MAP_DATA"][(23-len(config['S3']["PLAYER_MAP_DATA"])):]
home = '/home/workspace/airflow/Data'

FIXTURE_EVENTS_FOLDER = home+config['LOCAL']["FIXTURE_EVENTS_FOLDER"]
LINEUP_DESCRIPTIVE_FOLDER = home+config['LOCAL']["LINEUP_DESCRIPTIVE_FOLDER"]
LINEUP_PLAYER_FOLDER = home+config['LOCAL']["LINEUP_PLAYER_FOLDER"]
FIXTURE_LIST_FOLDER = home+config['LOCAL']["FIXTURE_LIST_FOLDER"]
FIXTURE_STATS_FOLDER = home+config['LOCAL']["FIXTURE_STATS_FOLDER"]
PLAYER_FIXTURE_STATS_FOLDER = home+config['LOCAL']["PLAYER_FIXTURE_STATS_FOLDER"]
PLAYER_STATS_FOLDER = home+config['LOCAL']["PLAYER_STATS_FOLDER"]
ROUNDS_FOLDER = home+config['LOCAL']["ROUNDS_FOLDER"]
STANDINGS_FOLDER = home+config['LOCAL']["STANDINGS_FOLDER"]
TEAM_LIST_FOLDER = home+config['LOCAL']["TEAM_LIST_FOLDER"]
TEAM_SQUADS_FOLDER = home+config['LOCAL']["TEAM_SQUADS_FOLDER"]

my_bucket= config['S3']['DESTINATION_BUCKET']
# Default settings for DAG
default_args = {
    'owner': 'Gareth',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retries': 1,
    'catchup': False,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,    
}

## Define the DAG object
dag = DAG(dag_id='fb_dwh_dag', 
          default_args=default_args,
          description='Load data from s3 to Redshift and transform with Spark via Airflow',
          schedule_interval=None
         )

local_dir_list = [FIXTURE_EVENTS_FOLDER, LINEUP_DESCRIPTIVE_FOLDER, \
                 LINEUP_PLAYER_FOLDER, FIXTURE_LIST_FOLDER, FIXTURE_STATS_FOLDER, PLAYER_FIXTURE_STATS_FOLDER, \
                 PLAYER_STATS_FOLDER, ROUNDS_FOLDER, STANDINGS_FOLDER, TEAM_LIST_FOLDER, TEAM_SQUADS_FOLDER]

s3_bucket_list = [FANTASY_GW_DATA, FANTASY_OVERALL_DATA, FIFA_RATINGS_DATA, FIXTURE_EVENTS_DATA, LINEUP_DESCRIPTIVE_DATA, \
                 LINEUP_PLAYER_DATA, FIXTURE_LIST_DATA, FIXTURE_STATS_DATA, PLAYER_FIXTURE_STATS_DATA, \
                 PLAYER_STATS_DATA, ROUNDS_DATA, STANDINGS_DATA, TEAM_LIST_DATA, TEAM_SQUADS_DATA, PLAYER_MAP_DATA]
    
start_operator = DummyOperator(task_id='Begin-execution', dag=dag)
start_operator.doc = "Begins the boundary and execution of the fb_dwh dag"


reset_local_workspace = LocalResetOperator(task_id='reset_local_workspace',
                                           dir_list=local_dir_list,
                                           dag=dag)

reset_s3_bucket = BucketResetOperator(task_id='reset_s3_bucket',
                                      aws_credentials_id="aws_credentials",
                                      s3_bucket=my_bucket,
                                      dir_list=s3_bucket_list,
                                      dag=dag)

# Ensure Spark has access to redshift driver and execute permissions
# When using bash command be sure to leave an extra ' ' after raw command if not using a template
set_execution_permissions = BashOperator(task_id='set_execution_permissions',
                                    bash_command= 'cd /home/workspace chmod +x execute_set_up.sh ./execute_set_up.sh ',
                                    dag=dag)


# Load fantasy_ratings_gw_upload to s3
# params['python_script'] = 'fantasy_ratings_gw_upload.py'
fantasy_ratings_gw_upload = BashOperator(task_id='fantasy_ratings_gw_upload',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fantasy_ratings_gw_upload.py keep ',
                                    dag=dag)
    
# Load fantasy_ratings_overall_upload to s3
# params['python_script'] = 'fantasy_ratings_overall_upload.py'
fantasy_ratings_overall_upload = BashOperator(task_id='fantasy_ratings_overall_upload',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fantasy_ratings_overall_upload.py keep ',
                                    dag=dag)

# Load fifa_ratings_upload to s3
# params['python_script'] = 'fifa_ratings_upload.py'
fifa_ratings_upload = BashOperator(task_id='fifa_ratings_upload',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fifa_ratings_upload.py keep ',
                                    dag=dag)

# Load fixture_events_api_extract to s3
# params['python_script'] = 'fixture_events_api_extract.py'
fixture_events_api_extract = BashOperator(task_id='fixture_events_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fixture_events_api_extract.py prod wipe ',
                                    dag=dag)

# Load fixture_stats_api_extract to s3
# params['python_script'] = 'fixture_stats_api_extract.py'
fixture_stats_api_extract = BashOperator(task_id='fixture_stats_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fixture_stats_api_extract.py prod wipe ',
                                    dag=dag)

# Load fixtures_api_extract to s3
# params['python_script'] = 'fixtures_api_extract.py'
fixtures_api_extract = BashOperator(task_id='fixtures_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/fixtures_api_extract.py wipe ',
                                    dag=dag)

# Load json_filepath_upload to s3
# params['python_script'] = 'json_filepath_upload.py'
json_filepath_upload = BashOperator(task_id='json_filepath_upload',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/json_filepath_upload.py ',
                                    dag=dag)

# Load lineups_desc_api_extract to s3
# params['python_script'] = 'lineups_desc_api_extract.py'
lineups_desc_api_extract = BashOperator(task_id='lineups_desc_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/lineups_desc_api_extract.py prod wipe ',
                                    dag=dag)

# Load lineups_players_api_extract to s3
# params['python_script'] = 'lineups_players_api_extract.py'
lineups_players_api_extract = BashOperator(task_id='lineups_players_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/lineups_players_api_extract.py prod wipe ',
                                    dag=dag)

# Load player_fixture_stats_api_extract to s3
# params['python_script'] = 'player_fixture_stats_api_extract.py'
player_fixture_stats_api_extract = BashOperator(task_id='player_fixture_stats_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/player_fixture_stats_api_extract.py prod wipe ',
                                    dag=dag)

# Load player_stats_api_extract to s3
# params['python_script'] = 'player_stats_api_extract.py'
player_stats_api_extract = BashOperator(task_id='player_stats_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/player_stats_api_extract.py prod wipe ',
                                    dag=dag)

# Load rounds_api_extract to s3
# params['python_script'] = 'rounds_api_extract.py'
rounds_api_extract = BashOperator(task_id='rounds_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/rounds_api_extract.py wipe ',
                                    dag=dag)

# Load squads_api_extract to s3
# params['python_script'] = 'squads_api_extract.py'
squads_api_extract = BashOperator(task_id='squads_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/squads_api_extract.py prod wipe ',
                                    dag=dag)

# Load standings_api_extract to s3
# params['python_script'] = 'standings_api_extract.py'
standings_api_extract = BashOperator(task_id='standings_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/standings_api_extract.py wipe',
                                    dag=dag)

# Load teams_api_extract to s3
# params['python_script'] = 'teams_api_extract.py'
teams_api_extract = BashOperator(task_id='teams_api_extract',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/teams_api_extract.py wipe ',
                                    dag=dag)

# Load player mapping table to s3
# params['python_script'] = 'player_map_upload.py'
player_map_upload = BashOperator(task_id='player_map_upload',
                                    bash_command= 'python3 /home/workspace/airflow/Downloading/player_map_upload.py keep ',
                                    dag=dag)

# creates and loads staging tables in redshift
# params['python_script'] = 'create_db_skeleton_redshift.py'
create_db_skeleton_redshift = BashOperator(task_id='create_db_skeleton_redshift',
                                    bash_command= 'python3 /home/workspace/airflow/Staging/create_db_skeleton_redshift.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_fixture_events.sh'
spark_sub_fixtures = BashOperator(task_id='spark_sub_fixtures',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/fixtures_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_fixtures.sh'
spark_sub_fixture_events = BashOperator(task_id='spark_sub_fixture_events',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/fixtureEvents_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_gwlineupdesc.sh'
spark_sub_gwlineupdesc = BashOperator(task_id='spark_sub_gwlineupdesc',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/gwLineupDesc_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_gwlineupplayers.sh'
spark_sub_gwlineupplayers = BashOperator(task_id='spark_sub_gwlineupplayers',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/gwLineupPlayers_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_gwmetricsplayers.sh'
spark_sub_gwmetricsplayers = BashOperator(task_id='spark_sub_gwmetricsplayers',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/gwMetricsPlayers_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_gwmetricsteams.sh'
spark_sub_gwmetricsteams = BashOperator(task_id='spark_sub_gwmetricsteams',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/gwMetricsTeams_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_leagues.sh'
spark_sub_leagues = BashOperator(task_id='spark_sub_leagues',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/leagues_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_players.sh'
spark_sub_players = BashOperator(task_id='spark_sub_players',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/players_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_rounds.sh'
spark_sub_rounds = BashOperator(task_id='spark_sub_rounds',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/rounds_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_seasonmetricsplayers.sh'
spark_sub_seasonmetricsplayers = BashOperator(task_id='spark_sub_seasonmetricsplayers',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/seasonMetricsPlayers_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_seasonmetricsteam.sh'
spark_sub_seasonmetricsteam = BashOperator(task_id='spark_sub_seasonmetricsteam',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/seasonMetricsTeam_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_squads.sh'
spark_sub_squads = BashOperator(task_id='spark_sub_squads',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/squads_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_standings.sh'
spark_sub_standings = BashOperator(task_id='spark_sub_standings',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/standings_spark.py ',
                                    dag=dag)

# runs a spark job to transform and load data into redshift
# params['python_script'] = 'spark_sub_teams.sh'
spark_sub_teams = BashOperator(task_id='spark_sub_teams',
                                    bash_command= 'spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar /home/workspace/airflow/Transform_Load/teams_spark.py ',
                                    dag=dag)


check_list=[{'sql_template': SqlQueries.data_quality_records, 'expected_result': 20, 'check_column': "team_id", 'check_table': "dimTeams", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 671, 'check_column': "player_id", 'check_table': "dimPlayers", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 1, 'check_column': "league_id", 'check_table': "dimLeague", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 20, 'check_column': "standing_key", 'check_table': "dimStandings", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 380, 'check_column': "fixture_id", 'check_table': "dimFixtures", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 38, 'check_column': "gameweek_round_id", 'check_table': "dimRounds", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 671, 'check_column': "squad_submit_id", 'check_table': "dimSquads", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 20, 'check_column': "season_teams_metrics_id", 'check_table': "factSeasonMetricsTeams", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 607, 'check_column': "season_player_metrics_id", 'check_table': "factSeasonMetricsPlayers", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 380, 'check_column': "fixture_gameweek_team_stat_id", 'check_table': "factGameweekMetricsTeams", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 10789, 'check_column': "fixture_gameweek_player_stat_id", 'check_table': "factGameweekMetricsPlayers", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 380, 'check_column': "fixture_lineup_desc_id", 'check_table': "dimGameweekLineupDesc", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 13679, 'check_column': "fixture_lineup_id", 'check_table': "dimGameweekLineupPlayers", 'check_type': "Records"},
           {'sql_template': SqlQueries.data_quality_records, 'expected_result': 4496, 'check_column': "event_id", 'check_table': "factFixtureEvents", 'check_type': "Records"}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_list=check_list
)
run_quality_checks.doc="Performs a number of user specified data quality checks on loaded tables"


end_operator = DummyOperator(task_id='End-execution', dag=dag)

# Define data pipeline DAG structure
start_operator >> [reset_local_workspace, reset_s3_bucket, set_execution_permissions] >> json_filepath_upload >> \
[fantasy_ratings_gw_upload, fantasy_ratings_overall_upload, fifa_ratings_upload, fixture_events_api_extract, fixture_stats_api_extract, fixtures_api_extract, lineups_desc_api_extract, lineups_players_api_extract, player_fixture_stats_api_extract, player_stats_api_extract, rounds_api_extract, squads_api_extract, standings_api_extract, teams_api_extract, player_map_upload] >> \
create_db_skeleton_redshift >> [spark_sub_fixtures, spark_sub_gwlineupdesc, spark_sub_gwmetricsplayers, spark_sub_gwmetricsteams, spark_sub_leagues, spark_sub_players, spark_sub_seasonmetricsteam, spark_sub_squads, spark_sub_standings, spark_sub_teams, spark_sub_fixture_events, spark_sub_gwlineupplayers, spark_sub_seasonmetricsplayers, spark_sub_rounds] >> \
run_quality_checks >> end_operator
