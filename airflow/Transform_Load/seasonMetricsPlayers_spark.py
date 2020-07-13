# Uncomment this block if testing spark job while still in jupyter notebook
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /home/workspace/gh_project/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar --jars /home/workspace/gh_project/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar pyspark-shell'

import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, regexp_extract, regexp_replace


def create_spark_session(app_name):
    """Creates or get spark session 
        
    Creates a spark session instance or gets
    and returns most recently created instance.
    
    Args:
        app_name: The application name of the spark job being submitted
        
    Returns:
        spark: SparkSession object
    """
    print("Building Spark session")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.shuffle.partitions",5) \
        .appName(app_name) \
        .getOrCreate()
    
    print("Spark session created")
    
    return spark


def read_from_redshift(spark, dbtable_read, endpoint, user, password):
    """Reads Data from Redshift Table and creates Temp View
        
    Reads in data from specified Redshift table
    Creates temp view to allow spark sql operations
    for later transforming table
    
    Args:
        spark: SparkSession object
        dtable_read: Redshift table name to read data from
        endpoint: the Url endpoint of the database to read from
        user: user account with database permissions
        password: password for user account        
        
    Returns:
        staging_df: staging data from redshift contained in a spark dataframe
    """
    
    print("Begin reading in data from Table: {}".format(dbtable_read))
    
    staging_df = spark.read \
    .format("jdbc") \
    .option("url", endpoint) \
    .option("dbtable", dbtable_read) \
    .option("user", user) \
    .option("password", password) \
    .load()
    
    print("Data loaded, here is your schema:")
    # check schema at initial read
    staging_df.printSchema()

    return staging_df
    

def sql_transform(spark, query, staging_df_1, staging_df_2, staging_df_3, \
                  staging_df_4, staging_df_5, dbtable_read_base, dbtable_read_join_1, dbtable_read_join_2, \
                 dbtable_read_join_3, dbtable_read_join_4):
    """Processes staging data into final transformed format
        
    Processes redshift staging data into final transformed format
    and then returns the transformed dataframe to later be loaded
    into analytical tables in redshift
    
    Args:
        spark: SparkSession object
        query: SparkSQL query to transform dataframe
        staging_df_1: staging data from redshift contained in a spark dataframe
        staging_df_2: staging data from redshift contained in a spark dataframe
        staging_df_3: staging data from redshift contained in a spark dataframe
        staging_df_4: staging data from redshift contained in a spark dataframe
        staging_df_5: staging data from redshift contained in a spark dataframe        
        dtable_read_base: Redshift table name to read data from
        dtable_read_join_1: Redshift table name to read data from
        dtable_read_join_2: Redshift table name to read data from
        dtable_read_join_3: Redshift table name to read data from
        dtable_read_join_4: Redshift table name to read data from        
       
    Returns:
        transform_table: transformed data in spark dataframe
    """
    print("Creating Temp View for Spark SQL")
    staging_df_1.createOrReplaceTempView(dbtable_read_base)
    print("Temp View Created for Table: {}".format(dbtable_read_base))
    
    print("Creating Temp View for Spark SQL")
    staging_df_2.createOrReplaceTempView(dbtable_read_join_1)
    print("Temp View Created for Table: {}".format(dbtable_read_join_1))
    
    print("Creating Temp View for Spark SQL")
    staging_df_3.createOrReplaceTempView(dbtable_read_join_2)
    print("Temp View Created for Table: {}".format(dbtable_read_join_2))
    
    print("Creating Temp View for Spark SQL")
    staging_df_4.createOrReplaceTempView(dbtable_read_join_3)
    print("Temp View Created for Table: {}".format(dbtable_read_join_3))

    print("Creating Temp View for Spark SQL")
    staging_df_5.createOrReplaceTempView(dbtable_read_join_4)
    print("Temp View Created for Table: {}".format(dbtable_read_join_4))
    
    print("Performing Transformations")
    transform_table = spark.sql(query)
    print("Transformations completed")
    
    return transform_table


def clean_load(transform_table, na_replace_val, dbtable_write, mode, endpoint, user, password):
    """Cleans transformed data and writes to final table in Redshift
        
    Takes in a transformed spark dataframe, replaces null values
    and then writes that dataframe to the final redshift table
    
    Args:
        transform_table: transformed data in spark dataframe
        na_replace_val: the value to replace nulls in the data
        dbtable_write: redshift analytical table to load data into
        mode: save mode type i.e. append or overwrite
        endpoint: the Url endpoint of the database to read from
        user: user account with database permissions
        password: password for user account
        
    Returns:
        None
    """
    print("Replacing null values in table with {}".format(na_replace_val))
    transform_table_cleaned = transform_table.na.fill(na_replace_val)
    print("Table successfully cleaned")
    
    print("Writing data to Redshift Analytical Table {}".format(dbtable_write))
    transform_table_cleaned.write \
    .format("jdbc") \
    .option("url", endpoint) \
    .option("dbtable", dbtable_write) \
    .option("user", user) \
    .option("password", password) \
    .mode(mode) \
    .save()
    
    print('Table {} successfully loaded'.format(dbtable_write))


def main():
    spark = create_spark_session('seasonMetricsPlayersLoad')
    
    # Read in the configuration specifications
    config = configparser.ConfigParser()
    config.read('/home/workspace/airflow/Configs/ars.cfg')
    
    # Set the relevant credentials from config file
    endpoint = config['CLUSTER']['HOST']
    db = config['CLUSTER']['DB_NAME']
    user = config['CLUSTER']['DB_USER']
    db_pass = config['CLUSTER']['DB_PASSWORD']
    port = config['CLUSTER']['DB_PORT']
    url = 'jdbc:redshift://'+endpoint+':'+str(port)+'/'+db
    
    # Set the staging tables to read and join data from
    dbtable_read_base = 'staging_player_stats'
    dbtable_read_join_1 = 'staging_player_fantasy_stats_overall'
    dbtable_read_join_2 = 'staging_team_squads'
    dbtable_read_join_3 = "staging_player_fifa_ratings"
    dbtable_read_join_4 = "staging_player_map_table"
    
    # Outline the query to be executed in the spark job    
    query = '''
    SELECT DISTINCT
    ts.player_id,
    sps.team_id,
    2 AS league_id,
    sps.captain AS season_captain,
    sps.cards_red AS season_cards_red,
    sps.cards_yellow AS season_cards_yellow,
    sps.cards_yellowred AS season_cards_yellowred,
    sps.dribbles_attempts AS season_dribbles_attempts,
    sps.dribbles_success AS season_dribbles_success,
    sps.duels_total AS season_duels_total,
    sps.duels_won AS season_duels_won,
    sps.fouls_committed AS season_fouls_committed,
    sps.fouls_drawn AS season_fouls_drawn,
    sps.games_appearances AS season_games_appearances,
    sps.games_lineups AS season_games_lineups,
    sps.games_minutes_played AS season_games_minutes_played,
    sps.goals_assists AS season_goals_assists,
    sps.goals_conceded AS season_goals_conceded,
    sps.goals_total AS season_goals_total,
    sps.injured AS season_injured,
    sps.passes_accuracy AS season_passes_accuracy,
    sps.passes_key AS season_passes_key,
    sps.passes_total AS season_passes_total,
    sps.penalty_committed AS season_penalty_committed,
    sps.penalty_missed AS season_penalty_missed,
    sps.penalty_saved AS season_penalty_saved,
    sps.penalty_success AS season_penalty_success,
    sps.penalty_won AS season_penalty_won,
    sps.rating AS season_rating,
    sps.shots_on AS season_shots_on,
    sps.shots_total AS season_shots_total,
    sps.substitutes_bench AS season_substitutes_bench,
    sps.substitutes_in AS season_substitutes_in,
    sps.substitutes_out AS season_substitutes_out,
    sps.tackles_blocks AS season_tackles_blocks,
    sps.tackles_interceptions AS season_tackles_interceptions,
    sps.tackles_total AS season_tackles_total,
    fr.Overall AS fifa_Overall,
    fr.Potential AS fifa_Potential,
    fr.Value AS fifa_Value,
    fr.Wage AS fifa_Wage,
    CAST(fr.Weak_Foot as INT) AS fifa_Weak_Foot_rating,
    CAST(fr.Skill_Moves as INT) AS fifa_Skill_Moves_rating,
    fr.Work_Rate,
    CAST(LEFT(fr.LS,2) as INT) AS fifa_LS_rating,
    CAST(LEFT(fr.ST,2) as INT) AS fifa_ST_rating,
    CAST(LEFT(fr.RS,2) as INT) AS fifa_RS_rating,
    CAST(LEFT(fr.LW,2) as INT) AS fifa_LW_rating,
    CAST(LEFT(fr.LF,2) as INT) AS fifa_LF_rating,
    CAST(LEFT(fr.CF,2) as INT) AS fifa_CF_rating,
    CAST(LEFT(fr.RF,2) as INT) AS fifa_RF_rating,
    CAST(LEFT(fr.RW,2) as INT) AS fifa_RW_rating,
    CAST(LEFT(fr.LAM,2) as INT) AS fifa_LAM_rating,
    CAST(LEFT(fr.CAM,2) as INT) AS fifa_CAM_rating,
    CAST(LEFT(fr.RAM,2) as INT) AS fifa_RAM_rating,
    CAST(LEFT(fr.LM,2) as INT) AS fifa_LM_rating,
    CAST(LEFT(fr.LCM,2) as INT) AS fifa_LCM_rating,
    CAST(LEFT(fr.CM,2) as INT) AS fifa_CM_rating,
    CAST(LEFT(fr.RCM,2) as INT) AS fifa_RCM_rating,
    CAST(LEFT(fr.RM,2) as INT) AS fifa_RM_rating,
    CAST(LEFT(fr.LWB,2) as INT) AS fifa_LWB_rating,
    CAST(LEFT(fr.LDM,2) as INT) AS fifa_LDM_rating,
    CAST(LEFT(fr.CDM,2) as INT) AS fifa_CDM_rating,
    CAST(LEFT(fr.RDM,2) as INT) AS fifa_RDM_rating,
    CAST(LEFT(fr.RWB,2) as INT) AS fifa_RWB_rating,
    CAST(LEFT(fr.LB,2) as INT) AS fifa_LB_rating,
    CAST(LEFT(fr.LCB,2) as INT) AS fifa_LCB_rating,
    CAST(LEFT(fr.CB,2) as INT) AS fifa_CB_rating,
    CAST(LEFT(fr.RCB,2) as INT) AS fifa_RCB_rating,
    CAST(LEFT(fr.RB,2) as INT) AS fifa_RB_rating,
    CAST(fr.Crossing as INT) AS fifa_Crossing_rating,
    CAST(fr.Finishing as INT) AS fifa_Finishing_rating,
    CAST(fr.HeadingAccuracy as INT) AS fifa_HeadingAccuracy_rating,
    CAST(fr.ShortPassing as INT) AS fifa_ShortPassing_rating,
    CAST(fr.Volleys as INT) AS fifa_Volleys_rating,
    CAST(fr.Dribbling as INT) AS fifa_Dribbling_rating,
    CAST(fr.Curve as INT) AS fifa_Curve_rating,
    CAST(fr.FKAccuracy as INT) AS fifa_FKAccuracy_rating,
    CAST(fr.LongPassing as INT) AS fifa_LongPassing_rating,
    CAST(fr.BallControl as INT) AS fifa_BallControl_rating,
    CAST(fr.Acceleration as INT) AS fifa_Acceleration_rating,
    CAST(fr.SprintSpeed as INT) AS fifa_SprintSpeed_rating,
    CAST(fr.Agility as INT) AS fifa_Agility_rating,
    CAST(fr.Reactions as INT) AS fifa_Reactions_rating,
    CAST(fr.Balance as INT) AS fifa_Balance_rating,
    CAST(fr.ShotPower as INT) AS fifa_ShotPower_rating,
    CAST(fr.Jumping as INT) AS fifa_Jumping_rating,
    CAST(fr.Stamina as INT) AS fifa_Stamina_rating,
    CAST(fr.Strength AS INT) AS fifa_Strength_rating,
    CAST(fr.LongShots as INT) AS fifa_LongShots_rating,
    CAST(fr.Aggression as INT) As fifa_Aggression_rating,
    CAST(fr.Interceptions as INT) AS fifa_Interceptions_rating,
    CAST(fr.Positioning as INT) AS fifa_Positioning_rating,
    CAST(fr.Vision as INT) As fifa_Vision_rating,
    CAST(fr.Penalties AS INT) AS fifa_Penalties_rating,
    CAST(fr.Composure as INT) AS fifa_Composure_rating,
    CAST(fr.Marking as INT) AS fifa_Marking_rating,
    CAST(fr.StandingTackle as INT) as fifa_StandingTackle_rating,
    CAST(fr.SlidingTackle as INT) AS fifa_SlidingTackle_rating,
    CAST(fr.GKDiving as INT) AS fifa_GKDiving_rating,
    CAST(fr.GKHandling as INT) AS fifa_GKHandling_rating,
    CAST(fr.GKKicking as INT) AS fifa_GKKicking_rating,
    CAST(fr.GKPositioning as INT) As fifa_GKPositioning_rating,
    CAST(fr.GKReflexes as INT) As fifa_GKReflexes_rating,
    CAST(spfa.total_points as INT) AS fantasy_total_points,
    spfa.creativity AS fantasy_creativity_rating,
    spfa.influence AS fantasy_influence_rating,
    spfa.threat AS fantasy_threat_rating,
    CAST(spfa.bonus as INT) AS fantasy_bonus_rating,
    CAST(spfa.bps as INT) AS fantasy_bps_rating,
    spfa.selected_by_percent AS fantasy_selected_by_percent,
    current_timestamp() AS last_data_update
 
    FROM staging_team_squads ts
    LEFT JOIN staging_player_stats sps ON CAST(sps.player_id as INT) = CAST(ts.player_id AS INT)
    LEFT JOIN staging_player_map_table spmt ON CAST(spmt.player_original_id AS INT) = CAST(ts.player_id AS INT) 
    LEFT JOIN staging_player_fifa_ratings fr ON CAST(fr.Player_id AS INT) = CAST(spmt.fifa_id AS INT)
    LEFT JOIN staging_player_fantasy_stats_overall spfa ON CAST(spfa.player_id AS INT)
    = CAST(spmt.fantasy_id AS INT)
    WHERE sps.league = 'Premier League'
                '''
    
    # Set the table to write the transformed data to    
    dbtable_write = 'factSeasonMetricsPlayers'
    mode = 'append'
    na_replace_val = 'NULL'
    
    # Read in the data and create the materialised views in spark
    staging_df_1 = read_from_redshift(spark, dbtable_read_base, endpoint=url, user=user, password=db_pass)
    staging_df_2 = read_from_redshift(spark, dbtable_read_join_1, endpoint=url, user=user, password=db_pass)
    staging_df_3 = read_from_redshift(spark, dbtable_read_join_2, endpoint=url, user=user, password=db_pass)
    staging_df_4 = read_from_redshift(spark, dbtable_read_join_3, endpoint=url, user=user, password=db_pass)
    staging_df_5 = read_from_redshift(spark, dbtable_read_join_4, endpoint=url, user=user, password=db_pass)    
    
    # Transform and load the data    
    transform_table = sql_transform(spark, query, staging_df_1, staging_df_2, staging_df_3, staging_df_4, \
                                    staging_df_5, dbtable_read_base, dbtable_read_join_1, \
                                    dbtable_read_join_2, dbtable_read_join_3, dbtable_read_join_4)
    clean_load(transform_table, na_replace_val, dbtable_write, mode, endpoint=url, user=user, password=db_pass)
    
    # Close out the spark connect
    spark.stop()
    
    # Exit the python terminal
    exit()

if __name__ == "__main__":
    main()