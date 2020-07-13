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
    

def sql_transform(spark, query, staging_df_1, staging_df_2, staging_df_3, staging_df_4, \
                  staging_df_5, dbtable_read_base, dbtable_read_join_1, dbtable_read_join_2, \
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
    spark = create_spark_session('gwMetricsPlayersLoad')
    
    # Read in the configuration specifications
    config = configparser.ConfigParser()
    config.read('/home/workspace/gh_project/Configs/ars.cfg')
    
    # Set the relevant credentials from config file
    endpoint = config['CLUSTER']['HOST']
    db = config['CLUSTER']['DB_NAME']
    user = config['CLUSTER']['DB_USER']
    db_pass = config['CLUSTER']['DB_PASSWORD']
    port = config['CLUSTER']['DB_PORT']
    url = 'jdbc:redshift://'+endpoint+':'+str(port)+'/'+db
    
    # Set the staging tables to read and join data from
    dbtable_read_base = 'staging_fixture_player_stats'
    dbtable_read_join_1 = 'staging_player_fantasy_stats_gw'
    dbtable_read_join_2 = 'staging_team_squads'
    dbtable_read_join_3 = 'staging_fixtures'
    dbtable_read_join_4 = 'staging_player_map_table'
    
    # Outline the query to be executed in the spark job
    query = '''
    SELECT
    ts.player_id,
    sfps.fixture_id,
    sfps.team_id,
    upper(sfps.captain) AS player_fixture_captain,
    sfps.cards_red AS player_fixture_cards_red,
    sfps.cards_yellow AS player_fixture_cards_yellow,
    sfps.dribbles_attempts AS player_fixture_dribbles_attempts,
    sfps.dribbles_past As player_fixture_dribbles_past,
    sfps.dribbles_success AS player_fixture_dribbles_success,
    sfps.duels_total As player_fixture_duels_total,
    sfps.duels_won AS player_fixture_duels_won,
    sfps.event_id AS player_fixture_event_id,
    sfps.fouls_committed AS player_fixture_fouls_committed,
    sfps.fous_drawn AS player_fixture_fouls_drawn,
    sfps.goals_assists AS player_fixture_goals_assists,
    sfps.goals_conceded AS player_fixture_goals_conceded,
    sfps.goals_total AS player_fixture_goals_total,
    sfps.minutes_played AS player_fixture_minutes_played,
    COALESCE(sfps.offsides,0) AS player_fixture_offsides,
    sfps.passes_accuracy AS player_fixture_passes_accuracy,
    sfps.passes_key AS player_fixture_passes_key,
    sfps.passes_total AS player_fixture_passes_total,
    sfps.penalty_committed AS player_fixture_penalty_committed,
    sfps.penalty_missed AS player_fixture_penalty_missed,
    sfps.penalty_saved AS player_fixture_penalty_saved,
    sfps.penalty_success AS player_fixture_penalty_success,
    sfps.penalty_won AS player_fixture_penalty_won,
    CASE WHEN sfps.rating = '–' THEN NULL
    ELSE CAST(sfps.rating as FLOAT) END AS player_fixture_rating,
    sfps.shots_on AS player_fixture_shots_on,
    sfps.shots_total AS player_fixture_shots_total,
    upper(sfps.substitute) AS player_fixture_substitute,
    sfps.tackles_blocks AS player_fixture_tackles_blocks,
    sfps.tackles_interceptions AS player_fixture_tackles_interceptions,
    sfps.tackles_total AS player_fixture_tackles_total,
    sfps.updateAt AS updateAt_api,
    CAST(spfgw.attempted_passes as INT) AS player_fixture_attempted_passes,
    CAST(spfgw.big_chances_created as INT) AS player_fixture_big_chances_created,
    CAST(spfgw.big_chances_missed as INT) AS player_fixture_big_chances_missed,
    CAST(spfgw.bonus as INT) AS player_fixture_fantasy_bonus,
    CAST(spfgw.bps AS int) AS player_fixture_fantasy_bps,
    CAST(spfgw.clean_sheets as INT) AS player_fixture_clean_sheets,
    CAST(spfgw.clearances_blocks_interceptions as INT) AS player_fixture_clearances_blocks_interceptions,
    CAST(spfgw.completed_passes AS INT) AS player_fixture_completed_passes,
    CAST(spfgw.creativity as FLOAT) AS player_fixture_fantasy_creativity_rating,
    CAST(spfgw.errors_leading_to_goal as INT) AS player_fixture_errors_leading_to_goal,
    CAST(spfgw.errors_leading_to_goal_attempt as INT) AS player_fixture_errors_leading_to_goal_attempt,
    CAST(spfgw.influence AS FLOAT) AS player_fixture_fantasy_influence_rating,
    CAST(spfgw.open_play_crosses AS INT) AS player_fixture_open_play_crosses,
    CAST(spfgw.own_goals AS INT) AS player_fixture_own_goals,
    CAST(spfgw.recoveries AS INT) AS player_fixture_recoveries,
    CAST(spfgw.saves AS INT) AS player_fixture_saves,
    selected AS player_fixture_selected,
    CAST(spfgw.tackled as INT) AS player_fixture_tackled,
    CAST(spfgw.target_missed as INT) AS player_fixture_target_missed,
    CAST(CAST(spfgw.threat as FLOAT) AS INT) AS player_fixture_fantasy_threat_rating,
    CAST(spfgw.total_points as INT) AS player_fixture_fantasy_total_points,
    CAST(spfgw.transfers_in as INT) AS player_fixture_fantasy_transfers_in,
    CAST(spfgw.transfers_out as INT) AS player_fixture_fantasy_transfers_out,
    CAST(spfgw.value as INT) AS player_fixture_fantasy_value,
    current_timestamp() AS last_data_update
    
    FROM staging_team_squads ts
    LEFT JOIN staging_fixture_player_stats sfps on CAST(sfps.player_id AS INT) = CAST(ts.player_id AS INT)
    LEFT JOIN staging_fixtures sfix on sfix.fixture_id = sfps.fixture_id
    LEFT JOIN staging_player_map_table spmt ON CAST(spmt.player_original_id AS INT) = CAST(ts.player_id AS INT)
    LEFT JOIN staging_player_fantasy_stats_gw spfgw ON
    CAST(regexp_extract(spfgw.name, '^.+_([^_]+)$', 1) AS INT)
    = CAST(spmt.fantasy_id AS INT)
    AND
    CAST(trim(regexp_replace(regexp_replace(spfgw.kickoff_time, 'T', ' '), 'Z', ' ')) as timestamp)
    =
    CAST(from_unixtime(sfix.event_timestamp) as timestamp) 
            '''
    
    # Set the table to write the transformed data to    
    dbtable_write = 'factGameweekMetricsPlayers'
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