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
    

def sql_transform(spark, query, staging_df_1, staging_df_2, dbtable_read_base, dbtable_read_join):
    """Processes staging data into final transformed format
        
    Processes redshift staging data into final transformed format
    and then returns the transformed dataframe to later be loaded
    into analytical tables in redshift
    
    Args:
        spark: SparkSession object
        query: SparkSQL query to transform dataframe
        staging_df_1: staging data from redshift contained in a spark dataframe
        staging_df_2: staging data from redshift contained in a spark dataframe
        dtable_read_base: Redshift table name to read data from
        dtable_read_join: Redshift table name to read data from
       
    Returns:
        transform_table: transformed data in spark dataframe
    """
    print("Creating Temp View for Spark SQL")
    staging_df_1.createOrReplaceTempView(dbtable_read_base)
    print("Temp View Created for Table: {}".format(dbtable_read_base))
    
    print("Creating Temp View for Spark SQL")
    staging_df_2.createOrReplaceTempView(dbtable_read_join)
    print("Temp View Created for Table: {}".format(dbtable_read_join))
    
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
    spark = create_spark_session('dwMetricsteamsLoad')
    
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
    dbtable_read_base = 'staging_fixture_stats'
    dbtable_read_join = 'staging_fixtures'
    
    # Outline the query to be executed in the spark job    
    query = '''
    SELECT
    sfs.fixture_id,
    sf.awayTeam_team_id,
    sf.homeTeam_team_id,
    sfs.Ball_Possession_away,
    sfs.Ball_Possession_home,
    COALESCE(sfs.Blocked_Shots_away, 0) AS Blocked_Shots_away,
    COALESCE(sfs.Blocked_Shots_home, 0) AS Blocked_Shots_home, 
    COALESCE(sfs.Corner_Kicks_away, 0) AS Corner_Kicks_away,
    COALESCE(sfs.Corner_Kicks_home, 0) AS Corner_Kicks_home,
    COALESCE(sfs.Fouls_away, 0) AS Fouls_away,
    COALESCE(sfs.Fouls_home, 0) AS Fouls_home,
    COALESCE(sfs.Goalkeeper_Saves_away, 0) As Goalkeeper_Saves_away,
    COALESCE(sfs.Goalkeeper_Saves_home, 0) AS Goalkeeper_Saves_home,
    COALESCE(sfs.Offsides_away, 0) AS Offsides_away,
    COALESCE(sfs.Offsides_home, 0) AS Offsides_home,
    COALESCE(sfs.Passes_Percent_away, 0) AS Passes_Percent_away,
    COALESCE(sfs.Passes_Percent_home, 0) AS Passes_Percent_home,
    COALESCE(sfs.Passes_accurate_away, 0) AS Passes_accurate_away,
    COALESCE(sfs.Passes_accurate_home, 0) AS Passes_accurate_home,
    COALESCE(sfs.Red_Cards_away, 0) AS Red_Cards_away,
    COALESCE(sfs.Red_Cards_home, 0) AS Red_Cards_home,
    COALESCE(sfs.Shots_insidebox_away, 0) AS Shots_insidebox_away,
    COALESCE(sfs.Shots_insidebox_home, 0) AS Shots_insidebox_home,
    COALESCE(sfs.Shots_off_goal_away, 0) As Shots_off_goal_away,
    COALESCE(sfs.Shots_off_goal_home, 0) AS Shots_off_goal_home,
    COALESCE(sfs.Shots_on_goal_away, 0) AS Shots_on_goal_away,
    COALESCE(sfs.Shots_on_goal_home, 0) AS Shots_on_goal_home,
    COALESCE(sfs.Shots_outsidebox_away, 0) AS Shots_outsidebox_away,
    COALESCE(sfs.Shots_outsidebox_home, 0) AS Shots_outsidebox_home,
    COALESCE(sfs.Total_Shots_away, 0) AS Total_Shots_away,
    COALESCE(sfs.Total_Shots_home, 0) AS Total_Shots_home,
    COALESCE(sfs.Total_Passes_away, 0) AS Total_Passes_away,
    COALESCE(sfs.Total_Passes_home, 0) AS Total_Passes_home,
    COALESCE(sfs.Yellow_Cards_away, 0) As Yellow_Cards_away,
    COALESCE(sfs.Yellow_Cards_home, 0) AS Yellow_Cards_home,
    current_timestamp() AS last_data_update
 
     FROM {} sfs
     LEFT JOIN {} sf on sf.fixture_id = sfs.fixture_id
     ORDER BY sfs.fixture_id ASC
            '''.format(dbtable_read_base, dbtable_read_join)
    
    # Set the table to write the transformed data to
    dbtable_write = 'factGameweekMetricsTeams'
    mode = 'append'
    na_replace_val = 'NULL'
    
    # Read in the data and create the materialised views in spark    
    staging_df_1 = read_from_redshift(spark, dbtable_read_base, endpoint=url, user=user, password=db_pass)
    staging_df_2 = read_from_redshift(spark, dbtable_read_join, endpoint=url, user=user, password=db_pass)
    
    # Transform and load the data    
    transform_table = sql_transform(spark, query, staging_df_1, staging_df_2, dbtable_read_base, dbtable_read_join)
    clean_load(transform_table, na_replace_val, dbtable_write, mode, endpoint=url, user=user, password=db_pass)
    
    # Close out the spark connect
    spark.stop()
    
    # Exit the python terminal
    exit()

if __name__ == "__main__":
    main()