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
    

def sql_transform(spark, query, staging_df, dbtable_read):
    """Processes staging data into final transformed format
        
    Processes redshift staging data into final transformed format
    and then returns the transformed dataframe to later be loaded
    into analytical tables in redshift
    
    Args:
        spark: SparkSession object
        query: SparkSQL query to transform dataframe
        staging_df: staging data from redshift contained in a spark dataframe
        dtable_read: Redshift table name to read data from
       
    Returns:
        transform_table: transformed data in spark dataframe
    """
    print("Creating Temp View for Spark SQL")
    staging_df.createOrReplaceTempView(dbtable_read)
    print("Temp View Created for Table: {}".format(dbtable_read))
    
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
    spark = create_spark_session('dimFixturesLoad')
    
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
    dbtable_read = 'staging_fixtures'
    
      # Outline the query to be executed in the spark job
    query = '''
    SELECT
    fixture_id,
    awayTeam_team_id,
    elapsed,
    to_date(from_unixtime(event_timestamp,"yyyy-MM-dd")) AS event_date,
    from_unixtime(event_timestamp) + INTERVAL 1 hour AS event_timestamp,
    from_unixtime(firstHalfStart) + INTERVAL 1 hour AS firstHalfStart,
    homeTeam_team_id,
    2 AS league_id,
    goalsAwayTeam,
    goalsHomeTeam,
    score_fulltime,
    score_halftime,
    from_unixtime(secondHalfStart) + INTERVAL 1 hour AS secondHalfStart,
    referee,
    round AS fixture_round,
    status AS fixture_status,
    statusShort AS fixture_statusShort,
    venue AS fixture_venue,
    CASE WHEN goalsAwayTeam > goalsHomeTeam THEN 'AWAY WIN'
    WHEN goalsAwayTeam = goalsHomeTeam THEN 'DRAW'
    WHEN goalsAwayTeam < goalsHomeTeam THEN 'HOME WIN'
    END AS fixture_result,
 
    CASE WHEN goalsAwayTeam > goalsHomeTeam THEN 3
    WHEN goalsAwayTeam = goalsHomeTeam THEN 1
    WHEN goalsAwayTeam < goalsHomeTeam THEN 0
    END AS fixture_awayTeam_points,
 
    CASE WHEN goalsAwayTeam > goalsHomeTeam THEN 0
    WHEN goalsAwayTeam = goalsHomeTeam THEN 1
    WHEN goalsAwayTeam < goalsHomeTeam THEN 3
    END AS fixture_homeTeam_points,
    current_timestamp() AS last_data_update
    FROM {}
            '''.format(dbtable_read)
    
    # Set the table to write the transformed data to
    dbtable_write = 'dimFixtures'
    mode = 'append'
    na_replace_val = 'NULL'
    
    # Read in the data and create the materialised views in spark
    staging_df = read_from_redshift(spark, dbtable_read, user=user, endpoint=url, password=db_pass)    
    
    # Transform and load the data
    transform_table = sql_transform(spark, query, staging_df, dbtable_read)
    clean_load(transform_table, na_replace_val, dbtable_write, mode, user=user, endpoint=url, password=db_pass)
    
    # Close out the spark connect    
    spark.stop()
    
    # Exit the python terminal    
    exit()

if __name__ == "__main__":
    main()