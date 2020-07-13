import configparser
import psycopg2
from sql_stage_queries import drop_stage_table_queries, create_stage_table_queries, copy_table_queries
from sql_transform_load_queries import drop_transform_table_queries, create_transform_table_queries

def drop_tables(cur, conn, drop_list):
    """Drops any pre-existing versions of the staging or analytical tables in Redshift RDB

    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB
    drop_list: a list of queries to drop a number of tables
    
    Returns:
    None

   """
    
    for query in drop_list:
        cur.execute(query)
        conn.commit()
        print('Dropped {}'.format(query))


def create_tables(cur, conn, create_list):
    """Creates the staging and analytical tables in Redshift RDB

    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB
    create_list: a list of queries to create a number of tables
    
    Returns:
    None    

   """

    for query in create_list:
        cur.execute(query)
        conn.commit()
        print('Created table for {}'.format(query))


def load_staging_tables(cur, conn, copy_list):
    """Copies song and event data from S3 bucket and load into staging tables in Redshift RDB
    
    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB
    copy_list: a list of queries to copy json manifest files from s3 to Redshift tables
    
    Returns:
    None

   """

    for query in copy_list:
        cur.execute(query)
        conn.commit()
        print('Loaded table {}'.format(query))



def main():
    # Read in the configurations
    config = configparser.ConfigParser()
    config.read('/home/workspace/airflow/Configs/ars.cfg')

    # Create connection string to Redshift RDB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create cursor to execute SQL queries
    cur = conn.cursor()

    # Create tables for staging
    print('Begin Dropping Staging Tables')
    drop_tables(cur, conn, drop_stage_table_queries)
    print('Table skeletons for Staging successfully dropped')
    
    print('Begin Creating Staging Tables')
    create_tables(cur, conn, create_stage_table_queries)
    print('Table skeletons successfully created for staging')
        
    # Create tables for holding transformed data
    print('Begin Dropping Transform Tables')
    drop_tables(cur, conn, drop_transform_table_queries)
    print('Table skeletons successfully dropped for transforms')
    
    print('Begin Creating Transform Tables')
    create_tables(cur, conn, create_transform_table_queries)
    print('Table skeletons successfully created for transformed tables')

    # Load json files from S3 into staging tables
    print('Begin loading Staging Tables from S3')
    load_staging_tables(cur, conn, copy_table_queries)
    print('Staging Tables successfully loaded')

    
    # Close out connection to Redshift RDB
    conn.close()


if __name__ == "__main__":
    main()