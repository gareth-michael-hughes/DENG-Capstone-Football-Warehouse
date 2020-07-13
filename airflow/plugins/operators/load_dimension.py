from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 destination_table="",
                 sql_query="",
                 truncate_table=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_query=sql_query
        self.truncate_table=truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Redshift connection created")
        
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("TRUNCATE {}".format(self.table))
            
        self.log.info(f"Requesting Redshift to run the following SQL query: Insert into {self.destination_table} {self.sql_query}")
        redshift.run(f"Insert into {self.destination_table} {self.sql_query}")
        self.log.info(f"Records loaded into dimension table {self.destination_table}")